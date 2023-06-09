/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class FsDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        return switch (s) {
            case "native" -> NativeFSLockFactory.INSTANCE;
            case "simple" -> SimpleFSLockFactory.INSTANCE;
            default -> throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        }; // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings);
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        final String storeType = indexSettings.getSettings()
            .get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey());
        IndexModule.Type type;
        if (IndexModule.Type.FS.match(storeType)) {
            type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));
        } else {
            type = IndexModule.Type.fromSettingsKey(storeType);
        }
        Set<String> preLoadExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        switch (type) {
            case HYBRIDFS:
                // Use Lucene defaults
                final FSDirectory primaryDirectory = FSDirectory.open(location, lockFactory);
                if (primaryDirectory instanceof MMapDirectory mMapDirectory) {
                    return wrap(new HybridDirectory(lockFactory, setPreload(mMapDirectory, lockFactory, preLoadExtensions)));
                } else {
                    return wrap(primaryDirectory);
                }
            case MMAPFS:
                return wrap(setPreload(new MMapDirectory(location, lockFactory), lockFactory, preLoadExtensions));
            case SIMPLEFS:
            case NIOFS:
                return wrap(new NIOFSDirectory(location, lockFactory));
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    private static FilterDirectory wrap(Directory directory) {
        return new TrackingDeletionsDirectory(directory);
    }

    static final Predicate<String> isDocValueUpdateFile = file -> {
        try {
            return file.startsWith("_") && IndexFileNames.parseGeneration(file) > 0L;
        } catch (Exception e) {
            return false;
        }
    };

    static final long SLEEP_IN_MILLIS = 20L;
    static final long SLEEP_AFTER_BYTES = 16777216L; // 16 mb

    private static class TrackingDeletionsDirectory extends FilterDirectory {

        private final Map<String, Exception> deletes = new ConcurrentHashMap<>();

        protected TrackingDeletionsDirectory(Directory in) {
            super(in);
        }

        private void ensureNotDeleted(String name) {
            if (deletes.containsKey(name)) {
                throw new IllegalStateException("Reading from deleted IndexInput [" + name + ']');
            }
        }

        @Override
        public void deleteFile(String name) throws IOException {
            if (isDocValueUpdateFile.test(name)) {
                // capture the stack at the time the generational doc value file is deleted
                deletes.put(name, new Exception());
            }
            super.deleteFile(name);
        }

        @Override
        public void close() throws IOException {
            deletes.clear();
            super.close();
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            try {
                Thread.sleep(SLEEP_IN_MILLIS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return new FilterIndexInput("slow(" + name + ')', super.openInput(name, context)) {

                long readBytes = 0L;

                @Override
                public void readBytes(byte[] b, int offset, int len) throws IOException {
                    ensureNotDeleted(name);
                    if (readBytes + len >= SLEEP_AFTER_BYTES) {
                        readBytes = 0L;
                        try {
                            Thread.sleep(SLEEP_IN_MILLIS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    super.readBytes(b, offset, len);
                    readBytes += len;
                }

                @Override
                public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                    ensureNotDeleted(name);
                    return super.slice(sliceDescription, offset, length);
                }

                @Override
                public IndexInput clone() {
                    ensureNotDeleted(name);
                    return super.clone();
                }
            };
        }

    }

    public static MMapDirectory setPreload(MMapDirectory mMapDirectory, LockFactory lockFactory, Set<String> preLoadExtensions)
        throws IOException {
        assert mMapDirectory.getPreload() == false;
        if (preLoadExtensions.isEmpty() == false) {
            if (preLoadExtensions.contains("*")) {
                mMapDirectory.setPreload(true);
            } else {
                return new PreLoadMMapDirectory(mMapDirectory, lockFactory, preLoadExtensions);
            }
        }
        return mMapDirectory;
    }

    /**
     * Returns true iff the directory is a hybrid fs directory
     */
    public static boolean isHybridFs(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        return unwrap instanceof HybridDirectory;
    }

    static final class HybridDirectory extends NIOFSDirectory {
        private final MMapDirectory delegate;

        HybridDirectory(LockFactory lockFactory, MMapDirectory delegate) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            this.delegate = delegate;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (useDelegate(name, context)) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(name);
                // we only use the mmap to open inputs. Everything else is managed by the NIOFSDirectory otherwise
                // we might run into trouble with files that are pendingDelete in one directory but still
                // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                // and intersect for perf reasons.
                return delegate.openInput(name, context);
            } else {
                return super.openInput(name, context);
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(super::close, delegate);
        }

        private static String getExtension(String name) {
            // Unlike FileSwitchDirectory#getExtension, we treat `tmp` as a normal file extension, which can have its own rules for mmaping.
            final int lastDotIndex = name.lastIndexOf('.');
            if (lastDotIndex == -1) {
                return "";
            } else {
                return name.substring(lastDotIndex + 1);
            }
        }

        static boolean useDelegate(String name, IOContext ioContext) {
            if (ioContext == Store.READONCE_CHECKSUM) {
                // If we're just reading the footer for the checksum then mmap() isn't really necessary, and it's desperately inefficient
                // if pre-loading is enabled on this file.
                return false;
            }

            final LuceneFilesExtensions extension = LuceneFilesExtensions.fromExtension(getExtension(name));
            if (extension == null || extension.shouldMmap() == false) {
                // Other files are either less performance-sensitive (e.g. stored field index, norms metadata)
                // or are large and have a random access pattern and mmap leads to page cache trashing
                // (e.g. stored fields and term vectors).
                return false;
            }
            return true;
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }

    // TODO it would be nice to share code between PreLoadMMapDirectory and HybridDirectory but due to the nesting aspect of
    // directories here makes it tricky. It would be nice to allow MMAPDirectory to pre-load on a per IndexInput basis.
    static final class PreLoadMMapDirectory extends MMapDirectory {
        private final MMapDirectory delegate;
        private final Set<String> preloadExtensions;

        PreLoadMMapDirectory(MMapDirectory delegate, LockFactory lockFactory, Set<String> preload) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            super.setPreload(false);
            this.delegate = delegate;
            this.delegate.setPreload(true);
            this.preloadExtensions = preload;
            assert getPreload() == false;
        }

        @Override
        public void setPreload(boolean preload) {
            throw new IllegalArgumentException("can't set preload on a preload-wrapper");
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (useDelegate(name)) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(name);
                return delegate.openInput(name, context);
            }
            return super.openInput(name, context);
        }

        @Override
        public synchronized void close() throws IOException {
            IOUtils.close(super::close, delegate);
        }

        boolean useDelegate(String name) {
            final String extension = FileSwitchDirectory.getExtension(name);
            return preloadExtensions.contains(extension);
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }
}
