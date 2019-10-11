package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class BlobStoreDirectory extends BaseDirectory {

    private static final Logger logger = LogManager.getLogger(BlobStoreDirectory.class);

    // associate a snapshot name & index name but we could pin a specific snapshot too
    // if we don't use restore to bootstrap the index then we need to check the number of shards is correct
    public static Setting<String> REPOSITORY_NAME = Setting.simpleString("index.glacial.repository", Property.IndexScope);
    public static Setting<String> REPOSITORY_SNAPSHOT = Setting.simpleString("index.glacial.snapshot", Property.IndexScope);
    public static Setting<String> REPOSITORY_INDEX = Setting.simpleString("index.glacial.index", Property.IndexScope);
    public static Setting<ByteSizeValue> REPOSITORY_BUFFER = Setting.byteSizeSetting("index.glacial.buffer_size",
        new ByteSizeValue(BufferedIndexInput.BUFFER_SIZE), new ByteSizeValue(BufferedIndexInput.MIN_BUFFER_SIZE),
        new ByteSizeValue(Integer.MAX_VALUE), Property.IndexScope);

    private final BlobStoreRepository repository;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;
    private final int buffer;

    private volatile BlobStoreIndexShardSnapshot files;

    public BlobStoreDirectory(final IndexSettings indexSettings, final ShardPath shardPath, final RepositoriesService repositories) {
        super(new SingleInstanceLockFactory());
        this.repository = requireRepository(repositories, indexSettings);
        this.indexId = requireIndex(repository.getRepositoryData(), indexSettings);
        this.snapshotId = requireSnapshot(repository.getRepositoryData(), indexSettings, indexId);
        this.buffer = Math.toIntExact(REPOSITORY_BUFFER.get(indexSettings.getSettings()).getBytes());
        this.shardId = shardPath.getShardId();
    }

    private BlobContainer shardContainer() {
        ensureOpen();
        return repository.shardContainer(indexId, shardId.id());
    }

    private void ensureSnapshotFilesLoaded() {
        ensureOpen();
        if (files == null) {
            ensureValidSnapshot(repository, snapshotId);
            synchronized (this) {
                if (files == null) {
                    this.files = repository.loadShardSnapshot(shardContainer(), snapshotId);
                }
            }
            logger.info("snapshot {} total_files {} total_size {}\n{}",
                files.snapshot(), files.totalFileCount(), new ByteSizeValue(files.totalSize()), Strings.toString(files, true, false));
        }
        assert files != null;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureSnapshotFilesLoaded();
        return files.indexFiles().stream()
            .map(BlobStoreIndexShardSnapshot.FileInfo::physicalName)
            .toArray(String[]::new);
    }

    private BlobStoreIndexShardSnapshot.FileInfo fileInfo(final String name) throws FileNotFoundException {
        ensureSnapshotFilesLoaded();
        for (BlobStoreIndexShardSnapshot.FileInfo file : files.indexFiles()) {
            if (file.physicalName().equals(name)) {
                return file;
            }
        }
        throw new FileNotFoundException(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return fileInfo(name).length();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        BlobStoreIndexShardSnapshot.FileInfo file = fileInfo(name);
        int buffer = adjustBufferSize(file, this.buffer);
        String resourceDesc = String.format(Locale.ROOT, "repository: %s, snapshot: %s, index: %s, shard: %d, file: %s",
            repository.getMetadata().name(), snapshotId, indexId, shardId.id(), name);

        logger.trace("open file [{}] with buffer [{}]", file, buffer);
        return new BlobStoreIndexInput(resourceDesc, file, shardContainer(), buffer);
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Collections.emptySet();
    }

    private static BlobStoreRepository requireRepository(final RepositoriesService repositories, final IndexSettings indexSettings) {
        final String repositoryName = indexSettings.getSettings().get(REPOSITORY_NAME.getKey());
        if (Strings.hasLength(repositoryName) == false) {
            throw new IllegalStateException("No repository defined in index settings");
        }
        final Repository repository = repositories.repository(repositoryName);
        if (repository == null) {
            throw new IllegalStateException("Repository [" + repositoryName + "] does not exist");
        } else if ((repository instanceof BlobStoreRepository) == false) {
            throw new IllegalStateException("Repository [" + repositoryName + "] is not supported");
        }
        return (BlobStoreRepository) repository;
    }

    private static IndexId requireIndex(final RepositoryData repositoryData, final IndexSettings indexSettings) {
        final String indexName = indexSettings.getSettings().get(REPOSITORY_INDEX.getKey());
        if (Strings.hasLength(indexName) == false) {
            throw new IllegalStateException("No index name defined in index settings");
        }
        if (repositoryData.getIndices().containsKey(indexName) == false) {
            throw new IllegalStateException("Index [" + indexName + "] not found in repository data");
        }
        return repositoryData.resolveIndexId(indexName);
    }

    private static SnapshotId requireSnapshot(final RepositoryData repositoryData, final IndexSettings indexSettings, final IndexId index) {
        final String snapshotName = indexSettings.getSettings().get(REPOSITORY_SNAPSHOT.getKey());
        if (Strings.hasLength(snapshotName) == false) {
            throw new IllegalStateException("No snapshot name defined in index settings");
        }
        final Set<SnapshotId> snapshotIds = repositoryData.getSnapshots(index).stream()
            .filter(snapshotId -> snapshotName.equals(snapshotId.getName()))
            .collect(Collectors.toSet());
        if (snapshotIds.isEmpty() || snapshotIds.size() != 1) {
            throw new IllegalStateException("No snapshots with name [" + snapshotName + "] found for index [" + index + "]");
        }
        return snapshotIds.iterator().next();
    }

    private static void ensureValidSnapshot(final Repository repository, final SnapshotId snapshotId) {
        final SnapshotInfo snapshotInfo = repository.getSnapshotInfo(snapshotId);
        if (snapshotInfo.state().restorable() == false) {
            throw new IllegalStateException("Unsupported snapshot state [" + snapshotInfo.state() + "] for snapshot [" + snapshotId + "]");
        }
        if (Version.CURRENT.before(snapshotInfo.version())) {
            throw new IllegalStateException("Snapshot [" + snapshotId + "] was created with Elasticsearch version [" +
                snapshotInfo.version() + "] which is higher than the version of this node [" + Version.CURRENT + "]");
        }
    }

    private static int adjustBufferSize(final BlobStoreIndexShardSnapshot.FileInfo fileInfo, final int defaultBufferSize) {
        return Math.toIntExact(Math.min((long) defaultBufferSize, fileInfo.length()));
    }
}
