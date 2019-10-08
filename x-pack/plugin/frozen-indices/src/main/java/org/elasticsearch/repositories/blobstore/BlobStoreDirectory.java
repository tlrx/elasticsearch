package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.elasticsearch.Version;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Setting;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class BlobStoreDirectory extends BaseDirectory {

    public static Setting<String> REPOSITORY_NAME = Setting.simpleString("index.glacial.repository", Setting.Property.IndexScope);
    public static Setting<String> REPOSITORY_SNAPSHOT = Setting.simpleString("index.glacial.snapshot", Setting.Property.IndexScope);
    public static Setting<String> REPOSITORY_INDEX = Setting.simpleString("index.glacial.index", Setting.Property.IndexScope);

    private final BlobStoreRepository repository;
    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;

    private volatile BlobStoreIndexShardSnapshot files;

    public BlobStoreDirectory(final IndexSettings indexSettings, final ShardPath shardPath, final RepositoriesService repositories) {
        super(new SingleInstanceLockFactory());
        String repositoryName = Objects.requireNonNull(indexSettings.getSettings().get(REPOSITORY_NAME.getKey()));
        this.repository = requireRepository(repositories, repositoryName);
        RepositoryData repositoryData = repository.getRepositoryData();
        String indexName = Objects.requireNonNull(indexSettings.getSettings().get(REPOSITORY_INDEX.getKey()));
        this.indexId = requireIndex(repositoryData, indexName);
        String snapshotName = Objects.requireNonNull(indexSettings.getSettings().get(REPOSITORY_SNAPSHOT.getKey()));
        this.snapshotId = requireSnapshot(repositoryData, indexId, snapshotName);
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
        return new BlobStoreIndexInput(String.format(Locale.ROOT, "repository: %s, snapshot: %s, index: %s, shard: %d, file: %s",
            repository.getMetadata().name(), snapshotId, indexId, shardId.id(), name), fileInfo(name), shardContainer());
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new IndexOutput(name, name){

            @Override
            public void writeByte(byte b) throws IOException {

            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {

            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public long getFilePointer() {
                return 0;
            }

            @Override
            public long getChecksum() throws IOException {
                return 0;
            }
        };
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return new IndexOutput(prefix, prefix){

            @Override
            public void writeByte(byte b) throws IOException {

            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {

            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public long getFilePointer() {
                return 0;
            }

            @Override
            public long getChecksum() throws IOException {
                return 0;
            }
        };
    }

    @Override
    public void rename(String source, String dest) throws IOException {
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
    }

    @Override
    public void syncMetaData() throws IOException {
    }

    @Override
    public void deleteFile(String name) throws IOException {
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Collections.emptySet();
    }

    private static BlobStoreRepository requireRepository(final RepositoriesService repositories, final String repositoryName) {
        final Repository repository = repositories.repository(repositoryName);
        if (repository == null) {
            throw new IllegalStateException("Repository [" + repositoryName + "] does not exist");
        } else if ((repository instanceof BlobStoreRepository) == false) {
            throw new IllegalStateException("Repository [" + repositoryName + "] is not supported");
        }
        return (BlobStoreRepository) repository;
    }

    private static IndexId requireIndex(final RepositoryData repositoryData, final String indexName) {
        if (repositoryData.getIndices().containsKey(indexName) == false) {
            throw new IllegalStateException("Index [" + indexName + "] not found in repository data");
        }
        return repositoryData.resolveIndexId(indexName);
    }

    private static SnapshotId requireSnapshot(final RepositoryData repositoryData, final IndexId indexId, final String snapshotName) {
        final Set<SnapshotId> snapshotIds = repositoryData.getSnapshots(indexId).stream()
            .filter(snapshotId -> snapshotName.equals(snapshotId.getName()))
            .collect(Collectors.toSet());
        if (snapshotIds.isEmpty() || snapshotIds.size() != 1) {
            throw new IllegalStateException("No snapshots with name [" + snapshotName + "] found for index [" + indexId + "]");
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
}
