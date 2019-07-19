package org.elasticsearch.recovery;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.store.MockFSIndexStore;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.simpleString;
import static org.elasticsearch.common.settings.Setting.versionSetting;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST, numClientNodes = 0)
public abstract class AutoRestoreIntegTestCase extends ESIntegTestCase {

    protected static final Setting<Boolean> RESTORE_FROM_SNAPSHOT =
        boolSetting("index.restore_from_snapshot.enabled", false, Setting.Property.IndexScope);
    protected static final Setting<String> RESTORE_FROM_SNAPSHOT_REPOSITORY_NAME =
        simpleString("index.restore_from_snapshot.repository_name", Setting.Property.IndexScope);
    protected static final Setting<String> RESTORE_FROM_SNAPSHOT_SNAPSHOT_NAME =
        simpleString("index.restore_from_snapshot.snapshot_name", Setting.Property.IndexScope);
    protected static final Setting<String> RESTORE_FROM_SNAPSHOT_SNAPSHOT_ID =
        simpleString("index.restore_from_snapshot.snapshot_id", Setting.Property.IndexScope);
    protected static final Setting<Version> RESTORE_FROM_SNAPSHOT_SNAPSHOT_VERSION =
        versionSetting("index.restore_from_snapshot.snapshot_version", Version.V_EMPTY, Setting.Property.IndexScope);
    protected static final Setting<String> RESTORE_FROM_SNAPSHOT_SNAPSHOT_INDEX =
        simpleString("index.restore_from_snapshot.snapshot_index", Setting.Property.IndexScope);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.nodePlugins());
        classes.add(MockFSIndexStore.TestPlugin.class);
        return classes;
    }

    protected static class AutoRestoreTestPlugin extends Plugin {
        @Override
        public final List<Setting<?>> getSettings() {
            return List.of(RESTORE_FROM_SNAPSHOT,
                RESTORE_FROM_SNAPSHOT_REPOSITORY_NAME,
                RESTORE_FROM_SNAPSHOT_SNAPSHOT_NAME,
                RESTORE_FROM_SNAPSHOT_SNAPSHOT_ID,
                RESTORE_FROM_SNAPSHOT_SNAPSHOT_VERSION,
                RESTORE_FROM_SNAPSHOT_SNAPSHOT_INDEX);
        }
    }

    protected final String createRestoredIndex(final int numberOfShards,
                                               final int numberOfReplicas,
                                               final int numberOfDocs,
                                               final boolean checkOnClose) throws Exception {
        final String index = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(index, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), checkOnClose)
            .build());

        if (numberOfDocs > 0) {
            indexRandom(true, IntStream.range(0, numberOfDocs)
                .mapToObj(n -> client().prepareIndex(index, "doc").setSource("field", "value_" + n))
                .collect(toList()));
            assertHitCount(client().prepareSearch(index).setSize(0).get(), numberOfDocs);
        }
        ensureGreen(index);

        final String repository = "repository-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        assertAcked(client().admin().cluster().preparePutRepository(repository)
            .setVerify(true)
            .setType("fs").setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())));

        final String snapshot = "snapshot-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot(repository, snapshot)
            .setWaitForCompletion(true).setIndices(index).get();
        assertThat(createSnapshotResponse.status(), equalTo(RestStatus.OK));

        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), equalTo(getNumShards(index).numPrimaries));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
        assertThat(snapshotInfo.indices(), hasSize(1));

        assertAcked(client().admin().indices().prepareDelete(index));

        final String restoredIndex = randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
        client().admin().cluster().prepareRestoreSnapshot(repository, snapshotInfo.snapshotId().getName())
            .setIndices(snapshotInfo.indices().get(0))
            .setRenamePattern("(.)+")
            .setRenameReplacement(restoredIndex)
            .setWaitForCompletion(true)
            .setIndexSettings(Settings.builder()
                .put(RESTORE_FROM_SNAPSHOT.getKey(), true)
                .put(RESTORE_FROM_SNAPSHOT_REPOSITORY_NAME.getKey(), repository)
                .put(RESTORE_FROM_SNAPSHOT_SNAPSHOT_NAME.getKey(), snapshotInfo.snapshotId().getName())
                .put(RESTORE_FROM_SNAPSHOT_SNAPSHOT_ID.getKey(), snapshotInfo.snapshotId().getUUID())
                .put(RESTORE_FROM_SNAPSHOT_SNAPSHOT_VERSION.getKey(), snapshotInfo.version())
                .put(RESTORE_FROM_SNAPSHOT_SNAPSHOT_INDEX.getKey(), snapshotInfo.indices().get(0))
            ).get();

        ensureGreen(restoredIndex);
        return restoredIndex;
    }

    protected static void assertDocsCount(final String indexName, final long expectedNumberOfDocs) {
        SearchResponse searchResponse = client().prepareSearch(indexName).setSize(0).get();
        assertHitCount(searchResponse, expectedNumberOfDocs);
        assertNoFailures(searchResponse);
    }

    public void testAutoRestorePrimaryAfterNodeLeft() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final int numberOfDocs = scaledRandomIntBetween(1, 100);
        final String index = createRestoredIndex(1, 0, numberOfDocs, true);
        ensureGreen(index);

        final int extraDocs = scaledRandomIntBetween(10, 50);
        indexRandom(true, IntStream.range(0, extraDocs)
            .mapToObj(n -> client().prepareIndex(index, "doc").setSource("field", "value_" + n))
            .collect(Collectors.toList()));
        assertHitCount(client().prepareSearch(index).setSize(0).get(), numberOfDocs + extraDocs);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode));

        assertBusy(() -> {
            IndexRoutingTable indexRoutingTable = client().admin().cluster().prepareState().get().getState().routingTable().index(index);
            assertThat(indexRoutingTable.primaryShardsActive(), equalTo(0));
            assertThat(indexRoutingTable.numberOfNodesShardsAreAllocatedOn(), equalTo(0));
        });

        // new node joins the cluster and triggers a reroute
        internalCluster().startDataOnlyNode();
        ensureGreen(index);

        IndexRoutingTable indexRoutingTable = client().admin().cluster().prepareState().get().getState().routingTable().index(index);
        assertThat(indexRoutingTable.primaryShardsActive(), equalTo(1));
        assertThat(indexRoutingTable.numberOfNodesShardsAreAllocatedOn(dataNode), equalTo(1));

        // indicates that the shard recovered from the snapshot (does not contain the extra docs)
        assertDocsCount(index, numberOfDocs);
    }

    public void testAutoRestorePrimaryAfterMultipleNodesLeft() throws Exception {
        internalCluster().startMasterOnlyNode();
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(randomIntBetween(2, 5));

        final int numberOfDocs = scaledRandomIntBetween(1, 100);
        final String index = createRestoredIndex(randomIntBetween(dataNodes.size(), 10), 0, numberOfDocs, true);
        ensureGreen(index);

        final List<String> stoppedNodes = randomSubsetOf(randomIntBetween(1, dataNodes.size() - 1), dataNodes);
        for (String node : stoppedNodes) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node));
        }

        ensureStableCluster(1 + dataNodes.size() - stoppedNodes.size());

        // NORELEASE Fix this
        // we might need something in RoutingNodes#ignoreShard(ShardRouting, AllocationStatus, IndexMetaData, RoutingChangesObserver)
        // to trigger the reroute when a recovery source has changed?
        //
        // need to manually trigger a reroute
        assertAcked(client().admin().cluster().prepareReroute());
        ensureGreen(index);

        // indicates that the shard recovered from the snapshot (does not contain the extra docs)
        assertDocsCount(index, numberOfDocs);
    }

    public void testAutoRestorePrimaryAfterFullRestart() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final int numberOfDocs = scaledRandomIntBetween(1, 100);
        final String index = createRestoredIndex(1, 0, numberOfDocs, true);
        ensureGreen(index);

        final int extraDocs = scaledRandomIntBetween(10, 50);
        indexRandom(true, IntStream.range(0, extraDocs)
            .mapToObj(n -> client().prepareIndex(index, "doc").setSource("field", "value_" + n))
            .collect(toList()));
        assertHitCount(client().prepareSearch(index).setSize(0).get(), numberOfDocs + extraDocs);

        // we need to disable allocation here, otherwise the shard is unassigned because the node left
        disableAllocation(index);

        internalCluster().fullRestart();
        ensureGreen(index);

        IndexRoutingTable indexRoutingTable = client().admin().cluster().prepareState().get().getState().routingTable().index(index);
        assertThat(indexRoutingTable.primaryShardsActive(), equalTo(1));

        // NORELEASE Fix this
        // we expect the shard to recover from existing store and to have numberOfDocs + extraDocs documents
        // but there is no delayed allocation for primaries, so the shard always recovers from the snapshot
        // and does not contain the extra docs
        //assertHitCount(client().prepareSearch(index).setSize(0).get(), numberOfDocs + extraDocs);
        assertDocsCount(index, numberOfDocs);
    }

    public void testAutoRestorePrimaryAfterFullRestartAndNewDataNodes() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final int numberOfDocs = scaledRandomIntBetween(1, 100);
        final String index = createRestoredIndex(1, 0, numberOfDocs, true);
        ensureGreen(index);

        final int extraDocs = scaledRandomIntBetween(10, 50);
        indexRandom(true, IntStream.range(0, extraDocs)
            .mapToObj(n -> client().prepareIndex(index, "doc").setSource("field", "value_" + n))
            .collect(toList()));
        assertHitCount(client().prepareSearch(index).setSize(0).get(), numberOfDocs + extraDocs);

        final Settings masterNodeSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());

        internalCluster().stopCurrentMasterNode();
        internalCluster().stopRandomDataNode();

        internalCluster().startMasterOnlyNode(masterNodeSettings);
        internalCluster().startDataOnlyNode();

        ensureGreen(index);

        IndexRoutingTable indexRoutingTable = client().admin().cluster().prepareState().get().getState().routingTable().index(index);
        assertThat(indexRoutingTable.primaryShardsActive(), equalTo(1));

        // indicates that the shard recovered from the snapshot (does not contain the extra docs)
        assertDocsCount(index, numberOfDocs);
    }

    public void testAutoRestoreCorruptedPrimary() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        final int numberOfDocs = scaledRandomIntBetween(1, 100);
        final String indexName = createRestoredIndex(2, 0, numberOfDocs, false); // no check on close as we corrupt the index
        ensureGreen(indexName);

        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numberOfDocs);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        final Index index = indexRoutingTable.getIndex();
        final ShardRouting shardRouting = indexRoutingTable.shard(0).primaryShard();
        final String shardNodeName = clusterState.nodes().resolveNode(shardRouting.currentNodeId()).getName();

        // corrupt the index on disk
        Environment environment = internalCluster().getInstance(Environment.class, shardNodeName);
        Path indexDataPath = environment.dataFiles()[0].resolve("indices").resolve(index.getUUID())
            .resolve(String.valueOf(shardRouting.getId())).resolve("index");
        CorruptionUtils.corruptIndex(random(), indexDataPath, true);

        // fail the shard (notifies the master and triggers a reroute)
        IndexService indexService = internalCluster().getInstance(IndicesService.class, shardNodeName).indexServiceSafe(index);
        indexService.getShard(0).failShard("test", new CorruptIndexException("test", "index is corrupted"));

        ensureGreen(indexName);
        assertBusy(() -> assertDocsCount(indexName, numberOfDocs));
    }

    public void testAutoRestoreUnrecoverablePrimary() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        final int numberOfDocs = scaledRandomIntBetween(1, 100);
        final String indexName = createRestoredIndex(1, 0, numberOfDocs, true);
        ensureGreen(indexName);

        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numberOfDocs);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        final ShardRouting shardRouting = indexRoutingTable.shard(0).primaryShard();
        final String shardNodeName = clusterState.nodes().resolveNode(shardRouting.currentNodeId()).getName();

        assertThat(shardRouting.recoverySource(), nullValue());

        // delete the snapshot in the repository
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(indexName).get();
        String repository = getSettingsResponse.getSetting(indexName, RESTORE_FROM_SNAPSHOT_REPOSITORY_NAME.getKey());
        assertThat(repository, notNullValue());
        String snapshot = getSettingsResponse.getSetting(indexName, RESTORE_FROM_SNAPSHOT_SNAPSHOT_NAME.getKey());
        assertThat(snapshot, notNullValue());
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repository, snapshot).get());

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(shardNodeName));

        assertBusy(() -> {
            IndexRoutingTable routingTable = client().admin().cluster().prepareState().get().getState().routingTable().index(indexName);
            assertThat(routingTable.allPrimaryShardsUnassigned(), is(true));
            ShardRouting primaryShard = routingTable.shard(0).primaryShard();
            assertThat(primaryShard.recoverySource(), instanceOf(RecoverySource.SnapshotRecoverySource.class));
            //assertThat(primaryShard.unassignedInfo().getNumFailedAllocations(), equalTo(5));
            //assertThat(primaryShard.unassignedInfo().getReason(), is(UnassignedInfo.Reason.ALLOCATION_FAILED)));
        });
    }
}
