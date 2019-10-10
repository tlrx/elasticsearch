/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.frozen;

import org.apache.lucene.store.Directory;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.FrozenEngine;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.repositories.blobstore.BlobStoreDirectory;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.action.TransportFreezeIndexAction;
import org.elasticsearch.xpack.frozen.rest.action.RestFreezeIndexAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class FrozenIndices extends Plugin implements ActionPlugin, EnginePlugin, IndexStorePlugin {

    public static final String SEARCHABLE_SNAPSHOT_STORE = "searchable_snapshot";

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getValue(FrozenEngine.INDEX_FROZEN)) {
            return Optional.of(FrozenEngine::new);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(FrozenEngine.INDEX_FROZEN,
            BlobStoreDirectory.REPOSITORY_NAME,
            BlobStoreDirectory.REPOSITORY_SNAPSHOT,
            BlobStoreDirectory.REPOSITORY_INDEX,
            BlobStoreDirectory.REPOSITORY_BUFFER
            );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (FrozenEngine.INDEX_FROZEN.get(indexModule.getSettings())) {
            indexModule.addSearchOperationListener(new FrozenEngine.ReacquireEngineSearcherListener());
        }
        super.onIndexModule(indexModule);
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(SEARCHABLE_SNAPSHOT_STORE, new DirectoryFactory() {
            @Override
            public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
                return newDirectory(indexSettings, shardPath, null);
            }

            @Override
            public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath, RepositoriesService repositories) throws IOException {
                //assert FrozenEngine.INDEX_FROZEN.get(indexSettings.getSettings());
                return new BlobStoreDirectory(indexSettings, shardPath, repositories);
            }
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(XPackUsageFeatureAction.FROZEN_INDICES, FrozenIndicesUsageTransportAction.class));
        actions.add(new ActionHandler<>(XPackInfoFeatureAction.FROZEN_INDICES, FrozenIndicesInfoTransportAction.class));
        actions.add(new ActionHandler<>(FreezeIndexAction.INSTANCE, TransportFreezeIndexAction.class));
        return actions;
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Collections.singletonList(new RestFreezeIndexAction(restController));
    }
}
