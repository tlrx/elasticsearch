/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.archive;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class LegacySearchableSnapshotsIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(LegacySearchableSnapshotsIT.class);

    public static final String REPOSITORY = "repository";
    public static final String SNAPSHOT = "snapshot";

    private static final Version VERSION_9 = Version.CURRENT;
    private static final Version VERSION_8 = Version.fromString("8.18.0");
    private static final Version VERSION_7 = Version.fromString("7.17.25");

    public static TemporaryFolder REPOSITORY_PATH = new TemporaryFolder();

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(VERSION_7)
        .nodes(2)
        .setting("path.repo", () -> REPOSITORY_PATH.getRoot().getPath())
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("path.repo", () -> REPOSITORY_PATH.getRoot().getPath())
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(REPOSITORY_PATH).around(cluster);

    private String indexName;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected boolean resetFeatureStates() {
        return false;
    }

    public void testUgrades() throws Exception {

        ////////////////////////////////////////////////////////////////////////////////
        // Create+Snapshot index on VERSION_7
        ////////////////////////////////////////////////////////////////////////////////
        var version = clusterVersions();
        assertThat(version.elasticsearch, equalTo(VERSION_7));

        indexName = randomIdentifier();
        logger.fatal("--> [cluster {}] creating index [{}]", version, indexName);
        createIndex(
            client(),
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );

        int numDocs = randomIntBetween(1, 100);
        logger.fatal("--> [cluster {}] indexing [{}] docs in [{}]", version, numDocs, indexName);

        final var bulks = new StringBuilder();
        IntStream.range(0, numDocs).forEach(n -> bulks.append(Strings.format("""
            {"index":{"_id":"%s","_index":"%s"}}
            {"test":"test"}
            """, n, indexName)));

        var bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulks.toString());
        var bulkResponse = client().performRequest(bulkRequest);
        assertOK(bulkResponse);
        assertThat(entityAsMap(bulkResponse).get("errors"), allOf(notNullValue(), is(false)));

        logger.fatal("--> [cluster {}] registering repository [{}]", version, REPOSITORY);
        registerRepository(
            client(),
            REPOSITORY,
            FsRepository.TYPE,
            true,
            Settings.builder().put("location", REPOSITORY_PATH.getRoot().getPath()).build()
        );

        logger.fatal("--> [cluster {}] creating snapshot [{}]", version, SNAPSHOT);
        createSnapshot(client(), REPOSITORY, SNAPSHOT, true);

        ////////////////////////////////////////////////////////////////////////////////
        // Upgrade cluster to VERSION_8
        ////////////////////////////////////////////////////////////////////////////////
        logger.fatal("--> [cluster {}] upgrading", version);
        cluster.upgradeToVersion(VERSION_8);
        closeClients();
        initClient();

        version = clusterVersions();
        assertThat(version.elasticsearch, equalTo(VERSION_8));

        logger.fatal("--> [cluster {}] upgraded", version);
        ensureGreen(indexName);

        assertThat(indexLuceneVersion(indexName), equalTo(VERSION_7));
        assertDocCount(client(), indexName, numDocs);

        logger.fatal("--> [cluster {}] deleting index [{}]", version, indexName);
        deleteIndex(indexName);

        // Also mount index V7 on V8, expected to work as usual
        var mountedIndexOnV8 = indexName + "-mounted-on-v8";
        logger.fatal("--> [cluster {}] mounting index [{}]", version, mountedIndexOnV8);
        mountIndex(REPOSITORY, SNAPSHOT, indexName, false, mountedIndexOnV8);
        ensureGreen(mountedIndexOnV8);

        assertDocCount(client(), mountedIndexOnV8, numDocs);

        ////////////////////////////////////////////////////////////////////////////////
        // Upgrade cluster to VERSION_9
        ////////////////////////////////////////////////////////////////////////////////
        logger.fatal("--> [cluster {}] upgrading", version);
        cluster.upgradeToVersion(VERSION_9);
        closeClients();
        initClient();

        ensureGreen(mountedIndexOnV8);

        assertThat(indexLuceneVersion(mountedIndexOnV8), equalTo(VERSION_7));
        assertDocCount(client(), mountedIndexOnV8, numDocs);

        updateIndexSettings(mountedIndexOnV8, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(mountedIndexOnV8);

        // Also mount index V7 again on V9
        var mountedIndexOnV9 = indexName + "-mounted-on-v9";
        logger.fatal("--> [cluster {}] mounting index [{}]", version, mountedIndexOnV9);
        mountIndex(REPOSITORY, SNAPSHOT, indexName, false, mountedIndexOnV9);
        ensureGreen(mountedIndexOnV9);

        assertThat(indexLuceneVersion(mountedIndexOnV9), equalTo(VERSION_7));
        assertDocCount(client(), mountedIndexOnV9, numDocs);

        updateIndexSettings(mountedIndexOnV9, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(mountedIndexOnV9);

        logger.fatal("--> [cluster {}] restarting", version);
        cluster.restart(true);
        closeClients();
        initClient();

        ensureGreen(mountedIndexOnV8);

        assertThat(indexLuceneVersion(mountedIndexOnV8), equalTo(VERSION_7));
        assertDocCount(client(), mountedIndexOnV8, numDocs);

        ensureGreen(mountedIndexOnV9);

        assertThat(indexLuceneVersion(mountedIndexOnV9), equalTo(VERSION_7));
        assertDocCount(client(), mountedIndexOnV9, numDocs);

        logger.fatal("--> [cluster {}] done", version);
    }

    record Versions(Version elasticsearch, org.apache.lucene.util.Version lucene) {
        @Override
        public String toString() {
            return '[' + "elasticsearch=" + elasticsearch + ", lucene=" + lucene + ']';
        }
    }

    private static Versions clusterVersions() throws Exception {
        var response = assertOK(client().performRequest(new Request("GET", "/")));
        var responseBody = createFromResponse(response);
        var version = Version.fromString(responseBody.evaluate("version.number").toString());
        var luceneVersion = org.apache.lucene.util.Version.parse(responseBody.evaluate("version.lucene_version").toString());
        return new Versions(version, luceneVersion);
    }

    private static Version indexLuceneVersion(String indexName) throws Exception {
        var response = assertOK(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
        int id = Integer.parseInt(createFromResponse(response).evaluate(indexName + ".settings.index.version.created"));
        return new Version(
            (byte) ((id / 1000000) % 100),
            (byte) ((id / 10000) % 100),
            (byte) ((id / 100) % 100)
        );
    }

    private static void restoreIndex(String repository, String snapshot, String indexName, String renamedIndexName) throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(Strings.format("""
            {
              "indices": "%s",
              "include_global_state": false,
              "rename_pattern": "(.+)",
              "rename_replacement": "%s",
              "include_aliases": false
            }""", indexName, renamedIndexName));
        var responseBody = createFromResponse(client().performRequest(request));
        assertThat(responseBody.evaluate("snapshot.shards.total"), equalTo((int) responseBody.evaluate("snapshot.shards.successful")));
        assertThat(responseBody.evaluate("snapshot.shards.failed"), equalTo(0));
    }

    private static void mountIndex(String repository, String snapshot, String indexName, boolean partial, String renamedIndexName)
        throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_mount");
        request.addParameter("wait_for_completion", "true");
        var storage = partial ? "shared_cache" : "full_copy";
        request.addParameter("storage", storage);
        request.setJsonEntity(Strings.format("""
            {
              "index": "%s",
              "renamed_index": "%s"
            }""", indexName, renamedIndexName));
        var responseBody = createFromResponse(client().performRequest(request));
        assertThat(responseBody.evaluate("snapshot.shards.total"), equalTo((int) responseBody.evaluate("snapshot.shards.successful")));
        assertThat(responseBody.evaluate("snapshot.shards.failed"), equalTo(0));
    }

    private static String debug(Response response) throws IOException {
        return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
    }
}
