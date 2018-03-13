/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.dump;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

class ExportDumpCommand extends DumpCommand {

    private final OptionSpec<String> indicesOption;
    private final OptionSpec<String> templatesOption;

    ExportDumpCommand() {
        super("Export data from Elasticsearch");

        this.indicesOption = parser.accepts("indices",
            "Comma-separated list of the indices to export")
            .withOptionalArg()
            .defaultsTo("_all");

        this.templatesOption = parser.accepts("templates",
            "Comma-separated list of the index templates to export")
            .availableUnless(indicesOption)
            .withOptionalArg()
            .defaultsTo("_all");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (options.has(templatesOption)) {
            terminal.println("export tzemplayte not supoprted");
        } else {
            exportIndices(terminal, options);
        }
    }

    private void exportIndices(final Terminal terminal, final OptionSet options) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(indicesOption.value(options));
        final TimeValue scroll = TimeValue.timeValueMinutes(5);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.version(true);
        searchSourceBuilder.size(500);
        searchSourceBuilder.sort("_doc", SortOrder.ASC);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indices);
        searchRequest.scroll(scroll);
        searchRequest.source(searchSourceBuilder);

        final long limit = options.has(limitOption) ? limitOption.value(options) : Long.MAX_VALUE;

        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create(hostOption.value(options))))) {
            SearchResponse searchResponse = client.search(searchRequest);
            if (searchResponse.getHits().getTotalHits() > 0) {
                String scrollId = searchResponse.getScrollId();
                try {
                    SearchHit[] searchHits = searchResponse.getHits().getHits();

                    long count = 0;
                    do {
                        for (SearchHit hit : searchHits) {
                            if (count >= limit) {
                                return;
                            }

                            terminal.print(Terminal.Verbosity.NORMAL, "{\"index\":{");
                            {
                                terminal.print(Terminal.Verbosity.NORMAL, "\"_index\":\"");
                                terminal.print(Terminal.Verbosity.NORMAL, hit.getIndex());
                                terminal.print(Terminal.Verbosity.NORMAL, "\",");

                                terminal.print(Terminal.Verbosity.NORMAL, "\"_type\":\"");
                                terminal.print(Terminal.Verbosity.NORMAL, hit.getType());
                                terminal.print(Terminal.Verbosity.NORMAL, "\",");

                                terminal.print(Terminal.Verbosity.NORMAL, "\"_id\":\"");
                                terminal.print(Terminal.Verbosity.NORMAL, hit.getId());
                                terminal.print(Terminal.Verbosity.NORMAL, "\"");

                            }
                            terminal.print(Terminal.Verbosity.NORMAL, "}\n");
                            terminal.println(hit.getSourceAsString());
                            count += 1;
                        }

                        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                        scrollRequest.scroll(scroll);

                        searchResponse = client.searchScroll(scrollRequest);
                        scrollId = searchResponse.getScrollId();
                        searchHits = searchResponse.getHits().getHits();

                    } while ((searchHits != null) && (searchHits.length > 0));

                } finally {
                    if (scrollId != null) {
                        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                        clearScrollRequest.addScrollId(scrollId);
                        client.clearScroll(clearScrollRequest);
                    }
                }
            }
        }
    }
}
