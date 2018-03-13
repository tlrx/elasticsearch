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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

class ImportDumpCommand extends DumpCommand {

    private final OptionSpec<Void> indicesOption;
    private final OptionSpec<Void> templatesOption;

    ImportDumpCommand() {
        super("Import data to Elasticsearch");
        this.indicesOption = parser.accepts("indices", "Import indices to Elasticsearch");
        this.templatesOption = parser.accepts("templates", "Import index templates to Elasticsearch")
            .availableUnless(indicesOption);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (options.has(templatesOption)) {
            terminal.println("import templates is not supported");
        } else {
            importIndices(terminal, options);
        }
    }

    InputStream getStdin() {
        return System.in;
    }

    private void importIndices(final Terminal terminal, final OptionSet options) throws IOException {
        final long limit = options.has(limitOption) ? limitOption.value(options) : Long.MAX_VALUE;

        try (RestClient client = RestClient.builder(HttpHost.create(hostOption.value(options))).build()) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(getStdin(), StandardCharsets.UTF_8))) {
                final BytesStreamOutput output = new BytesStreamOutput();

                int batchSize = 500;
                long count = 0;
                boolean metadata = true;
                String line;

                while ((line = reader.readLine()) != null) {
                    output.write(line.getBytes(StandardCharsets.UTF_8));
                    output.write('\n');
                    if (metadata == false) {
                        count = count + 1;
                        if (count % batchSize == 0) {
                            output.write('\n');
                            executeBulk(client, BytesReference.toBytes(output.bytes()));
                            output.reset();
                        }
                        if (count >= limit) {
                            break;
                        }
                    }
                    metadata = !metadata;
                }

                if (output.size() > 0) {
                    output.write('\n');
                    executeBulk(client, BytesReference.toBytes(output.bytes()));
                    output.reset();
                }
                terminal.println("Imported " + count + " documents.");

            }
        }
    }

    private void executeBulk(final RestClient client, final byte[] bytes) throws IOException {
        HttpEntity entity = new ByteArrayEntity(bytes, 0, bytes.length, Request.createContentType(XContentType.JSON));
        client.performRequest("POST", "/_bulk", Collections.emptyMap(), entity);
    }
}
