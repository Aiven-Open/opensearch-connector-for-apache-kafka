/*
 * Copyright 2026 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.kafka.connect.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch.indices.PutIndexTemplateRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenSearchSinkRoutingConnectorIT extends AbstractKafkaConnectIT {

    static final String CONNECTOR_NAME = "os-sink-connector";

    static final String TOPIC_NAME = "os-routing-topic";

    static final String INDEX_TEMPLATE_NAME = "two_shards_template";

    public OpenSearchSinkRoutingConnectorIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @BeforeEach
    void createTemplate() throws Exception {
        opensearchClient.indices()
                .putIndexTemplate(PutIndexTemplateRequest.builder()
                        .name(INDEX_TEMPLATE_NAME)
                        .indexPatterns(List.of(TOPIC_NAME))
                        .template(b -> b.settings(IndexSettings.builder().numberOfShards(2).build()))
                        .build());
    }

    @AfterEach
    void deleteTemplate() throws Exception {
        opensearchClient.indices().deleteIndexTemplate(b -> b.name(INDEX_TEMPLATE_NAME));
    }

    @Test
    public void testKeyRoutingConnector() throws Exception {
        final var props = connectorProperties(TOPIC_NAME);
        props.put(OpenSearchSinkConnectorConfig.ROUTING_TYPE, RoutingType.KEY.toString());
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);

        for (var i = 0; i < 5; i++) {
            writeRecord(TOPIC_NAME, "0", i);
        }
        for (var i = 0; i < 5; i++) {
            writeRecord(TOPIC_NAME, "1", i + 5);
        }

        waitForRecords(TOPIC_NAME, 10);
        assertRoutingRecords();
    }

    @Test
    public void testValueRoutingConnector() throws Exception {
        final var props = connectorProperties(TOPIC_NAME);
        props.put(OpenSearchSinkConnectorConfig.ROUTING_TYPE, RoutingType.VALUE.toString());
        props.put(OpenSearchSinkConnectorConfig.ROUTING_RECORD_VALUE_PATH, "/r");
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);

        for (var i = 0; i < 5; i++) {
            writeRecord(TOPIC_NAME, String.valueOf(i), String.format("{\"r\":0, \"doc_num\":%d}", i));
        }
        for (var i = 0; i < 5; i++) {
            writeRecord(TOPIC_NAME, String.valueOf(i), String.format("{\"r\":1, \"doc_num\":%d}", i + 5));
        }

        waitForRecords(TOPIC_NAME, 10);
        assertRoutingRecords();
    }

    void assertRoutingRecords() throws Exception {
        var searchResults = opensearchClient.search(SearchRequest.of(b -> b.index(TOPIC_NAME).routing("0")), Map.class)
                .hits();
        var counter = 0;
        for (final var hit : searchResults.hits()) {
            assertEquals("0", hit.routing());
            assertEquals(String.valueOf(counter++), hit.source().get("doc_num").toString());
        }

        searchResults = opensearchClient.search(SearchRequest.of(b -> b.index(TOPIC_NAME).routing("1")), Map.class)
                .hits();
        for (final var hit : searchResults.hits()) {
            assertEquals("1", hit.routing());
            assertEquals(String.valueOf(counter++), hit.source().get("doc_num").toString());
        }
    }

}
