/*
 * Copyright 2021 Aiven Oy
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch.core.SearchRequest;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchSinkConnectorIT extends AbstractKafkaConnectIT {

    static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSinkConnectorIT.class);

    static final String CONNECTOR_NAME = "os-sink-connector";

    static final String TOPIC_NAME = "os-topic";

    public OpenSearchSinkConnectorIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @Test
    public void testConnector() throws Exception {
        connect.configureConnector(CONNECTOR_NAME, connectorProperties(TOPIC_NAME));
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10, TOPIC_NAME);

        waitForRecords(TOPIC_NAME, 10);

        final var searchResults = opensearchClient.search(SearchRequest.of(b -> b.index(TOPIC_NAME)), Map.class).hits();
        for (final var hit : searchResults.hits()) {
            final var id = (Integer) hit.source().get("doc_num");
            assertNotNull(id);
            assertTrue(id < 10);
            assertEquals(TOPIC_NAME, hit.index());
        }
        assertEquals(DynamicMapping.True,
                opensearchClient.indices()
                        .getMapping(b -> b.index(List.of(TOPIC_NAME)))
                        .get(TOPIC_NAME)
                        .mappings()
                        .dynamic());
    }

    @Test
    public void testConnectorConfig() throws Exception {
        assertEquals(connect.validateConnectorConfig("io.aiven.kafka.connect.opensearch.OpenSearchSinkConnector",
                Map.of("connector.class", "io.aiven.kafka.connect.opensearch.OpenSearchSinkConnector", "topics",
                        "example-topic-name", "name", "test-connector-name"))
                .errorCount(), 1);
    }

}
