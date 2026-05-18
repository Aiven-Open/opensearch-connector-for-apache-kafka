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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.indices.CreateDataStreamRequest;
import org.opensearch.client.opensearch.indices.DeleteDataStreamRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.PutIndexTemplateRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenSearchSinkConnectorExistingDataStreamIT extends AbstractKafkaConnectIT {
    static final String TOPIC_NAME = "ds-topic";

    static final String CONNECTOR_NAME = "os-ds-sink-connector";

    static final String DATA_STREAM_NAME = "ds-index";

    public OpenSearchSinkConnectorExistingDataStreamIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @BeforeEach
    public void creteDataStream() throws Exception {
        openSearchClient.indices()
                .putIndexTemplate(PutIndexTemplateRequest.builder()
                        .name("ds-index-template")
                        .priority(200)
                        .indexPatterns(DATA_STREAM_NAME)
                        .dataStream(ds -> ds.timestampField(t -> t.name("@timestamp")))
                        .build());
        openSearchClient.indices().createDataStream(CreateDataStreamRequest.builder().name(DATA_STREAM_NAME).build());

    }

    @AfterEach
    public void deleteDataStream() throws Exception {
        openSearchClient.indices().deleteDataStream(DeleteDataStreamRequest.builder().name(DATA_STREAM_NAME).build());
        openSearchClient.indices()
                .deleteIndexTemplate(DeleteIndexTemplateRequest.builder().name("ds-index-template").build());
    }

    @Test
    public void testConnector() throws Exception {
        connect.configureConnector(CONNECTOR_NAME, connectorProperties(TOPIC_NAME));
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10, TOPIC_NAME);

        waitForRecords(DATA_STREAM_NAME, 10);

        final var searchResults = openSearchClient.search(SearchRequest.of(b -> b.index(DATA_STREAM_NAME)), Map.class)
                .hits();
        for (final var hit : searchResults.hits()) {
            final var id = (Integer) hit.source().get("doc_num");
            assertNotNull(id);
            assertTrue(id < 10);
        }
    }
    @Override
    Map<String, String> connectorProperties(String topicName) {
        final var props = new HashMap<>(super.connectorProperties(topicName));
        props.put(OpenSearchSinkConnectorConfig.EXISTING_RESOURCE_TYPE,
                ExistingResourceType.DATA_STREAM.name().toLowerCase());
        props.put(OpenSearchSinkConnectorConfig.TOPIC_TO_EXISTING_RESOURCE_MAPPING,
                String.format("%s:%s", TOPIC_NAME, DATA_STREAM_NAME));
        return Map.copyOf(props);
    }

}
