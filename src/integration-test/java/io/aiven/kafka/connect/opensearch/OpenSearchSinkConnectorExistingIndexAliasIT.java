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
import java.util.List;
import java.util.Map;

import org.opensearch.client.opensearch._types.WaitForActiveShards;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.PutAliasRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenSearchSinkConnectorExistingIndexAliasIT extends AbstractKafkaConnectIT {
    static final String CONNECTOR_NAME = "existing-index-alias-connector";

    static final String TOPIC_NAME = "topic-for-existing-index-alias";

    static final String EXISTING_INDEX_NAME = "existing-index-with-alias";
    static final String EXISTING_INDEX_ALIAS = "existing-index-alias";

    public OpenSearchSinkConnectorExistingIndexAliasIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @BeforeEach
    void createIndex() throws Exception {
        openSearchClient.indices()
                .create(CreateIndexRequest.builder()
                        .index(EXISTING_INDEX_NAME)
                        .waitForActiveShards(WaitForActiveShards.builder().count(1).build())
                        .build());
        openSearchClient.indices()
                .putAlias(PutAliasRequest.builder()
                        .alias(EXISTING_INDEX_ALIAS)
                        .index(List.of(EXISTING_INDEX_NAME))
                        .build());
    }

    @AfterEach
    public void deleteIndex() throws Exception {
        openSearchClient.indices().delete(DeleteIndexRequest.builder().index(EXISTING_INDEX_NAME).build());
    }

    @Test
    public void testConnector() throws Exception {
        connect.configureConnector(CONNECTOR_NAME, connectorProperties(TOPIC_NAME));
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10, TOPIC_NAME);

        waitForRecords(EXISTING_INDEX_ALIAS, 10);

        final var searchResults = openSearchClient
                .search(SearchRequest.of(b -> b.index(EXISTING_INDEX_ALIAS)), Map.class)
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
        props.put(OpenSearchSinkConnectorConfig.EXISTING_RESOURCE_TYPE, ExistingResourceType.INDEX_ALIAS.name());
        props.put(OpenSearchSinkConnectorConfig.TOPIC_TO_EXISTING_RESOURCE_MAPPING,
                String.format("%s:%s", TOPIC_NAME, EXISTING_INDEX_ALIAS));
        return Map.copyOf(props);
    }
}
