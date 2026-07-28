/*
 * Copyright 2024 Aiven Oy
 * Copyright 2016 Confluent Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpensearchSinkConnectorExistingIndexAliasIT extends AbstractKafkaConnectIT {

    static final String CONNECTOR_NAME = "existing-index-alias-connector";

    static final String TOPIC_NAME = "topic-for-existing-index-alias";

    static final String EXISTING_INDEX_NAME = "existing-index-with-alias";

    static final String EXISTING_INDEX_ALIAS = "existing-index-alias";

    public OpensearchSinkConnectorExistingIndexAliasIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @BeforeEach
    void createIndexAndAlias() throws Exception {
        opensearchClient.client.indices()
                .create(new CreateIndexRequest(EXISTING_INDEX_NAME), RequestOptions.DEFAULT);
        final var aliasRequest = new IndicesAliasesRequest();
        aliasRequest.addAliasAction(
                IndicesAliasesRequest.AliasActions.add()
                        .index(EXISTING_INDEX_NAME)
                        .alias(EXISTING_INDEX_ALIAS));
        opensearchClient.client.indices().updateAliases(aliasRequest, RequestOptions.DEFAULT);
    }

    @AfterEach
    void deleteIndex() throws Exception {
        opensearchClient.client.indices()
                .delete(new DeleteIndexRequest(EXISTING_INDEX_NAME), RequestOptions.DEFAULT);
    }

    @Test
    public void testConnector() throws Exception {
        connect.configureConnector(CONNECTOR_NAME, connectorProperties());
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10);

        waitForRecords(EXISTING_INDEX_ALIAS, 10);

        for (final var hit : search(EXISTING_INDEX_ALIAS)) {
            final var id = (Integer) hit.getSourceAsMap().get("doc_num");
            assertNotNull(id);
            assertTrue(id < 10);
            // Queries against an alias return hits from the underlying index.
            assertEquals(EXISTING_INDEX_NAME, hit.getIndex());
        }
    }

    @Override
    Map<String, String> connectorProperties() {
        final var props = new HashMap<>(super.connectorProperties());
        props.put(OpensearchSinkConnectorConfig.EXISTING_RESOURCE_TYPE,
                ExistingResourceType.INDEX_ALIAS.toString());
        props.put(OpensearchSinkConnectorConfig.TOPIC_TO_EXISTING_RESOURCE_MAPPING,
                String.format("%s:%s", TOPIC_NAME, EXISTING_INDEX_ALIAS));
        return Map.copyOf(props);
    }
}
