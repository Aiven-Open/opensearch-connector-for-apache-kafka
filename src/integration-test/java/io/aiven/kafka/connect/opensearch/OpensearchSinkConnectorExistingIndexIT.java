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

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpensearchSinkConnectorExistingIndexIT extends AbstractKafkaConnectIT {

    static final String CONNECTOR_NAME = "existing-index-connector";

    static final String TOPIC_NAME = "topic-for-existing-index";

    static final String EXISTING_INDEX_NAME = "existing-index-target";

    public OpensearchSinkConnectorExistingIndexIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @BeforeEach
    void createIndex() throws Exception {
        opensearchClient.client.indices()
                .create(new CreateIndexRequest(EXISTING_INDEX_NAME), RequestOptions.DEFAULT);
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

        waitForRecords(EXISTING_INDEX_NAME, 10);

        for (final var hit : search(EXISTING_INDEX_NAME)) {
            final var id = (Integer) hit.getSourceAsMap().get("doc_num");
            assertNotNull(id);
            assertTrue(id < 10);
            assertEquals(EXISTING_INDEX_NAME, hit.getIndex());
        }
    }

    @Override
    Map<String, String> connectorProperties() {
        final var props = new HashMap<>(super.connectorProperties());
        props.put(OpensearchSinkConnectorConfig.EXISTING_RESOURCE_TYPE,
                ExistingResourceType.INDEX.toString());
        props.put(OpensearchSinkConnectorConfig.TOPIC_TO_EXISTING_RESOURCE_MAPPING,
                String.format("%s:%s", TOPIC_NAME, EXISTING_INDEX_NAME));
        return Map.copyOf(props);
    }
}
