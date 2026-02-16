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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.indices.DataStreamsStatsRequest;
import org.opensearch.client.opensearch.indices.PutIndexTemplateRequest;

import org.junit.jupiter.api.Test;

public class OpenSearchSinkDataStreamConnectorIT extends AbstractKafkaConnectIT {

    static final String TOPIC_NAME = "ds-topic";

    static final String TOPIC_NAME1 = "ds-topic1";

    static final String DATA_STREAM_PREFIX = "os-data-stream";

    static final String DATA_STREAM_PREFIX_WITH_TIMESTAMP = "os-data-stream-ts";

    static final String DATA_STREAM_WITH_PREFIX_INDEX_NAME = String.format("%s-%s", DATA_STREAM_PREFIX, TOPIC_NAME);

    static final String DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME = String.format("%s-%s",
            DATA_STREAM_PREFIX_WITH_TIMESTAMP, TOPIC_NAME);
    static final String CONNECTOR_NAME = "os-ds-sink-connector";

    public OpenSearchSinkDataStreamConnectorIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @Test
    void testConnector() throws Exception {
        final var props = connectorProperties(TOPIC_NAME);
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10, TOPIC_NAME);

        waitForRecords(TOPIC_NAME, 10);

        assertDataStream(TOPIC_NAME);
        assertDocs(TOPIC_NAME, OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT);
    }

    @Test
    void testConnectorWithDataStreamCustomTimestamp() throws Exception {
        final var props = connectorProperties(topicName);
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX, DATA_STREAM_PREFIX_WITH_TIMESTAMP);
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD, "custom_timestamp");
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);

        for (int i = 0; i < 10; i++) {
            connect.kafka()
                    .produce(topicName, String.valueOf(i),
                            String.format("{\"doc_num\":%d, \"custom_timestamp\": %s}", i, System.currentTimeMillis()));
        }

        waitForRecords(DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME, 10);

        assertDataStream(DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME);
        assertDocs(DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME, "custom_timestamp");
    }

    @Test
    void testConnectorWithDataStreamPrefix() throws Exception {
        final var props = connectorProperties(TOPIC_NAME);
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX, DATA_STREAM_PREFIX);
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);
        writeRecords(10, TOPIC_NAME);
        waitForRecords(DATA_STREAM_WITH_PREFIX_INDEX_NAME, 10);

        assertDataStream(DATA_STREAM_WITH_PREFIX_INDEX_NAME);
        assertDocs(DATA_STREAM_WITH_PREFIX_INDEX_NAME,
                OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT);
    }

    /*
     * User provided template doesn't exist, so create one
     */
    @Test
    void testConnectorWithUserProvidedTemplateDoesNotExist() throws Exception {
        final var props = connectorProperties(TOPIC_NAME1);
        connect.kafka().createTopic(TOPIC_NAME1);
        String userProvidedTemplateName = "test-template1";
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_INDEX_TEMPLATE_NAME, userProvidedTemplateName);
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10, TOPIC_NAME1);
        waitForRecords(TOPIC_NAME1, 10);

        // Search for datastreams with topic name, and it should exist
        final var dsStats = opensearchClient.indices()
                .dataStreamsStats(DataStreamsStatsRequest.builder().name(TOPIC_NAME1).build());
        assertEquals(1, dsStats.dataStreamCount());
        deleteTopic(TOPIC_NAME1);

        // Delete datastream
        opensearchClient.indices().deleteDataStream(b -> b.name(List.of(userProvidedTemplateName)));
    }

    // A new template will not be created, as the one provided by user already exists
    @Test
    void testConnectorWithUserProvidedTemplateAlreadyExists() throws Exception {
        final var props = connectorProperties(TOPIC_NAME1);
        connect.kafka().createTopic(TOPIC_NAME1);
        String existingTemplate = "test-template2";
        String dataStream = "test-data-stream_1";
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_INDEX_TEMPLATE_NAME, existingTemplate);
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);
        // make sure index template exists
        createDataStreamAndTemplate(dataStream, existingTemplate);

        writeRecords(10, TOPIC_NAME1);
        waitForRecords(TOPIC_NAME1, 10);

        // Search for datastreams with default index - topic name, and it should not exist
        final var dsStats = opensearchClient.indices()
                .dataStreamsStats(DataStreamsStatsRequest.builder().name(List.of(TOPIC_NAME1)).build());
        assertEquals(1, dsStats.dataStreamCount());
        deleteTopic(TOPIC_NAME1);
    }

    void assertDataStream(final String dataStreamName) throws Exception {
        final var dsStats = opensearchClient.indices()
                .dataStreamsStats(DataStreamsStatsRequest.builder().name(List.of(dataStreamName)).build());

        assertEquals(1, dsStats.dataStreamCount());
        assertEquals(1, dsStats.backingIndices());
        assertEquals(dataStreamName, dsStats.dataStreams().get(dsStats.dataStreamCount() - 1).dataStream());
    }

    void assertDocs(final String dataStreamIndexName, final String timestampFieldName) throws Exception {
        final var searchResults = opensearchClient
                .search(SearchRequest.of(b -> b.index(dataStreamIndexName)), Map.class)
                .hits();
        for (final var hit : searchResults.hits()) {
            final var id = (Integer) hit.source().get("doc_num");
            final var timestamp = (Long) hit.source().get(timestampFieldName);
            assertNotNull(id);
            assertNotNull(timestamp);
            assertTrue(id < 10);
        }
    }

    void createDataStreamAndTemplate(String dataStream, String dataStreamTemplate) throws IOException {
        opensearchClient.indices()
                .putIndexTemplate(PutIndexTemplateRequest.builder()
                        .name(dataStreamTemplate)
                        .indexPatterns(List.of(dataStream, "index-logs-*"))
                        .priority(100)
                        .build());
    }

    void deleteTopic(final String topicName) {
        try (final var admin = connect.kafka().createAdminClient()) {
            final var result = admin.deleteTopics(List.of(topicName));
            result.all().get();
        } catch (final ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
