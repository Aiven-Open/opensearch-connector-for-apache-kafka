/*
 * Copyright 2021 Aiven Oy
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

import java.util.concurrent.TimeUnit;

import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.DataStreamsStatsRequest;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpensearchSinkDataStreamConnectorIT extends AbstractKafkaConnectIT {

    static final Logger LOGGER = LoggerFactory.getLogger(OpensearchSinkConnectorIT.class);

    static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(60);

    static final String TOPIC_NAME = "ds-topic";

    static final String DATA_STREAM_PREFIX = "os-data-stream";

    static final String DATA_STREAM_PREFIX_WITH_TIMESTAMP = "os-data-stream-ts";

    static final String DATA_STREAM_WITH_PREFIX_INDEX_NAME = String.format("%s-%s", DATA_STREAM_PREFIX, TOPIC_NAME);

    static final String DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME =
            String.format("%s-%s", DATA_STREAM_PREFIX_WITH_TIMESTAMP, TOPIC_NAME);
    static final String CONNECTOR_NAME = "os-ds-sink-connector";

    public OpensearchSinkDataStreamConnectorIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @Test
    void testConnector() throws Exception {
        final var props = connectorProperties();
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10);

        waitForRecords(TOPIC_NAME, 10);

        assertDataStream(TOPIC_NAME);
        assertDocs(TOPIC_NAME, OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT);
    }

    @Test
    void testConnectorWithDataStreamCustomTimestamp() throws Exception {
        final var props = connectorProperties();
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_PREFIX, DATA_STREAM_PREFIX_WITH_TIMESTAMP);
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD, "custom_timestamp");
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);

        for (int i = 0; i < 10; i++) {
            connect.kafka().produce(
                    topicName,
                    String.valueOf(i),
                    String.format("{\"doc_num\":%d, \"custom_timestamp\": %s}", i, System.currentTimeMillis())
            );
        }

        waitForRecords(DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME, 10);

        assertDataStream(DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME);
        assertDocs(DATA_STREAM_PREFIX_WITH_TIMESTAMP_INDEX_NAME, "custom_timestamp");
    }

    @Test
    void testConnectorWithDataStreamPrefix() throws Exception {
        final var props = connectorProperties();
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_PREFIX, DATA_STREAM_PREFIX);
        connect.configureConnector(CONNECTOR_NAME, props);

        waitForConnectorToStart(CONNECTOR_NAME, 1);
        writeRecords(10);
        waitForRecords(DATA_STREAM_WITH_PREFIX_INDEX_NAME, 10);

        assertDataStream(DATA_STREAM_WITH_PREFIX_INDEX_NAME);
        assertDocs(
                DATA_STREAM_WITH_PREFIX_INDEX_NAME,
                OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT
        );
    }

    void assertDataStream(final String dataStreamName) throws Exception {
        final var dsStats = opensearchClient.client.indices()
                .dataStreamsStats(
                        new DataStreamsStatsRequest(dataStreamName), RequestOptions.DEFAULT
                );

        assertEquals(1, dsStats.getDataStreamCount());
        assertEquals(1, dsStats.getBackingIndices());
        assertTrue(dsStats.getDataStreams().containsKey(dataStreamName));
    }

    void assertDocs(final String dataStreamIndexName, final String timestampFieldName) throws Exception {
        for (final var hit : search(dataStreamIndexName)) {
            final var id = (Integer) hit.getSourceAsMap().get("doc_num");
            final var timestamp = (Long) hit.getSourceAsMap().get(timestampFieldName);
            System.out.println(hit.getSourceAsMap());
            assertNotNull(id);
            assertNotNull(timestamp);
            assertTrue(id < 10);
        }
    }

}
