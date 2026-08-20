/*
 * Copyright 2023 Aiven Oy
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.VersionType;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestBuilderTest {

    private static final String KEY = "key";

    private static final String TOPIC = "topic";

    private static final int PARTITION = 5;

    private static final long OFFSET = 15;

    private static final String INDEX = "index";

    private static final String VALUE = "value";

    @Test
    public void requestWithDocumentIdStrategyNone() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                        OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                        OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG,
                        DocumentIDStrategy.NONE.toString()
                )
        );
        final var deleteRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(null))
                        .withPayload("some_payload")
                        .build();

        assertTrue(deleteRequest instanceof DeleteRequest);
        assertNull(deleteRequest.id(), deleteRequest.id());
        assertEquals(INDEX, deleteRequest.index(), deleteRequest.index());
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());


        final var indexRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(VALUE))
                        .withPayload("some_payload")
                        .build();
        assertTrue(indexRequest instanceof IndexRequest);
        assertNull(indexRequest.id(), indexRequest.id());
        assertEquals(VersionType.INTERNAL, indexRequest.versionType());
        assertEquals(
                "some_payload",
                readPayload(((IndexRequest) indexRequest).source(), "some_payload".length())
        );


    }

    @Test
    public void requestWithDocumentIdStrategyTopicPartitionOffset() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                        OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                        OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG,
                        DocumentIDStrategy.TOPIC_PARTITION_OFFSET.toString()
                )
        );
        final var deleteRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(null))
                        .withPayload("some_payload")
                        .build();

        assertTrue(deleteRequest instanceof DeleteRequest);
        assertEquals(
                String.format("%s+%s+%s", TOPIC, PARTITION, OFFSET),
                deleteRequest.id(),
                deleteRequest.id()
        );
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());

        final var indexRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(VALUE))
                        .withPayload("some_payload_2")
                        .build();

        assertTrue(indexRequest instanceof IndexRequest);
        assertEquals(
                String.format("%s+%s+%s", TOPIC, PARTITION, OFFSET),
                deleteRequest.id(),
                deleteRequest.id()
        );
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());
        assertEquals(DocWriteRequest.OpType.INDEX, indexRequest.opType());
        assertEquals(
                "some_payload_2",
                readPayload(((IndexRequest) indexRequest).source(), "some_payload_2".length())
        );
    }

    @Test
    public void requestWithDocumentIdStrategyRecordKey() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                        OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                        OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG,
                        DocumentIDStrategy.RECORD_KEY.toString()
                )
        );

        final var deleteRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(null))
                        .withPayload("some_payload")
                        .build();
        assertTrue(deleteRequest instanceof DeleteRequest);
        assertEquals(KEY, deleteRequest.id());
        assertEquals(VersionType.EXTERNAL, deleteRequest.versionType());
        assertEquals(OFFSET, deleteRequest.version());

        final var indexRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(VALUE))
                        .withPayload("some_payload")
                        .build();
        assertTrue(indexRequest instanceof IndexRequest);
        assertEquals(KEY, indexRequest.id());
        assertEquals(DocWriteRequest.OpType.INDEX, indexRequest.opType());
        assertEquals(VersionType.EXTERNAL, indexRequest.versionType());
        assertEquals(OFFSET, indexRequest.version());
        assertEquals(
                "some_payload",
                readPayload(((IndexRequest) indexRequest).source(), "some_payload".length())
        );
    }

    @Test
    void testUpsertRequest() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                        OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                            IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT)
//                        OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false"
                )
        );

        final var upsertRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord("aaa"))
                        .withPayload("aaa")
                        .build();

        assertTrue(upsertRequest instanceof UpdateRequest);
        assertEquals(KEY, upsertRequest.id());

        final var updateRequest = (UpdateRequest) upsertRequest;
        assertTrue(updateRequest.docAsUpsert());
        assertEquals(3, updateRequest.retryOnConflict());
        assertEquals("aaa", readPayload(updateRequest.doc().source(), "aaa".length()));
        assertEquals("aaa", readPayload(updateRequest.upsertRequest().source(), "aaa".length()));
    }

    @Test
    void dataStreamRequest() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                        OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true"
                )
        );

        final var deleteRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(null))
                        .withPayload("aaaa")
                        .build();

        assertTrue(deleteRequest instanceof DeleteRequest);
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());

        final var payloadWithoutTimestamp =
                objectMapper.writeValueAsString(
                        objectMapper.createObjectNode()
                                .put("a", "b")
                                .put("c", "d")
                                .put(OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT, "12345")
                );

        final var dataStreamRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(VALUE))
                        .withPayload(payloadWithoutTimestamp)
                        .build();
        assertTrue(dataStreamRequest instanceof IndexRequest);
        assertEquals(DocWriteRequest.OpType.CREATE, dataStreamRequest.opType());
        final var requestPayload = objectMapper.readValue(
                readPayload(
                        ((IndexRequest) dataStreamRequest).source(),
                        payloadWithoutTimestamp.length()
                ),
                new TypeReference<Map<String, String>>() {}
        );
        assertEquals("12345", requestPayload.get("@timestamp"));
    }

    @Test
    void dataStreamRequestWithCustomTimestamp() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                        OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                        OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD, "t"
                )
        );

        final var payloadWithoutTimestamp =
                objectMapper.writeValueAsString(
                        objectMapper.createObjectNode()
                                .put("a", "b")
                                .put("c", "d")
                                .put("t", "12345")
                );

        final var dataStreamRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(VALUE))
                        .withPayload(payloadWithoutTimestamp)
                        .build();
        assertTrue(dataStreamRequest instanceof IndexRequest);
        assertEquals(DocWriteRequest.OpType.CREATE, dataStreamRequest.opType());
        final var requestPayload = objectMapper.readValue(
                readPayload(
                        ((IndexRequest) dataStreamRequest).source(),
                        payloadWithoutTimestamp.length()
                ),
                new TypeReference<Map<String, String>>() {}
        );
        assertEquals("12345", requestPayload.get("t"));
    }

    @Test
    void dataStreamRequestWithEmptyTimestamp() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                        OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true"
                )
        );

        final var payloadWithoutTimestamp =
                objectMapper.writeValueAsString(
                        objectMapper.createObjectNode()
                                .put("a", "b")
                                .put("c", "d")
                );

        final var dataStreamRequest =
                RequestBuilder.builder()
                        .withConfig(config)
                        .withIndex(INDEX)
                        .withSinkRecord(createSinkRecord(VALUE))
                        .withPayload(payloadWithoutTimestamp)
                        .build();
        assertTrue(dataStreamRequest instanceof IndexRequest);
        assertEquals(DocWriteRequest.OpType.CREATE, dataStreamRequest.opType());
        final var requestPayload = objectMapper.readValue(
                readPayload(
                        ((IndexRequest) dataStreamRequest).source(),
                        payloadWithoutTimestamp.length()
                ),
                new TypeReference<Map<String, String>>() {}
        );
        assertNotNull(requestPayload.get(OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT));
    }

    SinkRecord createSinkRecord(final Object value) {
        return new SinkRecord(
                TOPIC,
                PARTITION,
                Schema.STRING_SCHEMA,
                KEY,
                SchemaBuilder
                        .struct()
                        .name("struct")
                        .field("string", Schema.STRING_SCHEMA)
                        .build(),
                value,
                OFFSET,
                System.currentTimeMillis(), TimestampType.CREATE_TIME
        );
    }

    String readPayload(final BytesReference source, final int length) throws IOException {
        try (final var buffer = new ByteArrayOutputStream(length)) {
            source.writeTo(buffer);
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }

}
