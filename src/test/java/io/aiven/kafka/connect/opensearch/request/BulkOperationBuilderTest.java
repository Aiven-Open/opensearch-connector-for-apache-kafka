/*
 * Copyright 2023 Aiven Oy
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
package io.aiven.kafka.connect.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.util.ByteArrayBinaryData;

import io.aiven.kafka.connect.opensearch.BehaviorOnNullValues;
import io.aiven.kafka.connect.opensearch.DocumentIDStrategy;
import io.aiven.kafka.connect.opensearch.IndexWriteMethod;
import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;

public class BulkOperationBuilderTest {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String KEY = "key";

    private static final String TOPIC = "topic";

    private static final int PARTITION = 5;

    private static final long OFFSET = 15;

    private static final String VALUE = "value";

    @Test
    public void noRequestWithDocumentIdStrategyNone() throws Exception {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.NONE.toString()));
        assertNull(new BulkOperationBuilder(config).buildFor(createSinkRecord(null)));
    }

    @Test
    public void requestWithDocumentIdStrategyNone() throws Exception {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.NONE.toString()));
        final var bulkOperationBuilder = new BulkOperationBuilder(config);
        final var indexBulkOperation = bulkOperationBuilder.buildFor(createSinkRecord(VALUE));
        assertNotNull(indexBulkOperation);
        assertTrue(indexBulkOperation.isIndex());
        assertNull(indexBulkOperation.index().id(), indexBulkOperation.index().id());
        assertEquals(VersionType.Internal, indexBulkOperation.index().versionType());
        assertEquals("""
                {"string":"value"}""", readPayload((ByteArrayBinaryData) indexBulkOperation.index().document()));
    }

    @Test
    public void requestWithDocumentIdStrategyTopicPartitionOffset() throws Exception {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG,
                DocumentIDStrategy.TOPIC_PARTITION_OFFSET.toString(),
                OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name()));

        final var bulkOperationBuilder = new BulkOperationBuilder(config);

        final var deleteOperation = bulkOperationBuilder.buildFor(createSinkRecord(null));
        assertNotNull(deleteOperation);
        assertTrue(deleteOperation.isDelete());
        assertEquals(String.format("%s+%s+%s", TOPIC, PARTITION, OFFSET), deleteOperation.delete().id(),
                deleteOperation.delete().id());
        assertEquals(VersionType.Internal, deleteOperation.delete().versionType());

        final var indexOperation = bulkOperationBuilder.buildFor(createSinkRecord(VALUE));
        assertNotNull(indexOperation);
        assertTrue(indexOperation.isIndex());
        assertEquals(String.format("%s+%s+%s", TOPIC, PARTITION, OFFSET), indexOperation.index().id(),
                indexOperation.index().id());
        assertEquals(VersionType.Internal, indexOperation.index().versionType());
        assertEquals("""
                {"string":"value"}""", readPayload(((ByteArrayBinaryData) indexOperation.index().document())));
    }

    @Test
    public void requestWithDocumentIdStrategyRecordKey() throws Exception {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.RECORD_KEY.toString(),
                OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name()));

        final var bulkOperationBuilder = new BulkOperationBuilder(config);

        final var deleteOperation = bulkOperationBuilder.buildFor(createSinkRecord(null));
        assertNotNull(deleteOperation);
        assertTrue(deleteOperation.isDelete());
        assertEquals(KEY, deleteOperation.delete().id());
        assertEquals(VersionType.External, deleteOperation.delete().versionType());
        assertEquals(OFFSET, deleteOperation.delete().version());

        final var indexOperation = bulkOperationBuilder.buildFor(createSinkRecord(VALUE));
        assertNotNull(indexOperation);
        assertTrue(indexOperation.isIndex());
        assertEquals(KEY, indexOperation.index().id());
        assertEquals(VersionType.External, indexOperation.index().versionType());
        assertEquals(OFFSET, indexOperation.index().version());
        assertEquals("""
                {"string":"value"}""", readPayload(((ByteArrayBinaryData) indexOperation.index().document())));
    }

    @Test
    void testUpsertRequest() throws Exception {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.INDEX_WRITE_METHOD, IndexWriteMethod.UPSERT.name()));

        final var upsertOperation = new BulkOperationBuilder(config).buildFor(createSinkRecord("aaa"));

        assertNotNull(upsertOperation);
        assertTrue(upsertOperation.isUpdate());
        final var update = upsertOperation.update();
        assertEquals(KEY, update.id());
        assertEquals(3, update.retryOnConflict());

        final var data = ReflectionUtils.tryToReadFieldValue(update.getClass().getDeclaredField("data"), update).get();
        assertTrue((Boolean) ReflectionUtils.tryToReadFieldValue(data.getClass().getDeclaredField("docAsUpsert"), data)
                .get());

        final var documentPayload = readPayload((ByteArrayBinaryData) ReflectionUtils
                .tryToReadFieldValue(data.getClass().getDeclaredField("document"), data)
                .get());
        final var upsertPayload = readPayload((ByteArrayBinaryData) ReflectionUtils
                .tryToReadFieldValue(data.getClass().getDeclaredField("upsert"), data)
                .get());

        assertEquals("""
                {"string":"aaa"}""", documentPayload);
        assertEquals("""
                {"string":"aaa"}""", upsertPayload);
    }

    @Test
    void dataStreamRequest() throws Exception {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.DELETE.name()));

        final var bulkOperationBuilder = new BulkOperationBuilder(config);

        final var deleteOperation = bulkOperationBuilder.buildFor(createSinkRecord(null));

        assertNotNull(deleteOperation);
        assertTrue(deleteOperation.isDelete());

        final var bulkOperation = bulkOperationBuilder.buildFor(recordWithCustomTime(
                Pair.of(OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT, "12345")));
        assertNotNull(bulkOperation);
        assertTrue(bulkOperation.isCreate());

        final var requestPayload = objectMapper.readValue(
                readPayload(((ByteArrayBinaryData) bulkOperation.create().document())),
                new TypeReference<Map<String, String>>() {
                });
        assertEquals("12345", requestPayload.get("@timestamp"));
    }

    @Test
    void dataStreamRequestWithCustomTimestamp() throws Exception {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD, "t"));

        final var dataStreamRequest = new BulkOperationBuilder(config)
                .buildFor(recordWithCustomTime(Pair.of("t", "12345")));
        assertNotNull(dataStreamRequest);
        assertTrue(dataStreamRequest.isCreate());
        final var requestPayload = objectMapper.readValue(
                readPayload(((ByteArrayBinaryData) dataStreamRequest.create().document())),
                new TypeReference<Map<String, String>>() {
                });
        assertEquals("12345", requestPayload.get("t"));
    }

    @Test
    void dataStreamRequestWithEmptyTimestamp() throws Exception {
        final var objectMapper = new ObjectMapper();
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true"));

        final var dataStreamRequest = new BulkOperationBuilder(config).buildFor(createSinkRecord(VALUE));
        assertNotNull(dataStreamRequest);
        assertTrue(dataStreamRequest.isCreate());
        final var requestPayload = objectMapper.readValue(
                readPayload(((ByteArrayBinaryData) dataStreamRequest.create().document())),
                new TypeReference<Map<String, String>>() {
                });
        assertNotNull(requestPayload.get(OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT));
    }

    @Test
    public void ignoreOnNullValue() {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.IGNORE.name()));
        assertNull(new BulkOperationBuilder(config).buildFor(createSinkRecord(null)));
    }

    @Test
    public void failOnNullValue() {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.FAIL.name()));
        assertThrows(DataException.class, () -> new BulkOperationBuilder(config).buildFor(createSinkRecord(null)));
    }

    SinkRecord createSinkRecord(final String value) {
        return createSinkRecord(value, true);
    }

    SinkRecord createSinkRecord(final String value, final boolean withTimestamp) {
        final var valueSchema = SchemaBuilder.struct()
                .name("struct")
                .field("string", SchemaBuilder.string().optional())
                .build();
        final Struct structValue;
        if (value != null) {
            structValue = new Struct(valueSchema);
            structValue.put("string", value);
        } else {
            structValue = null;
        }
        return withTimestamp
                ? new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, KEY, valueSchema, structValue, OFFSET,
                        System.currentTimeMillis(), TimestampType.CREATE_TIME)
                : new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, KEY, valueSchema, structValue, OFFSET);
    }

    String readPayload(final ByteArrayBinaryData source) throws IOException {
        try (final var buffer = new ByteArrayOutputStream()) {
            source.writeTo(buffer);
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }

    SinkRecord recordWithCustomTime(final Pair<String, String> time) {
        final var valueSchema = SchemaBuilder.struct()
                .name("struct")
                .field("a", SchemaBuilder.string().optional())
                .field("c", SchemaBuilder.string().optional())
                .field(time.getKey(), SchemaBuilder.string().optional())
                .build();
        final var structValue = new Struct(valueSchema);
        structValue.put("a", "b");
        structValue.put("c", "d");
        structValue.put(time.getKey(), time.getValue());
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, KEY, valueSchema, structValue, OFFSET);
    }

}
