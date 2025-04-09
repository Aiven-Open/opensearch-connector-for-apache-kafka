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
package io.aiven.kafka.connect.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

public class RequestBuilderTest {

    private static final String KEY = "key";

    private static final String TOPIC = "topic";

    private static final int PARTITION = 5;

    private static final long OFFSET = 15;

    private static final String INDEX = "index";

    private static final String VALUE = "value";

    @Test
    public void requestWithDocumentIdStrategyNone() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.NONE.toString()));
        final var deleteRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(null))
                .withPayload("some_payload")
                .build();

        assertTrue(deleteRequest instanceof DeleteRequest);
        assertNull(deleteRequest.id(), deleteRequest.id());
        assertEquals(INDEX, deleteRequest.index(), deleteRequest.index());
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());

        final var indexRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload("some_payload")
                .build();
        assertTrue(indexRequest instanceof IndexRequest);
        assertNull(indexRequest.id(), indexRequest.id());
        assertEquals(VersionType.INTERNAL, indexRequest.versionType());
        assertEquals("some_payload", readPayload(((IndexRequest) indexRequest).source(), "some_payload".length()));

    }

    @Test
    public void requestWithDocumentIdStrategyTopicPartitionOffset() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG,
                DocumentIDStrategy.TOPIC_PARTITION_OFFSET.toString()));
        final var deleteRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(null))
                .withPayload("some_payload")
                .build();

        assertTrue(deleteRequest instanceof DeleteRequest);
        assertEquals(String.format("%s+%s+%s", TOPIC, PARTITION, OFFSET), deleteRequest.id(), deleteRequest.id());
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());

        final var indexRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload("some_payload_2")
                .build();

        assertTrue(indexRequest instanceof IndexRequest);
        assertEquals(String.format("%s+%s+%s", TOPIC, PARTITION, OFFSET), deleteRequest.id(), deleteRequest.id());
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());
        assertEquals(DocWriteRequest.OpType.INDEX, indexRequest.opType());
        assertEquals("some_payload_2", readPayload(((IndexRequest) indexRequest).source(), "some_payload_2".length()));
    }

    @Test
    public void requestWithDocumentIdStrategyRecordKey() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true",
                OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.RECORD_KEY.toString()));

        final var deleteRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(null))
                .withPayload("some_payload")
                .build();
        assertTrue(deleteRequest instanceof DeleteRequest);
        assertEquals(KEY, deleteRequest.id());
        assertEquals(VersionType.EXTERNAL, deleteRequest.versionType());
        assertEquals(OFFSET, deleteRequest.version());

        final var indexRequest = RequestBuilder.builder()
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
        assertEquals("some_payload", readPayload(((IndexRequest) indexRequest).source(), "some_payload".length()));
    }

    @Test
    void testUpsertRequest() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT)
        // OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false"
        ));

        final var upsertRequest = RequestBuilder.builder()
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
        final var config = new OpensearchSinkConnectorConfig(Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true"));

        final var deleteRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(null))
                .withPayload("aaaa")
                .build();

        assertTrue(deleteRequest instanceof DeleteRequest);
        assertEquals(VersionType.INTERNAL, deleteRequest.versionType());

        final var payloadWithoutTimestamp = objectMapper.writeValueAsString(objectMapper.createObjectNode()
                .put("a", "b")
                .put("c", "d")
                .put(OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT, "12345"));

        final var dataStreamRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithoutTimestamp)
                .build();
        assertTrue(dataStreamRequest instanceof IndexRequest);
        assertEquals(DocWriteRequest.OpType.CREATE, dataStreamRequest.opType());
        final var requestPayload = objectMapper.readValue(
                readPayload(((IndexRequest) dataStreamRequest).source(), payloadWithoutTimestamp.length()),
                new TypeReference<Map<String, String>>() {
                });
        assertEquals("12345", requestPayload.get("@timestamp"));
    }

    @Test
    void dataStreamRequestWithCustomTimestamp() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;
        final var config = new OpensearchSinkConnectorConfig(Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD, "t"));

        final var payloadWithoutTimestamp = objectMapper
                .writeValueAsString(objectMapper.createObjectNode().put("a", "b").put("c", "d").put("t", "12345"));

        final var dataStreamRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithoutTimestamp)
                .build();
        assertTrue(dataStreamRequest instanceof IndexRequest);
        assertEquals(DocWriteRequest.OpType.CREATE, dataStreamRequest.opType());
        final var requestPayload = objectMapper.readValue(
                readPayload(((IndexRequest) dataStreamRequest).source(), payloadWithoutTimestamp.length()),
                new TypeReference<Map<String, String>>() {
                });
        assertEquals("12345", requestPayload.get("t"));
    }

    @Test
    void dataStreamRequestWithEmptyTimestamp() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;
        final var config = new OpensearchSinkConnectorConfig(Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true"));

        final var payloadWithoutTimestamp = objectMapper
                .writeValueAsString(objectMapper.createObjectNode().put("a", "b").put("c", "d"));

        final var dataStreamRequest = RequestBuilder.builder()
                .withConfig(config)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithoutTimestamp)
                .build();
        assertTrue(dataStreamRequest instanceof IndexRequest);
        assertEquals(DocWriteRequest.OpType.CREATE, dataStreamRequest.opType());
        final var requestPayload = objectMapper.readValue(
                readPayload(((IndexRequest) dataStreamRequest).source(), payloadWithoutTimestamp.length()),
                new TypeReference<Map<String, String>>() {
                });
        assertNotNull(requestPayload.get(OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT));
    }

    @Test
    void testRoutingFieldExtraction() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;
        final var routingFieldName = "customer_id";
        final var routingFieldValue = "12345";

        // Test with IndexRequest
        final var configForIndex = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_ENABLED_CONFIG, "true",
                OpensearchSinkConnectorConfig.ROUTING_FIELD_PATH_CONFIG, routingFieldName));

        final var payloadWithRoutingField = objectMapper
                .writeValueAsString(objectMapper.createObjectNode()
                        .put("a", "b")
                        .put("c", "d")
                        .put(routingFieldName, routingFieldValue));

        final var indexRequest = RequestBuilder.builder()
                .withConfig(configForIndex)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithRoutingField)
                .build();

        assertTrue(indexRequest instanceof IndexRequest);
        assertEquals(routingFieldValue, ((IndexRequest) indexRequest).routing());

        // Test with UpdateRequest (upsert)
        final var configForUpsert = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_ENABLED_CONFIG, "true",
                OpensearchSinkConnectorConfig.ROUTING_FIELD_PATH_CONFIG, routingFieldName,
                OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD, IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT)));

        final var updateRequest = RequestBuilder.builder()
                .withConfig(configForUpsert)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithRoutingField)
                .build();

        assertTrue(updateRequest instanceof UpdateRequest);
        assertEquals(routingFieldValue, ((UpdateRequest) updateRequest).routing());
    }

    @Test
    void testRoutingDisabledByDefault() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;
        final var routingFieldName = "customer_id";
        final var routingFieldValue = "12345";

        // Test 1: Routing is disabled by default, even if routing.field.path is set
        final var configWithRoutingFieldOnly = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_FIELD_PATH_CONFIG, routingFieldName));

        final var payloadWithRoutingField = objectMapper
                .writeValueAsString(objectMapper.createObjectNode()
                        .put("a", "b")
                        .put("c", "d")
                        .put(routingFieldName, routingFieldValue));

        final var requestWithRoutingDisabled = RequestBuilder.builder()
                .withConfig(configWithRoutingFieldOnly)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithRoutingField)
                .build();

        assertTrue(requestWithRoutingDisabled instanceof IndexRequest);
        // Routing should be null because routing.enabled is false by default
        assertNull(((IndexRequest) requestWithRoutingDisabled).routing());

        // Test 2: Routing is disabled by default, even if routing.key is set to true
        final var configWithRoutingKeyOnly = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_KEY_CONFIG, "true"));

        final var valueJson = objectMapper
                .writeValueAsString(objectMapper.createObjectNode()
                        .put("a", "b")
                        .put("c", "d"));

        final var requestWithRoutingKeyDisabled = RequestBuilder.builder()
                .withConfig(configWithRoutingKeyOnly)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(valueJson))
                .withPayload(valueJson)
                .build();

        assertTrue(requestWithRoutingKeyDisabled instanceof IndexRequest);
        // Routing should be null because routing.enabled is false by default
        assertNull(((IndexRequest) requestWithRoutingKeyDisabled).routing());
    }

    @Test
    void testNestedRoutingFieldPathExtraction() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;

        // Test with nested routing field path
        final var nestedRoutingFieldPath = "customer.id";
        final var routingFieldValue = "12345";

        // Create a configuration with the new routing.field.path parameter
        final var configWithNestedPath = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_ENABLED_CONFIG, "true",
                OpensearchSinkConnectorConfig.ROUTING_FIELD_PATH_CONFIG, nestedRoutingFieldPath));

        // Create a payload with nested fields
        final var customerObject = objectMapper.createObjectNode()
                .put("id", routingFieldValue)
                .put("name", "John Doe")
                .put("email", "john@example.com");

        final var payloadWithNestedField = objectMapper
                .writeValueAsString(objectMapper.createObjectNode()
                        .put("a", "b")
                        .put("c", "d")
                        .set("customer", customerObject));

        final var requestWithNestedPath = RequestBuilder.builder()
                .withConfig(configWithNestedPath)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithNestedField)
                .build();

        assertTrue(requestWithNestedPath instanceof IndexRequest);
        assertEquals(routingFieldValue, ((IndexRequest) requestWithNestedPath).routing());

        // Test with deeply nested routing field path
        final var deeplyNestedPath = "user.address.zip";
        final var zipValue = "10001";

        // Create a configuration with a deeply nested path
        final var configWithDeeplyNestedPath = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_ENABLED_CONFIG, "true",
                OpensearchSinkConnectorConfig.ROUTING_FIELD_PATH_CONFIG, deeplyNestedPath));

        // Create an address object
        final var addressObject = objectMapper.createObjectNode()
                .put("street", "123 Main St")
                .put("city", "New York")
                .put("state", "NY")
                .put("zip", zipValue);

        // Create a user object with the address
        final var userObject = objectMapper.createObjectNode()
                .put("name", "Jane Smith")
                .put("email", "jane@example.com")
                .set("address", addressObject);

        // Create the full payload
        final var payloadWithDeeplyNestedField = objectMapper
                .writeValueAsString(objectMapper.createObjectNode()
                        .put("a", "b")
                        .put("c", "d")
                        .set("user", userObject));

        final var requestWithDeeplyNestedPath = RequestBuilder.builder()
                .withConfig(configWithDeeplyNestedPath)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(VALUE))
                .withPayload(payloadWithDeeplyNestedField)
                .build();

        assertTrue(requestWithDeeplyNestedPath instanceof IndexRequest);
        assertEquals(zipValue, ((IndexRequest) requestWithDeeplyNestedPath).routing());
    }

    @Test
    void testRoutingKeyConfig() throws Exception {
        final var objectMapper = RequestBuilder.OBJECT_MAPPER;

        // Test 1: routing.enabled=true, routing.key=true, routing.field.name=null
        // Should use the entire key as routing
        final var configKeyOnly = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_ENABLED_CONFIG, "true",
                OpensearchSinkConnectorConfig.ROUTING_KEY_CONFIG, "true"));

        final var valueJson = objectMapper
                .writeValueAsString(objectMapper.createObjectNode()
                        .put("a", "b")
                        .put("c", "d"));

        final var requestKeyOnly = RequestBuilder.builder()
                .withConfig(configKeyOnly)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(valueJson))
                .withPayload(valueJson)
                .build();

        assertTrue(requestKeyOnly instanceof IndexRequest);
        // The key is serialized as a JSON string, so it's wrapped in quotes
        assertEquals("\"" + KEY + "\"", ((IndexRequest) requestKeyOnly).routing());

        // Test 2: routing.enabled=true, routing.key=false (default), routing.field.name=customer_id
        // Should extract the field from the value (existing behavior)
        final var routingFieldName = "customer_id";
        final var routingFieldValue = "12345";

        final var configValueWithField = new OpensearchSinkConnectorConfig(Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.ROUTING_ENABLED_CONFIG, "true",
                OpensearchSinkConnectorConfig.ROUTING_FIELD_PATH_CONFIG, routingFieldName));

        final var valueWithRoutingField = objectMapper
                .writeValueAsString(objectMapper.createObjectNode()
                        .put("a", "b")
                        .put("c", "d")
                        .put(routingFieldName, routingFieldValue));

        final var requestValueWithField = RequestBuilder.builder()
                .withConfig(configValueWithField)
                .withIndex(INDEX)
                .withSinkRecord(createSinkRecord(valueWithRoutingField))
                .withPayload(valueWithRoutingField)
                .build();

        assertTrue(requestValueWithField instanceof IndexRequest);
        assertEquals(routingFieldValue, ((IndexRequest) requestValueWithField).routing());
    }

    SinkRecord createSinkRecord(final Object value) {
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, KEY,
                SchemaBuilder.struct().name("struct").field("string", Schema.STRING_SCHEMA).build(), value, OFFSET,
                System.currentTimeMillis(), TimestampType.CREATE_TIME);
    }

    String readPayload(final BytesReference source, final int length) throws IOException {
        try (final var buffer = new ByteArrayOutputStream(length)) {
            source.writeTo(buffer);
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }

}
