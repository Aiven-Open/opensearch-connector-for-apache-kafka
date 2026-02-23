/*
 * Copyright 2019 Aiven Oy
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

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.opensearch.BehaviorOnNullValues;
import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DocumentPayloadTest {

    private OpenSearchSinkConnectorConfig connectorConfig;

    private String key;
    private String topic;
    private int partition;
    private long offset;
    private String index;
    private Schema valueSchema;

    @BeforeEach
    public void setUp() {
        connectorConfig = connectorConfig(true);
        key = "key";
        topic = "topic";
        partition = 0;
        offset = 0;
        index = "index";
        valueSchema = SchemaBuilder.struct().name("struct").field("string", Schema.STRING_SCHEMA).build();
    }

    private OpenSearchSinkConnectorConfig connectorConfig(final boolean useCompactMapEntries) {
        return connectorConfig(useCompactMapEntries, BehaviorOnNullValues.DEFAULT);
    }

    private OpenSearchSinkConnectorConfig connectorConfig(final boolean useCompactMapEntries,
            final BehaviorOnNullValues behaviorOnNullValues) {
        final var props = Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, behaviorOnNullValues.toString(),
                OpenSearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, Boolean.toString(useCompactMapEntries));
        return new OpenSearchSinkConnectorConfig(props);
    }

    @Test
    public void primitives() {
        assertIdenticalAfterPreProcess(Schema.INT8_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.INT16_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.INT32_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.INT64_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.FLOAT32_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.FLOAT64_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.BOOLEAN_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.STRING_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.BYTES_SCHEMA);

        assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT16_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT32_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT64_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.OPTIONAL_FLOAT32_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.OPTIONAL_FLOAT64_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.OPTIONAL_BOOLEAN_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.OPTIONAL_STRING_SCHEMA);
        assertIdenticalAfterPreProcess(Schema.OPTIONAL_BYTES_SCHEMA);

        assertIdenticalAfterPreProcess(SchemaBuilder.int8().defaultValue((byte) 42).build());
        assertIdenticalAfterPreProcess(SchemaBuilder.int16().defaultValue((short) 42).build());
        assertIdenticalAfterPreProcess(SchemaBuilder.int32().defaultValue(42).build());
        assertIdenticalAfterPreProcess(SchemaBuilder.int64().defaultValue(42L).build());
        assertIdenticalAfterPreProcess(SchemaBuilder.float32().defaultValue(42.0f).build());
        assertIdenticalAfterPreProcess(SchemaBuilder.float64().defaultValue(42.0d).build());
        assertIdenticalAfterPreProcess(SchemaBuilder.bool().defaultValue(true).build());
        assertIdenticalAfterPreProcess(SchemaBuilder.string().defaultValue("foo").build());
        assertIdenticalAfterPreProcess(SchemaBuilder.bytes().defaultValue(new byte[0]).build());
    }

    private void assertIdenticalAfterPreProcess(final Schema schema) {
        assertEquals(schema, DocumentPayload.preProcessSchema(schema, connectorConfig));
    }

    @Test
    public void decimal() {
        final Schema origSchema = Decimal.schema(2);
        final Schema preProcessedSchema = DocumentPayload.preProcessSchema(origSchema, connectorConfig);
        assertEquals(Schema.FLOAT64_SCHEMA, preProcessedSchema);
        assertEquals(0.02, DocumentPayload.preProcessValue(new BigDecimal("0.02"), origSchema, preProcessedSchema,
                connectorConfig));
        // optional
        assertEquals(Schema.OPTIONAL_FLOAT64_SCHEMA,
                DocumentPayload.preProcessSchema(Decimal.builder(2).optional().build(), connectorConfig));
        // default
        assertEquals(SchemaBuilder.float64().defaultValue(0.00).build(), DocumentPayload
                .preProcessSchema(Decimal.builder(2).defaultValue(new BigDecimal("0.00")).build(), connectorConfig));
    }

    @Test
    public void array() {
        final Schema origSchema = SchemaBuilder.array(Decimal.schema(2)).schema();
        final Schema preProcessedSchema = DocumentPayload.preProcessSchema(origSchema, connectorConfig);
        assertEquals(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), preProcessedSchema);
        assertEquals(List.of(0.02, 0.42),
                DocumentPayload.preProcessValue(List.of(new BigDecimal("0.02"), new BigDecimal("0.42")), origSchema,
                        preProcessedSchema, connectorConfig));
        // optional
        assertEquals(SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(), DocumentPayload
                .preProcessSchema(SchemaBuilder.array(Decimal.schema(2)).optional().build(), connectorConfig));
        // default value
        assertEquals(
                SchemaBuilder.array(preProcessedSchema.valueSchema()).defaultValue(Collections.emptyList()).build(),
                DocumentPayload.preProcessSchema(
                        SchemaBuilder.array(Decimal.schema(2)).defaultValue(Collections.emptyList()).build(),
                        connectorConfig));
    }

    @Test
    public void map() {
        final Schema origSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).build();
        final Schema preProcessedSchema = DocumentPayload.preProcessSchema(origSchema, connectorConfig);
        assertEquals(SchemaBuilder.array(SchemaBuilder.struct()
                .name(Schema.INT32_SCHEMA.type().name() + "-" + Decimal.LOGICAL_NAME)
                .field("key", Schema.INT32_SCHEMA)
                .field("value", Schema.FLOAT64_SCHEMA)
                .build()).build(), preProcessedSchema);
        final Map<Object, Object> origValue = Map.of(1, new BigDecimal("0.02"), 2, new BigDecimal("0.42"));
        assertEquals(
                Set.of(new Struct(preProcessedSchema.valueSchema()).put("key", 1).put("value", 0.02),
                        new Struct(preProcessedSchema.valueSchema()).put("key", 2).put("value", 0.42)),
                Set.copyOf((List<?>) DocumentPayload.preProcessValue(origValue, origSchema, preProcessedSchema,
                        connectorConfig)));
        // optional
        assertEquals(SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(),
                DocumentPayload.preProcessSchema(
                        SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).optional().build(), connectorConfig));
        // default value
        assertEquals(
                SchemaBuilder.array(preProcessedSchema.valueSchema()).defaultValue(Collections.emptyList()).build(),
                DocumentPayload.preProcessSchema(SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2))
                        .defaultValue(Collections.emptyMap())
                        .build(), connectorConfig));
    }

    @Test
    public void stringKeyedMapNonCompactFormat() {
        final Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
        final Map<Object, Object> origValue = Map.of("field1", 1, "field2", 2);
        // Use the older non-compact format for map entries with string keys
        final var config = connectorConfig(false);
        final Schema preProcessedSchema = DocumentPayload.preProcessSchema(origSchema, config);
        assertEquals(SchemaBuilder.array(SchemaBuilder.struct()
                .name(Schema.STRING_SCHEMA.type().name() + "-" + Schema.INT32_SCHEMA.type().name())
                .field("key", Schema.STRING_SCHEMA)
                .field("value", Schema.INT32_SCHEMA)
                .build()).build(), preProcessedSchema);
        assertEquals(
                Set.of(new Struct(preProcessedSchema.valueSchema()).put("key", "field1").put("value", 1),
                        new Struct(preProcessedSchema.valueSchema()).put("key", "field2").put("value", 2)),
                Set.copyOf(
                        (List<?>) DocumentPayload.preProcessValue(origValue, origSchema, preProcessedSchema, config)));
    }

    @Test
    public void stringKeyedMapCompactFormat() {
        final Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
        final Map<Object, Object> origValue = Map.of("field1", 1, "field2", 2);
        // Use the newer compact format for map entries with string keys
        final Schema preProcessedSchema = DocumentPayload.preProcessSchema(origSchema, connectorConfig);
        assertEquals(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), preProcessedSchema);
        final Map<?, ?> newValue = (Map<?, ?>) DocumentPayload.preProcessValue(origValue, origSchema,
                preProcessedSchema, connectorConfig);
        assertEquals(origValue, newValue);
    }

    @Test
    public void struct() {
        final Schema origSchema = SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).build();
        final Schema preProcessedSchema = DocumentPayload.preProcessSchema(origSchema, connectorConfig);
        assertEquals(SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).build(),
                preProcessedSchema);

        assertEquals(new Struct(preProcessedSchema).put("decimal", 0.02),
                DocumentPayload.preProcessValue(new Struct(origSchema).put("decimal", new BigDecimal("0.02")),
                        origSchema, preProcessedSchema, connectorConfig));

        // optional
        assertEquals(SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).optional().build(),
                DocumentPayload.preProcessSchema(
                        SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).optional().build(),
                        connectorConfig));
    }

    @Test
    public void optionalFieldsWithoutDefaults() {
        // One primitive type should be enough
        testOptionalFieldWithoutDefault(SchemaBuilder.bool(), connectorConfig);
        // Logical types
        testOptionalFieldWithoutDefault(Decimal.builder(2), connectorConfig);
        testOptionalFieldWithoutDefault(Time.builder(), connectorConfig);
        testOptionalFieldWithoutDefault(Timestamp.builder(), connectorConfig);
        // Complex types
        testOptionalFieldWithoutDefault(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA), connectorConfig);
        testOptionalFieldWithoutDefault(SchemaBuilder.struct().field("innerField", Schema.BOOLEAN_SCHEMA),
                connectorConfig);
        testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA),
                connectorConfig);
        // Have to test maps with useCompactMapEntries set to true and set to false
        final var config = connectorConfig(false);
        testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA), config);
    }

    private void testOptionalFieldWithoutDefault(final SchemaBuilder optionalFieldSchema,
            final OpenSearchSinkConnectorConfig config) {
        final Schema origSchema = SchemaBuilder.struct()
                .name("struct")
                .field("optionalField", optionalFieldSchema.optional().build())
                .build();
        final Schema preProcessedSchema = DocumentPayload.preProcessSchema(origSchema, config);

        final Object preProcessedValue = DocumentPayload.preProcessValue(
                new Struct(origSchema).put("optionalField", null), origSchema, preProcessedSchema, config);

        assertEquals(new Struct(preProcessedSchema).put("optionalField", null), preProcessedValue);
    }

    public SinkRecord createSinkRecordWithValue(final Object value) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, valueSchema, value, offset);
    }

}
