/*
 * Copyright 2019 Aiven Oy
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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.action.delete.DeleteRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordConverterTest {

    private RecordConverter converter;
    private String key;
    private String topic;
    private int partition;
    private long offset;
    private String index;
    private Schema schema;

    @BeforeEach
    public void setUp() {
        converter = createDataConverter(true);
        key = "key";
        topic = "topic";
        partition = 0;
        offset = 0;
        index = "index";
        schema = SchemaBuilder
            .struct()
            .name("struct")
            .field("string", Schema.STRING_SCHEMA)
            .build();
    }

    private RecordConverter createDataConverter(final boolean useCompactMapEntries) {
        return createDataConverter(useCompactMapEntries, RecordConverter.BehaviorOnNullValues.DEFAULT);
    }

    private RecordConverter createDataConverter(final boolean useCompactMapEntries,
                                                final RecordConverter.BehaviorOnNullValues behaviorOnNullValues) {
        final var props = Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
                behaviorOnNullValues.toString(),
                OpensearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, Boolean.toString(useCompactMapEntries)
        );
        return new RecordConverter(new OpensearchSinkConnectorConfig(props));
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
        assertEquals(schema, converter.preProcessSchema(schema));
    }

    @Test
    public void decimal() {
        final Schema origSchema = Decimal.schema(2);
        final Schema preProcessedSchema = converter.preProcessSchema(origSchema);
        assertEquals(Schema.FLOAT64_SCHEMA, preProcessedSchema);

        assertEquals(0.02, converter.preProcessValue(new BigDecimal("0.02"), origSchema, preProcessedSchema));

        // optional
        assertEquals(
            Schema.OPTIONAL_FLOAT64_SCHEMA,
            converter.preProcessSchema(Decimal.builder(2).optional().build())
        );

        // default
        assertEquals(
            SchemaBuilder.float64().defaultValue(0.00).build(),
            converter.preProcessSchema(Decimal.builder(2).defaultValue(new BigDecimal("0.00")).build())
        );
    }

    @Test
    public void array() {
        final Schema origSchema = SchemaBuilder.array(Decimal.schema(2)).schema();
        final Schema preProcessedSchema = converter.preProcessSchema(origSchema);
        assertEquals(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), preProcessedSchema);

        assertEquals(
            List.of(0.02, 0.42),
            converter.preProcessValue(
                List.of(new BigDecimal("0.02"), new BigDecimal("0.42")),
                origSchema,
                preProcessedSchema
            )
        );

        // optional
        assertEquals(
            SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(),
            converter.preProcessSchema(SchemaBuilder.array(Decimal.schema(2)).optional().build())
        );

        // default value
        assertEquals(
            SchemaBuilder.array(preProcessedSchema.valueSchema()).defaultValue(Collections.emptyList()).build(),
            converter.preProcessSchema(
                SchemaBuilder.array(Decimal.schema(2))
                    .defaultValue(Collections.emptyList()).build()
            )
        );
    }

    @Test
    public void map() {
        final Schema origSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).build();
        final Schema preProcessedSchema = converter.preProcessSchema(origSchema);
        assertEquals(
            SchemaBuilder.array(
                SchemaBuilder.struct().name(Schema.INT32_SCHEMA.type().name() + "-" + Decimal.LOGICAL_NAME)
                    .field(Mapping.KEY_FIELD, Schema.INT32_SCHEMA)
                    .field(Mapping.VALUE_FIELD, Schema.FLOAT64_SCHEMA)
                    .build()
            ).build(),
            preProcessedSchema
        );

        final Map<Object, Object> origValue = Map.of(1, new BigDecimal("0.02"), 2, new BigDecimal("0.42"));
        assertEquals(
            Set.of(
                    new Struct(preProcessedSchema.valueSchema())
                            .put(Mapping.KEY_FIELD, 1)
                            .put(Mapping.VALUE_FIELD, 0.02),
                    new Struct(preProcessedSchema.valueSchema())
                            .put(Mapping.KEY_FIELD, 2)
                            .put(Mapping.VALUE_FIELD, 0.42)),
            Set.copyOf((List<?>) converter.preProcessValue(origValue, origSchema, preProcessedSchema)));

        // optional
        assertEquals(
            SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(),
            converter.preProcessSchema(SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).optional().build())
        );

        // default value
        assertEquals(
            SchemaBuilder.array(
                preProcessedSchema.valueSchema())
                .defaultValue(Collections.emptyList())
                .build(),
            converter.preProcessSchema(
                SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2))
                    .defaultValue(Collections.emptyMap())
                    .build()
            )
        );
    }

    @Test
    public void stringKeyedMapNonCompactFormat() {
        final Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

        final Map<Object, Object> origValue = Map.of("field1", 1, "field2", 2);

        // Use the older non-compact format for map entries with string keys
        converter = createDataConverter(false);

        final Schema preProcessedSchema = converter.preProcessSchema(origSchema);
        assertEquals(
            SchemaBuilder.array(
                SchemaBuilder.struct().name(
                    Schema.STRING_SCHEMA.type().name()
                        + "-"
                        + Schema.INT32_SCHEMA.type().name()
                ).field(Mapping.KEY_FIELD, Schema.STRING_SCHEMA)
                 .field(Mapping.VALUE_FIELD, Schema.INT32_SCHEMA)
                 .build()
            ).build(),
            preProcessedSchema
        );
        assertEquals(
            Set.of(
                new Struct(preProcessedSchema.valueSchema())
                    .put(Mapping.KEY_FIELD, "field1")
                    .put(Mapping.VALUE_FIELD, 1),
                new Struct(preProcessedSchema.valueSchema())
                    .put(Mapping.KEY_FIELD, "field2")
                    .put(Mapping.VALUE_FIELD, 2)
            ),
            Set.copyOf((List<?>) converter.preProcessValue(origValue, origSchema, preProcessedSchema))
        );
    }

    @Test
    public void stringKeyedMapCompactFormat() {
        final Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

        final Map<Object, Object> origValue = Map.of("field1", 1, "field2", 2);

        // Use the newer compact format for map entries with string keys
        converter = createDataConverter(true);
        final Schema preProcessedSchema = converter.preProcessSchema(origSchema);
        assertEquals(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
            preProcessedSchema
        );
        final Map<?, ?> newValue = (Map<?, ?>)
            converter.preProcessValue(origValue, origSchema, preProcessedSchema);
        assertEquals(origValue, newValue);
    }

    @Test
    public void struct() {
        final Schema origSchema = SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).build();
        final Schema preProcessedSchema = converter.preProcessSchema(origSchema);
        assertEquals(
            SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).build(),
            preProcessedSchema
        );

        assertEquals(
            new Struct(preProcessedSchema).put("decimal", 0.02),
            converter.preProcessValue(
                new Struct(origSchema)
                    .put("decimal", new BigDecimal("0.02")),
                origSchema,
                preProcessedSchema
            )
        );

        // optional
        assertEquals(
            SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).optional().build(),
            converter.preProcessSchema(
                SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2))
                    .optional()
                    .build()
            )
        );
    }

    @Test
    public void optionalFieldsWithoutDefaults() {
        // One primitive type should be enough
        testOptionalFieldWithoutDefault(SchemaBuilder.bool());
        // Logical types
        testOptionalFieldWithoutDefault(Decimal.builder(2));
        testOptionalFieldWithoutDefault(Time.builder());
        testOptionalFieldWithoutDefault(Timestamp.builder());
        // Complex types
        testOptionalFieldWithoutDefault(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA));
        testOptionalFieldWithoutDefault(SchemaBuilder.struct().field("innerField", Schema.BOOLEAN_SCHEMA));
        testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA));
        // Have to test maps with useCompactMapEntries set to true and set to false
        converter = createDataConverter(false);
        testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA));
    }

    private void testOptionalFieldWithoutDefault(final SchemaBuilder optionalFieldSchema) {
        final Schema origSchema = SchemaBuilder.struct().name("struct").field(
            "optionalField", optionalFieldSchema.optional().build()
        ).build();
        final Schema preProcessedSchema = converter.preProcessSchema(origSchema);

        final Object preProcessedValue = converter.preProcessValue(
            new Struct(origSchema).put("optionalField", null), origSchema, preProcessedSchema
        );

        assertEquals(new Struct(preProcessedSchema).put("optionalField", null), preProcessedValue);
    }

    @Test
    public void ignoreOnNullValue() {
        converter = createDataConverter(true, RecordConverter.BehaviorOnNullValues.IGNORE);

        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        assertNull(converter.convert(sinkRecord, index));
    }

    @Test
    public void deleteOnNullValue() {
        converter = createDataConverter(true, RecordConverter.BehaviorOnNullValues.DELETE);

        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        final var deleteRecord = converter.convert(sinkRecord, index);

        assertTrue(deleteRecord instanceof DeleteRequest);
        assertEquals(key, deleteRecord.id());
        assertEquals(index, deleteRecord.index());
    }

    @Test
    public void ignoreDeleteOnNullValueWithNullKey() {
        converter = createDataConverter(true, RecordConverter.BehaviorOnNullValues.DELETE);
        key = null;

        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        assertNull(converter.convert(sinkRecord, index));
    }

    @Test
    public void failOnNullValue() {
        converter = createDataConverter(true, RecordConverter.BehaviorOnNullValues.FAIL);

        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        assertThrows(
                DataException.class,
                () -> converter.convert(sinkRecord, index));
    }

    public SinkRecord createSinkRecordWithValue(final Object value) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, schema, value, offset);
    }

}
