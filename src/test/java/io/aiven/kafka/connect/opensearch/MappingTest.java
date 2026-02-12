/*
 * Copyright 2020 Aiven Oy
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class MappingTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void throwsDataExceptionForNullSchema() {
        assertThrows(DataException.class, () -> Mapping.buildMappingFor(null));
    }

    @Test
    void buildMapping() throws IOException {
        final Schema schema = createSchema();
        verifyMapping(schema, convertSchema(schema));
    }

    @Test
    void buildMappingForStringSchema() throws IOException {
        final var schema = SchemaBuilder.struct().name("record").field("string", Schema.STRING_SCHEMA).build();
        final var result = convertSchema(schema);
        final var string = result.get("properties").get("string");
        final var keyword = string.get("fields").get("keyword");

        assertEquals("text", string.get("type").asText());
        assertEquals("keyword", keyword.get("type").asText());
        assertEquals(256, keyword.get("ignore_above").asInt());
    }

    @Test
    void buildMappingAndSetDefaultValues() throws IOException {
        final var expectedDate = new java.util.Date();
        final var schema = SchemaBuilder.struct()
                .name("record")
                .field("boolean", SchemaBuilder.bool().defaultValue(true).build())
                .field("int8", SchemaBuilder.int8().defaultValue((byte) 1).build())
                .field("int16", SchemaBuilder.int16().defaultValue((short) 1).build())
                .field("int32", SchemaBuilder.int32().defaultValue(1).build())
                .field("int64", SchemaBuilder.int64().defaultValue(1L).build())
                .field("float32", SchemaBuilder.float32().defaultValue(1F).build())
                .field("float64", SchemaBuilder.float64().defaultValue(1D).build())
                .field("date", Date.builder().defaultValue(expectedDate).build())
                .build();

        final var mapping = convertSchema(schema);
        final var mappingProperties = mapping.get("properties");

        assertEquals((byte) 1, (byte) mappingProperties.get("int8").get("null_value").asInt());
        assertEquals((short) 1, (short) mappingProperties.get("int16").get("null_value").asInt());
        assertEquals(1, mappingProperties.get("int32").get("null_value").asInt());
        assertEquals(1L, mappingProperties.get("int64").get("null_value").asLong());
        assertEquals(1F, (float) mappingProperties.get("float32").get("null_value").asDouble());
        assertEquals(1D, mappingProperties.get("float64").get("null_value").asDouble());
        assertTrue(mappingProperties.get("boolean").get("null_value").asBoolean());
        assertEquals(expectedDate.getTime(), mappingProperties.get("date").get("null_value").asLong());
    }

    @Test
    void buildMappingAndDoNotSetDefaultsForStrings() throws IOException {
        final var schema = SchemaBuilder.struct()
                .name("record")
                .field("string", Schema.STRING_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        final var mapping = convertSchema(schema);
        final var mappingProperties = mapping.get("properties");
        assertNull(mappingProperties.get("string").get("null_value"));
        assertNull(mappingProperties.get("bytes").get("null_value"));
    }

    JsonNode convertSchema(final Schema schema) throws IOException {
        final var v = Mapping.buildMappingFor(schema);
        return MAPPER.readTree(v);
    }

    protected Schema createSchema() {
        return SchemaBuilder.struct()
                .name("record")
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("int8", Schema.INT8_SCHEMA)
                .field("int16", Schema.INT16_SCHEMA)
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
                .field("struct", createInnerSchema())
                .field("decimal", Decimal.schema(2))
                .field("date", Date.SCHEMA)
                .field("time", Time.SCHEMA)
                .field("timestamp", Timestamp.SCHEMA)
                .build();
    }

    private Schema createInnerSchema() {
        return SchemaBuilder.struct()
                .name("inner")
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("int8", Schema.INT8_SCHEMA)
                .field("int16", Schema.INT16_SCHEMA)
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
                .field("decimal", Decimal.schema(2))
                .field("date", Date.SCHEMA)
                .field("time", Time.SCHEMA)
                .field("timestamp", Timestamp.SCHEMA)
                .build();
    }

    private void verifyMapping(final Schema schema, final JsonNode mapping) {
        final String schemaName = schema.name();

        final Object type = mapping.get("type");
        if (schemaName != null) {
            switch (schemaName) {
                case Date.LOGICAL_NAME :
                case Time.LOGICAL_NAME :
                case Timestamp.LOGICAL_NAME :
                    assertEquals("\"date\"", type.toString());
                    return;
                case Decimal.LOGICAL_NAME :
                    assertEquals("\"double\"", type.toString());
                    return;
                default :
            }
        }

        final var props = Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.IGNORE.toString(),
                OpenSearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true");
        // final RecordConverter converter = new RecordConverter(new OpenSearchSinkConnectorConfig(props));
        // final Schema.Type schemaType = schema.type();
        // switch (schemaType) {
        // case ARRAY :
        // verifyMapping(schema.valueSchema(), mapping);
        // break;
        // case MAP :
        // final Schema newSchema = converter.preProcessSchema(schema);
        // final var mapProperties = mapping.get("properties");
        // verifyMapping(newSchema.keySchema(), mapProperties.get("key"));
        // verifyMapping(newSchema.valueSchema(), mapProperties.get("value"));
        // break;
        // case STRUCT :
        // final var properties = mapping.get("properties");
        // for (final Field field : schema.fields()) {
        // verifyMapping(field.schema(), properties.get(field.name()));
        // }
        // break;
        // default :
        // assertEquals("\"" + Mapping.schemaTypeToOsType(schemaType) + "\"", type.toString());
        // }
    }
}
