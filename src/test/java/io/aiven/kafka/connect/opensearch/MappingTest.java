/*
 * Copyright 2020 Aiven Oy
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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import static io.aiven.kafka.connect.opensearch.Mapping.KEYWORD_TYPE;
import static io.aiven.kafka.connect.opensearch.Mapping.TEXT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MappingTest {

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
        final var schema = SchemaBuilder.struct().name("record")
                .field("string", Schema.STRING_SCHEMA)
                .build();
        final var result = convertSchema(schema);
        final var string = result.getAsJsonObject("properties").getAsJsonObject("string");
        final var keyword = string.getAsJsonObject("fields").getAsJsonObject("keyword");

        assertEquals(TEXT_TYPE, string.get("type").getAsString());
        assertEquals(KEYWORD_TYPE, keyword.get("type").getAsString());
        assertEquals(256, keyword.get("ignore_above").getAsInt());
    }

    @Test
    void buildMappingAndSetDefaultValues() throws IOException {
        final var expectedDate = new java.util.Date();
        final var schema = SchemaBuilder
                .struct()
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
        final var mappingProperties = mapping.getAsJsonObject("properties");

        assertEquals((byte) 1, mappingProperties.getAsJsonObject("int8").get("null_value").getAsByte());
        assertEquals((short) 1, mappingProperties.getAsJsonObject("int16").get("null_value").getAsShort());
        assertEquals(1, mappingProperties.getAsJsonObject("int32").get("null_value").getAsInt());
        assertEquals(1L, mappingProperties.getAsJsonObject("int64").get("null_value").getAsLong());
        assertEquals(1F, mappingProperties.getAsJsonObject("float32").get("null_value").getAsFloat());
        assertEquals(1D, mappingProperties.getAsJsonObject("float64").get("null_value").getAsDouble());
        assertTrue(mappingProperties.getAsJsonObject("boolean").get("null_value").getAsBoolean());
        assertEquals(expectedDate.getTime(), mappingProperties.getAsJsonObject("date").get("null_value").getAsLong());
    }

    @Test
    void buildMappingAndDoNotSetDefaultsForStrings() throws IOException {
        final var schema = SchemaBuilder.struct().name("record")
                .field("string", Schema.STRING_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        final var mapping = convertSchema(schema);
        final var mappingProperties = mapping.getAsJsonObject("properties");
        assertNull(mappingProperties.getAsJsonObject("string").get("null_value"));
        assertNull(mappingProperties.getAsJsonObject("bytes").get("null_value"));
    }

    JsonObject convertSchema(final Schema schema) throws IOException {
        final var builder = Mapping.buildMappingFor(schema);
        builder.flush();
        return (JsonObject) JsonParser.parseString(builder.getOutputStream().toString());
    }

    protected Schema createSchema() {
        return SchemaBuilder.struct().name("record")
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
        return SchemaBuilder.struct().name("inner")
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

    private void verifyMapping(final Schema schema, final JsonObject mapping) {
        final String schemaName = schema.name();

        final Object type = mapping.get("type");
        if (schemaName != null) {
            switch (schemaName) {
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                case Timestamp.LOGICAL_NAME:
                    assertEquals("\"" + Mapping.DATE_TYPE + "\"", type.toString());
                    return;
                case Decimal.LOGICAL_NAME:
                    assertEquals("\"" + Mapping.DOUBLE_TYPE + "\"", type.toString());
                    return;
                default:
            }
        }

        final var props = Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
                RecordConverter.BehaviorOnNullValues.IGNORE.toString(),
                OpensearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, "true"
        );
        final RecordConverter converter = new RecordConverter(new OpensearchSinkConnectorConfig(props));
        final Schema.Type schemaType = schema.type();
        switch (schemaType) {
            case ARRAY:
                verifyMapping(schema.valueSchema(), mapping);
                break;
            case MAP:
                final Schema newSchema = converter.preProcessSchema(schema);
                final JsonObject mapProperties = mapping.get("properties").getAsJsonObject();
                verifyMapping(
                        newSchema.keySchema(),
                        mapProperties.get(Mapping.KEY_FIELD).getAsJsonObject()
                );
                verifyMapping(
                        newSchema.valueSchema(),
                        mapProperties.get(Mapping.VALUE_FIELD).getAsJsonObject()
                );
                break;
            case STRUCT:
                final JsonObject properties = mapping.get("properties").getAsJsonObject();
                for (final Field field : schema.fields()) {
                    verifyMapping(field.schema(), properties.get(field.name()).getAsJsonObject());
                }
                break;
            default:
                assertEquals("\"" + Mapping.schemaTypeToOsType(schemaType) + "\"", type.toString());
        }
    }
}
