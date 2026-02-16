/*
 * Copyright 2026 Aiven Oy
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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DocumentPayload {

    private static final Converter JSON_CONVERTER;
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        JSON_CONVERTER = new JsonConverter();
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public static byte[] buildPayload(final OpenSearchSinkConnectorConfig config, final SinkRecord record) {
        final Schema schema = config.ignoreSchemaFor(record.topic())
                ? record.valueSchema()
                : preProcessSchema(record.valueSchema(), config);

        final Object value = config.ignoreSchemaFor(record.topic())
                ? record.value()
                : preProcessValue(record.value(), record.valueSchema(), schema, config);

        final byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(record.topic(), schema, value);
        return addTimestampToPayload(rawJsonPayload, config, record);
    }

    // We need to pre process the Kafka Connect schema before converting to JSON as OpenSearch
    // expects a different JSON format from the current JSON converter provides. Rather than
    // completely rewrite a converter for OpenSearch, we will refactor the JSON converter to
    // support customized translation. The pre process is no longer needed once we have the JSON
    // converter refactored.
    // visible for testing
    static Schema preProcessSchema(final Schema schema, final OpenSearchSinkConnectorConfig config) {
        if (schema == null) {
            return null;
        }
        // Handle logical types
        final String schemaName = schema.name();
        if (schemaName != null) {
            switch (schemaName) {
                case Decimal.LOGICAL_NAME :
                    return copySchemaBasics(schema, SchemaBuilder.float64(), config).build();
                case Date.LOGICAL_NAME :
                case Time.LOGICAL_NAME :
                case Timestamp.LOGICAL_NAME :
                    return schema;
                default :
                    // User type or unknown logical type
                    break;
            }
        }

        final Schema.Type schemaType = schema.type();
        switch (schemaType) {
            case ARRAY :
                return preProcessArraySchema(schema, config);
            case MAP :
                return preProcessMapSchema(schema, config);
            case STRUCT :
                return preProcessStructSchema(schema, config);
            default :
                return schema;
        }
    }

    private static Schema preProcessArraySchema(final Schema schema, final OpenSearchSinkConnectorConfig config) {
        final Schema valSchema = preProcessSchema(schema.valueSchema(), config);
        return copySchemaBasics(schema, SchemaBuilder.array(valSchema), config).build();
    }

    private static Schema preProcessMapSchema(final Schema schema, final OpenSearchSinkConnectorConfig config) {
        final Schema keySchema = schema.keySchema();
        final Schema valueSchema = schema.valueSchema();
        final String keyName = keySchema.name() == null ? keySchema.type().name() : keySchema.name();
        final String valueName = valueSchema.name() == null ? valueSchema.type().name() : valueSchema.name();
        final Schema preprocessedKeySchema = preProcessSchema(keySchema, config);
        final Schema preprocessedValueSchema = preProcessSchema(valueSchema, config);
        if (config.useCompactMapEntries() && keySchema.type() == Schema.Type.STRING) {
            final SchemaBuilder result = SchemaBuilder.map(preprocessedKeySchema, preprocessedValueSchema);
            return copySchemaBasics(schema, result, config).build();
        }
        final Schema elementSchema = SchemaBuilder.struct()
                .name(keyName + "-" + valueName)
                .field("key", preprocessedKeySchema)
                .field("value", preprocessedValueSchema)
                .build();
        return copySchemaBasics(schema, SchemaBuilder.array(elementSchema), config).build();
    }

    private static Schema preProcessStructSchema(final Schema schema, final OpenSearchSinkConnectorConfig config) {
        final SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct().name(schema.name()), config);
        for (final Field field : schema.fields()) {
            builder.field(field.name(), preProcessSchema(field.schema(), config));
        }
        return builder.build();
    }

    private static SchemaBuilder copySchemaBasics(final Schema source, final SchemaBuilder target,
            final OpenSearchSinkConnectorConfig config) {
        if (source.isOptional()) {
            target.optional();
        }
        if (source.defaultValue() != null && source.type() != Schema.Type.STRUCT) {
            final Object defaultVal = preProcessValue(source.defaultValue(), source, target, config);
            target.defaultValue(defaultVal);
        }
        return target;
    }

    // visible for testing
    static Object preProcessValue(final Object value, final Schema schema, final Schema newSchema,
            final OpenSearchSinkConnectorConfig config) {
        // Handle missing schemas and acceptable null values
        if (schema == null) {
            return value;
        }

        if (value == null) {
            return preProcessNullValue(schema);
        }

        // Handle logical types
        final String schemaName = schema.name();
        if (schemaName != null) {
            final Object result = preProcessLogicalValue(schemaName, value);
            if (result != null) {
                return result;
            }
        }

        final Schema.Type schemaType = schema.type();
        switch (schemaType) {
            case ARRAY :
                return preProcessArrayValue(value, schema, newSchema, config);
            case MAP :
                return preProcessMapValue(value, schema, newSchema, config);
            case STRUCT :
                return preProcessStructValue(value, schema, newSchema, config);
            default :
                return value;
        }
    }

    private static Object preProcessNullValue(final Schema schema) {
        if (schema.defaultValue() != null) {
            return schema.defaultValue();
        }
        if (schema.isOptional()) {
            return null;
        }
        throw new DataException("null value for field that is required and has no default value");
    }

    // @returns the decoded logical value or null if this isn't a known logical type
    private static Object preProcessLogicalValue(final String schemaName, final Object value) {
        switch (schemaName) {
            case Decimal.LOGICAL_NAME :
                return ((BigDecimal) value).doubleValue();
            case Date.LOGICAL_NAME :
            case Time.LOGICAL_NAME :
            case Timestamp.LOGICAL_NAME :
                return value;
            default :
                // User-defined type or unknown built-in
                return null;
        }
    }

    private static Object preProcessArrayValue(final Object value, final Schema schema, final Schema newSchema,
            final OpenSearchSinkConnectorConfig config) {
        final Collection<?> collection = (Collection<?>) value;
        final List<Object> result = new ArrayList<>();
        for (final Object element : collection) {
            result.add(preProcessValue(element, schema.valueSchema(), newSchema.valueSchema(), config));
        }
        return result;
    }

    private static Object preProcessMapValue(final Object value, final Schema schema, final Schema newSchema,
            final OpenSearchSinkConnectorConfig config) {
        final Schema keySchema = schema.keySchema();
        final Schema valueSchema = schema.valueSchema();
        final Schema newValueSchema = newSchema.valueSchema();
        final Map<?, ?> map = (Map<?, ?>) value;
        if (config.useCompactMapEntries() && keySchema.type() == Schema.Type.STRING) {
            final Map<Object, Object> processedMap = new HashMap<>();
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                processedMap.put(preProcessValue(entry.getKey(), keySchema, newSchema.keySchema(), config),
                        preProcessValue(entry.getValue(), valueSchema, newValueSchema, config));
            }
            return processedMap;
        }
        final List<Struct> mapStructs = new ArrayList<>();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            final Struct mapStruct = new Struct(newValueSchema);
            final Schema mapKeySchema = newValueSchema.field("key").schema();
            final Schema mapValueSchema = newValueSchema.field("value").schema();
            mapStruct.put("key", preProcessValue(entry.getKey(), keySchema, mapKeySchema, config));
            mapStruct.put("value", preProcessValue(entry.getValue(), valueSchema, mapValueSchema, config));
            mapStructs.add(mapStruct);
        }
        return mapStructs;
    }

    private static Object preProcessStructValue(final Object value, final Schema schema, final Schema newSchema,
            final OpenSearchSinkConnectorConfig config) {
        final Struct struct = (Struct) value;
        final Struct newStruct = new Struct(newSchema);
        for (final Field field : schema.fields()) {
            final Schema newFieldSchema = newSchema.field(field.name()).schema();
            final Object converted = preProcessValue(struct.get(field), field.schema(), newFieldSchema, config);
            newStruct.put(field.name(), converted);
        }
        return newStruct;
    }

    private static byte[] addTimestampToPayload(final byte[] payload, final OpenSearchSinkConnectorConfig config,
            final SinkRecord record) {
        if (config.dataStreamEnabled()
                && DATA_STREAM_TIMESTAMP_FIELD_DEFAULT.equals(config.dataStreamTimestampField())) {
            try {
                final var json = OBJECT_MAPPER.readTree(payload);
                if (!json.isObject()) {
                    throw new DataException(
                            "JSON payload is a type of " + json.getNodeType() + ". Required is JSON Object.");
                }
                final var rootObject = (ObjectNode) json;
                if (!rootObject.has(DATA_STREAM_TIMESTAMP_FIELD_DEFAULT)) {
                    if (Objects.isNull(record.timestamp())) {
                        throw new DataException("Record timestamp hasn't been set");
                    }
                    rootObject.put(DATA_STREAM_TIMESTAMP_FIELD_DEFAULT, record.timestamp());
                }
                return OBJECT_MAPPER.writeValueAsBytes(json);
            } catch (final IOException e) {
                throw new DataException("Could not parse payload", e);
            }
        } else {
            return payload;
        }
    }

}
