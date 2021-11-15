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
import java.util.Objects;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;

public class Mapping {

    public static final String BOOLEAN_TYPE = "boolean";

    public static final String BYTE_TYPE = "byte";

    public static final String BINARY_TYPE = "binary";

    public static final String SHORT_TYPE = "short";

    public static final String INTEGER_TYPE = "integer";

    public static final String LONG_TYPE = "long";

    public static final String FLOAT_TYPE = "float";

    public static final String DOUBLE_TYPE = "double";

    public static final String STRING_TYPE = "string";

    public static final String TEXT_TYPE = "text";

    public static final String KEYWORD_TYPE = "keyword";

    public static final String DATE_TYPE = "date";

    private static final String DEFAULT_VALUE_FIELD = "null_value";

    private static final String FIELDS_FIELD = "fields";

    private static final String IGNORE_ABOVE_FIELD = "ignore_above";

    public static final String KEY_FIELD = "key";

    private static final String KEYWORD_FIELD = "keyword";

    private static final String PROPERTIES_FIELD = "properties";

    private static final String TYPE_FIELD = "type";

    public static final String VALUE_FIELD = "value";

    public static XContentBuilder buildMappingFor(final Schema schema) {
        if (Objects.isNull(schema)) {
            throw new DataException("Cannot convert schema to mapping without schema    ");
        }
        try {
            final var builder = XContentFactory.jsonBuilder();
            builder.startObject();
            buildMapping(schema, builder);
            builder.endObject();
            return builder;
        } catch (final IOException e) {
            throw new ConnectException("Failed to convert schema to mapping. Schema is " + schema, e);
        }
    }

    private static XContentBuilder buildMapping(
            final Schema schema,
            final XContentBuilder builder) throws IOException {
        final var logicalConversion = logicalMapping(schema, builder);
        if (Objects.nonNull(logicalConversion)) {
            return builder;
        }

        switch (schema.type()) {
            case ARRAY:
                return buildMapping(schema.valueSchema(), builder);
            case MAP:
                return buildMap(schema, builder);
            case STRUCT:
                return buildStruct(schema, builder);
            default:
                return buildPrimitive(builder, schemaTypeToOsType(schema.type()), schema.defaultValue());
        }
    }

    private static XContentBuilder logicalMapping(
            final Schema schema,
            final XContentBuilder builder) throws IOException {
        if (Objects.isNull(schema.name())) {
            return null;
        }
        switch (schema.name()) {
            case Date.LOGICAL_NAME:
            case Time.LOGICAL_NAME:
            case Timestamp.LOGICAL_NAME:
                return buildPrimitive(builder, DATE_TYPE, schema.defaultValue());
            case Decimal.LOGICAL_NAME:
                return buildPrimitive(builder, DOUBLE_TYPE, schema.defaultValue());
            default:
                // User-defined type or unknown built-in
                return null;
        }
    }

    private static XContentBuilder buildMap(final Schema schema, final XContentBuilder builder) throws IOException {
        builder.startObject(PROPERTIES_FIELD);

        builder.startObject(KEY_FIELD);
        buildMapping(schema.keySchema(), builder);
        builder.endObject();

        builder.startObject(VALUE_FIELD);
        buildMapping(schema.valueSchema(), builder);
        builder.endObject();

        return builder.endObject();
    }

    private static XContentBuilder buildStruct(
            final Schema schema, final XContentBuilder builder) throws IOException {
        builder.startObject(PROPERTIES_FIELD);
        for (final var field : schema.fields()) {
            builder.startObject(field.name());
            buildMapping(field.schema(), builder);
            builder.endObject();
        }
        return builder.endObject();
    }

    // visible for testing
    protected static String schemaTypeToOsType(final Schema.Type schemaType) {
        switch (schemaType) {
            case BOOLEAN:
                return BOOLEAN_TYPE;
            case INT8:
                return BYTE_TYPE;
            case INT16:
                return SHORT_TYPE;
            case INT32:
                return INTEGER_TYPE;
            case INT64:
                return LONG_TYPE;
            case FLOAT32:
                return FLOAT_TYPE;
            case FLOAT64:
                return DOUBLE_TYPE;
            case STRING:
                return TEXT_TYPE;
            case BYTES:
                return BINARY_TYPE;
            default:
                return null;
        }
    }

    private static XContentBuilder buildPrimitive(
            final XContentBuilder builder,
            final String type,
            final Object defaultValue) throws IOException {
        if (type == null) {
            throw new DataException("Invalid primitive type");
        }
        builder.field(TYPE_FIELD, type);

        if (type.equals(TEXT_TYPE)) {
            addTextMapping(builder);
        }

        if (Objects.isNull(defaultValue)) {
            return builder;
        }

        switch (type) {
            case BYTE_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, (byte) defaultValue);
            case SHORT_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, (short) defaultValue);
            case INTEGER_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, (int) defaultValue);
            case LONG_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, (long) defaultValue);
            case FLOAT_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, (float) defaultValue);
            case DOUBLE_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, (double) defaultValue);
            case BOOLEAN_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, (boolean) defaultValue);
            case DATE_TYPE:
                return builder.field(DEFAULT_VALUE_FIELD, ((java.util.Date) defaultValue).getTime());
            case STRING_TYPE:
            case TEXT_TYPE:
            case BINARY_TYPE:
                // IGNORE default values for text and binary types as this is not supported by OS side.
                return builder;
            default:
                throw new DataException("Invalid primitive type " + type);
        }
    }

    private static void addTextMapping(final XContentBuilder builder) throws IOException {
        // Add additional mapping for indexing, per https://www.elastic.co/blog/strings-are-dead-long-live-strings
        builder.startObject(FIELDS_FIELD);
        builder.startObject(KEYWORD_FIELD);
        builder.field(TYPE_FIELD, KEYWORD_TYPE);
        builder.field(IGNORE_ABOVE_FIELD, 256);
        builder.endObject();
        builder.endObject();
    }

}
