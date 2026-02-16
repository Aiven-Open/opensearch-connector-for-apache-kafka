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

import static java.util.Objects.nonNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Mapping {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Set<String> LOGICAL_NAMES = Set.of(Date.LOGICAL_NAME, Time.LOGICAL_NAME,
            Timestamp.LOGICAL_NAME, Decimal.LOGICAL_NAME);

    public static ByteArrayInputStream buildMappingFor(final Schema schema) {
        if (Objects.isNull(schema)) {
            throw new DataException("Cannot convert schema to mapping without schema    ");
        }
        try {
            final var rootNode = MAPPER.createObjectNode();
            mapping(schema, rootNode);
            return new ByteArrayInputStream(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(rootNode));
        } catch (final IOException e) {
            throw new ConnectException("Failed to convert schema to mapping. Schema is " + schema, e);
        }
    }

    private static void mapping(final Schema schema, final ObjectNode rootNode) {
        if (nonNull(schema.name()) && LOGICAL_NAMES.contains(schema.name())) {
            switch (schema.name()) {
                case Date.LOGICAL_NAME :
                case Time.LOGICAL_NAME :
                case Timestamp.LOGICAL_NAME :
                    primitiveMapping(rootNode, "date", schema.defaultValue());
                    break;
                case Decimal.LOGICAL_NAME :
                    primitiveMapping(rootNode, "double", schema.defaultValue());
                    break;
                default :
                    break;
            }
        } else {
            switch (schema.type()) {
                case ARRAY :
                    mapping(schema.valueSchema(), rootNode);
                    break;
                case MAP :
                    mapMapping(schema, rootNode);
                    break;
                case STRUCT :
                    structMapping(schema, rootNode);
                    break;
                default :
                    primitiveMapping(rootNode, schemaTypeToOsType(schema.type()), schema.defaultValue());
                    break;
            }
        }
    }

    private static void mapMapping(final Schema schema, final ObjectNode rootNode) {
        final var keyNode = MAPPER.createObjectNode();
        mapping(schema.keySchema(), keyNode);

        final var valueNode = MAPPER.createObjectNode();
        mapping(schema.valueSchema(), valueNode);

        final var propertiesNode = MAPPER.createObjectNode();
        propertiesNode.set("key", keyNode);
        propertiesNode.set("value", valueNode);
        rootNode.set("properties", propertiesNode);
    }

    private static void structMapping(final Schema schema, final ObjectNode rootNode) {
        final var propertiesNode = MAPPER.createObjectNode();
        schema.fields().forEach(field -> {
            final var fieldNode = MAPPER.createObjectNode();
            mapping(field.schema(), fieldNode);
            propertiesNode.set(field.name(), fieldNode);
        });
        rootNode.set("properties", propertiesNode);
    }

    protected static String schemaTypeToOsType(final Schema.Type schemaType) {
        switch (schemaType) {
            case BOOLEAN :
                return "boolean";
            case INT8 :
                return "byte";
            case INT16 :
                return "short";
            case INT32 :
                return "integer";
            case INT64 :
                return "long";
            case FLOAT32 :
                return "float";
            case FLOAT64 :
                return "double";
            case STRING :
                return "text";
            case BYTES :
                return "binary";
            default :
                return null;
        }
    }

    private static void primitiveMapping(final ObjectNode rootNode, final String type, final Object defaultValue) {
        if (type == null) {
            throw new DataException("Invalid primitive type");
        }
        final var typeNode = MAPPER.createObjectNode();
        typeNode.put("type", type);
        if ("text".equals(type)) {
            textMapping(typeNode);
        } else if (nonNull(defaultValue)) {
            switch (type) {
                case "byte" :
                    typeNode.put("null_value", (byte) defaultValue);
                    break;
                case "short" :
                    typeNode.put("null_value", (short) defaultValue);
                    break;
                case "integer" :
                    typeNode.put("null_value", (int) defaultValue);
                    break;
                case "long" :
                    typeNode.put("null_value", (long) defaultValue);
                    break;
                case "float" :
                    typeNode.put("null_value", (float) defaultValue);
                    break;
                case "double" :
                    typeNode.put("null_value", (double) defaultValue);
                    break;
                case "boolean" :
                    typeNode.put("null_value", (boolean) defaultValue);
                    break;
                case "date" :
                    typeNode.put("null_value", ((java.util.Date) defaultValue).getTime());
                    break;
                case "string" :
                case "text" :
                case "binary" :
                    // IGNORE default values for text and binary types as this is not supported by OS side.
                    break;
                default :
                    throw new DataException("Invalid primitive type " + type);
            }
        }
        rootNode.setAll(typeNode);
    }

    private static void textMapping(final ObjectNode rootNode) {
        final var keywordNode = MAPPER.createObjectNode();
        keywordNode.put("type", "keyword").put("ignore_above", 256);
        rootNode.set("fields", MAPPER.createObjectNode().set("keyword", keywordNode));
    }

}
