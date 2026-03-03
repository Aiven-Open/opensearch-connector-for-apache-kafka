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
package io.aiven.kafka.connect.opensearch;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;

public enum RoutingType {

    NONE("none", "No routing is added"),

    KEY("key", "The routing value is determined by the record’s key"),

    VALUE("value", "The routing value is determined by the record's value");

    private final String name;

    private final String description;

    private RoutingType(final String name, final String description) {
        this.name = name.toLowerCase(Locale.ROOT);
        this.description = description;
    }

    public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
        private final String[] names = Arrays.stream(values()).map(Enum::toString).toArray(String[]::new);
        private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names);

        @Override
        public void ensureValid(final String name, final Object value) {
            if (value instanceof String) {
                final String lowerStringValue = ((String) value).toLowerCase(Locale.ROOT);
                validator.ensureValid(name, lowerStringValue);
            } else {
                validator.ensureValid(name, value);
            }
        }

        // Overridden here so that ConfigDef.toEnrichedRst shows possible values correctly
        @Override
        public String toString() {
            return validator.toString();
        }
    };

    @Override
    public String toString() {
        return name;
    }

    public static String describe() {
        return Arrays.stream(values())
                .map(v -> v.toString() + " : " + v.description)
                .collect(Collectors.joining(", ", "{", "}"));
    }

    public static RoutingType fromString(final String name) {
        for (final RoutingType routingType : RoutingType.values()) {
            if (routingType.name.equalsIgnoreCase(name)) {
                return routingType;
            }
        }
        throw new IllegalArgumentException("Unknown routing type " + name);
    }

}
