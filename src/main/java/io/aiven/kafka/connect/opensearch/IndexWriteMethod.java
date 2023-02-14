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

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public enum IndexWriteMethod {

    INSERT,
    UPSERT;

    public static String possibleValues() {
        return Arrays.stream(IndexWriteMethod.values())
                .map(IndexWriteMethod::name)
                .map(s -> String.format("``%s``", s.toLowerCase(Locale.ROOT)))
                .collect(Collectors.joining(", "));
    }

    public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
        @Override
        public void ensureValid(final String name, final Object value) {
            try {
                IndexWriteMethod.valueOf(((String) value).toUpperCase(Locale.ROOT));
            } catch (final IllegalArgumentException e) {
                throw new ConfigException(name, value, "Index write method must be one of: "
                        + IndexWriteMethod.possibleValues());
            }
        }

        @Override
        public String toString() {
            return IndexWriteMethod.possibleValues();
        }
    };

}
