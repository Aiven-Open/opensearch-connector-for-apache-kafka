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

import java.util.List;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class Utils {

    private Utils() {
    }

    public static String convertKey(final SinkRecord record) {
        return convertKey(record.keySchema(), record.key());
    }

    public static String convertKey(final Schema keySchema, final Object key) {
        if (key == null) {
            throw new DataException("Key is used as document id and can not be null.");
        }

        final Schema.Type schemaType;
        if (keySchema == null) {
            schemaType = ConnectSchema.schemaType(key.getClass());
            if (schemaType == null) {
                throw new DataException(
                        String.format("Java class %s does not have corresponding schema type.", key.getClass()));
            }
        } else {
            schemaType = keySchema.type();
        }

        switch (schemaType) {
            case INT8 :
            case INT16 :
            case INT32 :
            case INT64 :
            case STRING :
                return String.valueOf(key);
            default :
                throw new DataException(String.format("%s type is not supported. Supported are: %s", schemaType.name(),
                        List.of(Schema.INT8_SCHEMA, Schema.INT16_SCHEMA, Schema.INT32_SCHEMA, Schema.INT64_SCHEMA,
                                Schema.STRING_SCHEMA)));
        }
    }

}
