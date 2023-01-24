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

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.index.VersionType;

public enum DocumentIDStrategy {

    NONE("none", "No Doc ID is added",
         record -> null,
         (request, record) -> Objects.isNull(record.value()) ? null : request
    ),
    
    RECORD_KEY("record.key", "Generated from the record's key",
        record -> convertKey(record.keySchema(), record.key()),
        (request, record) ->  {
            request.versionType(VersionType.EXTERNAL);
            request.version(record.kafkaOffset());
            return request;
        }
    ),

    TOPIC_PARTITION_OFFSET("topic.partition.offset", "Generated as record's ``topic+partition+offset``",
        record ->  record.topic() + "+" + record.kafkaPartition() + "+" + record.kafkaOffset(),
        (request, record) -> request
    );

            
    private final String name;

    private final String description;

    private final Function<SinkRecord, String> docIdGenerator;

    private final BiFunction<DocWriteRequest<?>, SinkRecord, DocWriteRequest<?>> requestUpdater;

    private DocumentIDStrategy(final String name, final String description, 
                               final Function<SinkRecord, String> docIdGenerator,
                               final BiFunction<DocWriteRequest<?>, SinkRecord, DocWriteRequest<?>> requestUpdater) {
        this.name = name.toLowerCase(Locale.ROOT);
        this.description = description;
        this.docIdGenerator = docIdGenerator;
        this.requestUpdater = requestUpdater;
    }

    @Override
    public String toString() {
        return name;
    }

    public static DocumentIDStrategy fromString(final String name) {
        for (final DocumentIDStrategy strategy : DocumentIDStrategy.values()) {
            if (strategy.name.equalsIgnoreCase(name)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException("Unknown Document ID Strategy " + name);
    }

    public static String describe() {
        return Arrays.stream(values()).map(v -> v.toString() + " : " + v.description)
                .collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Return the DOC ID for a record using this Strategy
     * @param record The record for which to generate the DOC ID
     * @return Doc ID
     */
    public String docId(final SinkRecord record) {
        return docIdGenerator.apply(record);
    }

    /**
     * Update an IndexRequest for a record to include its DOC ID and External
     * Version if applicable based on this Strategy.
     * @param request The IndexRequest to be updated
     * @param record The record for which to update the IndexRequest
     * @return Updated IndexRequest
     */
    public DocWriteRequest<?> updateRequest(final IndexRequest request,
                                            final SinkRecord record) {
        return requestUpdater.apply(request.id(docId(record)), record);
    }

    /**
     * Update a DeleteRequest for a record to include its DOC ID and External
     * Version if applicable based on this Strategy.
     * @param request The DeleteRequest to be updated
     * @param record The record for which to update the DeleteRequest
     * @return Updated DeleteRequest
     */
    public DocWriteRequest<?> updateRequest(final DeleteRequest request,
                                            final SinkRecord record) {
        return requestUpdater.apply(request.id(docId(record)), record);
    }

    public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
        private final String[] names = Arrays.stream(values()).map(v -> v.toString()).toArray(String[]::new);
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

    private static String convertKey(final Schema keySchema, final Object key) {
        if (key == null) {
            throw new DataException("Key is used as document id and can not be null.");
        }

        final Schema.Type schemaType;
        if (keySchema == null) {
            schemaType = ConnectSchema.schemaType(key.getClass());
            if (schemaType == null) {
                throw new DataException(
                        String.format("Java class %s does not have corresponding schema type.", key.getClass())
                );
            }
        } else {
            schemaType = keySchema.type();
        }

        switch (schemaType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case STRING:
                return String.valueOf(key);
            default:
                throw new DataException(schemaType.name() + " is not supported as the document id.");
        }
    }
}
