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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.common.util.set.Sets;

import io.aiven.kafka.connect.opensearch.bulk.BulkProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchWriter {
    private static final Logger log = LoggerFactory.getLogger(OpensearchWriter.class);

    private final OpensearchClient client;
    private final String type;
    private final boolean ignoreKey;
    private final Set<String> ignoreKeyTopics;
    private final boolean ignoreSchema;
    private final Set<String> ignoreSchemaTopics;
    @Deprecated
    private final Map<String, String> topicToIndexMap;
    private final long flushTimeoutMs;
    private final BulkProcessor<IndexableRecord, ?> bulkProcessor;
    private final boolean dropInvalidMessage;
    private final DataConverter.BehaviorOnNullValues behaviorOnNullValues;
    private final DataConverter converter;

    private final Set<String> existingMappings;
    private final BulkProcessor.BehaviorOnMalformedDoc behaviorOnMalformedDoc;

    OpensearchWriter(
        final OpensearchClient client,
        final String type,
        final boolean useCompactMapEntries,
        final boolean ignoreKey,
        final Set<String> ignoreKeyTopics,
        final boolean ignoreSchema,
        final Set<String> ignoreSchemaTopics,
        final Map<String, String> topicToIndexMap,
        final long flushTimeoutMs,
        final int maxBufferedRecords,
        final int maxInFlightRequests,
        final int batchSize,
        final long lingerMs,
        final int maxRetries,
        final long retryBackoffMs,
        final boolean dropInvalidMessage,
        final DataConverter.BehaviorOnNullValues behaviorOnNullValues,
        final BulkProcessor.BehaviorOnMalformedDoc behaviorOnMalformedDoc
    ) {
        this.client = client;
        this.type = type;
        this.ignoreKey = ignoreKey;
        this.ignoreKeyTopics = ignoreKeyTopics;
        this.ignoreSchema = ignoreSchema;
        this.ignoreSchemaTopics = ignoreSchemaTopics;
        this.topicToIndexMap = topicToIndexMap;
        this.flushTimeoutMs = flushTimeoutMs;
        this.dropInvalidMessage = dropInvalidMessage;
        this.behaviorOnNullValues = behaviorOnNullValues;
        this.converter = new DataConverter(useCompactMapEntries, behaviorOnNullValues);
        this.behaviorOnMalformedDoc = behaviorOnMalformedDoc;

        bulkProcessor = new BulkProcessor<>(
            new SystemTime(),
            new BulkIndexingClient(client),
            maxBufferedRecords,
            maxInFlightRequests,
            batchSize,
            lingerMs,
            maxRetries,
            retryBackoffMs,
            behaviorOnMalformedDoc
        );

        existingMappings = Sets.newHashSet();
    }

    public static class Builder {
        private final OpensearchClient client;
        private String type;
        private boolean useCompactMapEntries = true;
        private boolean ignoreKey = false;
        private Set<String> ignoreKeyTopics = Collections.emptySet();
        private boolean ignoreSchema = false;
        private Set<String> ignoreSchemaTopics = Collections.emptySet();
        private Map<String, String> topicToIndexMap = new HashMap<>();
        private long flushTimeoutMs;
        private int maxBufferedRecords;
        private int maxInFlightRequests;
        private int batchSize;
        private long lingerMs;
        private int maxRetry;
        private long retryBackoffMs;
        private boolean dropInvalidMessage;
        private DataConverter.BehaviorOnNullValues behaviorOnNullValues = DataConverter.BehaviorOnNullValues.DEFAULT;
        private BulkProcessor.BehaviorOnMalformedDoc behaviorOnMalformedDoc;

        public Builder(final OpensearchClient client) {
            this.client = client;
        }

        public Builder setType(final String type) {
            this.type = type;
            return this;
        }

        public Builder setIgnoreKey(final boolean ignoreKey, final Set<String> ignoreKeyTopics) {
            this.ignoreKey = ignoreKey;
            this.ignoreKeyTopics = ignoreKeyTopics;
            return this;
        }

        public Builder setIgnoreSchema(final boolean ignoreSchema, final Set<String> ignoreSchemaTopics) {
            this.ignoreSchema = ignoreSchema;
            this.ignoreSchemaTopics = ignoreSchemaTopics;
            return this;
        }

        public Builder setCompactMapEntries(final boolean useCompactMapEntries) {
            this.useCompactMapEntries = useCompactMapEntries;
            return this;
        }

        public Builder setTopicToIndexMap(final Map<String, String> topicToIndexMap) {
            this.topicToIndexMap = topicToIndexMap;
            return this;
        }

        public Builder setFlushTimoutMs(final long flushTimeoutMs) {
            this.flushTimeoutMs = flushTimeoutMs;
            return this;
        }

        public Builder setMaxBufferedRecords(final int maxBufferedRecords) {
            this.maxBufferedRecords = maxBufferedRecords;
            return this;
        }

        public Builder setMaxInFlightRequests(final int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder setBatchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setLingerMs(final long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder setMaxRetry(final int maxRetry) {
            this.maxRetry = maxRetry;
            return this;
        }

        public Builder setRetryBackoffMs(final long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder setDropInvalidMessage(final boolean dropInvalidMessage) {
            this.dropInvalidMessage = dropInvalidMessage;
            return this;
        }

        /**
         * Change the behavior that the resulting {@link OpensearchWriter} will have when it
         * encounters records with null values.
         *
         * @param behaviorOnNullValues Cannot be null. If in doubt, {@link DataConverter.BehaviorOnNullValues#DEFAULT}
         *                             can be used.
         */
        public Builder setBehaviorOnNullValues(final DataConverter.BehaviorOnNullValues behaviorOnNullValues) {
            this.behaviorOnNullValues =
                Objects.requireNonNull(behaviorOnNullValues, "behaviorOnNullValues cannot be null");
            return this;
        }

        public Builder setBehaviorOnMalformedDoc(final BulkProcessor.BehaviorOnMalformedDoc behaviorOnMalformedDoc) {
            this.behaviorOnMalformedDoc = behaviorOnMalformedDoc;
            return this;
        }

        public OpensearchWriter build() {
            return new OpensearchWriter(
                client,
                type,
                useCompactMapEntries,
                ignoreKey,
                ignoreKeyTopics,
                ignoreSchema,
                ignoreSchemaTopics,
                topicToIndexMap,
                flushTimeoutMs,
                maxBufferedRecords,
                maxInFlightRequests,
                batchSize,
                lingerMs,
                maxRetry,
                retryBackoffMs,
                dropInvalidMessage,
                behaviorOnNullValues,
                behaviorOnMalformedDoc
            );
        }
    }

    public void write(final Collection<SinkRecord> records) {
        for (final SinkRecord sinkRecord : records) {
            // Preemptively skip records with null values if they're going to be ignored anyways
            if (ignoreRecord(sinkRecord)) {
                log.debug(
                    "Ignoring sink record with key {} and null value for topic/partition/offset {}/{}/{}",
                    sinkRecord.key(),
                    sinkRecord.topic(),
                    sinkRecord.kafkaPartition(),
                    sinkRecord.kafkaOffset());
                continue;
            }

            final String index = convertTopicToIndexName(sinkRecord.topic());
            final boolean ignoreKey = ignoreKeyTopics.contains(sinkRecord.topic()) || this.ignoreKey;
            final boolean ignoreSchema =
                ignoreSchemaTopics.contains(sinkRecord.topic()) || this.ignoreSchema;

            if (!ignoreSchema && !existingMappings.contains(index)) {
                //FIXME restore code here
//                try {
//                    if (Mapping.getMapping(client, index, type) == null) {
//                        Mapping.createMapping(client, index, type, sinkRecord.valueSchema());
//                    }
//                } catch (final IOException e) {
//                    // FIXME: concurrent tasks could attempt to create the mapping and one of the requests may
//                    // fail
//                    throw new ConnectException("Failed to initialize mapping for index: " + index, e);
//                }
                existingMappings.add(index);
            }

            tryWriteRecord(sinkRecord, index, ignoreKey, ignoreSchema);
        }
    }

    private boolean ignoreRecord(final SinkRecord record) {
        return record.value() == null && behaviorOnNullValues == DataConverter.BehaviorOnNullValues.IGNORE;
    }

    private void tryWriteRecord(
        final SinkRecord sinkRecord,
        final String index,
        final boolean ignoreKey,
        final boolean ignoreSchema) {

        try {
            final IndexableRecord record = converter.convertRecord(
                sinkRecord,
                index,
                type,
                ignoreKey,
                ignoreSchema);
            if (record != null) {
                bulkProcessor.add(record, flushTimeoutMs);
            }
        } catch (final ConnectException convertException) {
            if (dropInvalidMessage) {
                log.error(
                    "Can't convert record from topic {} with partition {} and offset {}. "
                        + "Error message: {}",
                    sinkRecord.topic(),
                    sinkRecord.kafkaPartition(),
                    sinkRecord.kafkaOffset(),
                    convertException.getMessage()
                );
            } else {
                throw convertException;
            }
        }
    }

    /**
     * Return the expected index name for a given topic, using the configured mapping or the topic
     * name. Elasticsearch accepts only lowercase index names
     * (<a href="https://github.com/elastic/elasticsearch/issues/29420">ref</a>_.
     */
    private String convertTopicToIndexName(final String topic) {
        final String indexOverride = topicToIndexMap.get(topic);
        final String index = indexOverride != null ? indexOverride : topic.toLowerCase();
        log.debug("Topic '{}' was translated as index '{}'", topic, index);
        return index;
    }

    public void flush() {
        bulkProcessor.flush(flushTimeoutMs);
    }

    public void start() {
        bulkProcessor.start();
    }

    public void stop() {
        try {
            bulkProcessor.flush(flushTimeoutMs);
        } catch (final Exception e) {
            log.warn("Failed to flush during stop", e);
        }
        bulkProcessor.stop();
        bulkProcessor.awaitStop(flushTimeoutMs);
    }

    public void createIndicesForTopics(final Set<String> assignedTopics) {
        Objects.requireNonNull(assignedTopics);
        client.createIndices(indicesForTopics(assignedTopics));
    }

    private Set<String> indicesForTopics(final Set<String> assignedTopics) {
        final Set<String> indices = Sets.newHashSet();
        for (final String topic : assignedTopics) {
            indices.add(convertTopicToIndexName(topic));
        }
        return indices;
    }

}
