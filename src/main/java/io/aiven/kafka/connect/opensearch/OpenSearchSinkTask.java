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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSinkTask.class);

    private OpenSearchClient client;

    private OpenSearchSinkConnectorConfig config;

    private final Set<String> indexCache = new HashSet<>();

    private final Set<String> indexMappingsCache = new HashSet<>();

    private RecordConverter recordConverter;

    private Function<String, String> topicToIndexConverter;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        try {
            LOGGER.info("Starting OpensearchSinkTask.");

            this.config = new OpenSearchSinkConnectorConfig(props);

            if (config.requiresErrantRecordReporter() && getErrantRecordReporter() == null) {
                throw new ConfigException(String.format(
                        "Errant record reporter must be configured when using 'report' option for %s or %s",
                        BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BEHAVIOR_ON_VERSION_CONFLICT_CONFIG));
            }

            this.topicToIndexConverter = config.topicToIndexNameConverter();
            // Calculate the maximum possible backoff time ...
            final long maxRetryBackoffMs = RetryUtil.computeRetryWaitTimeInMillis(config.maxRetry(),
                    config.retryBackoffMs());
            if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
                LOGGER.warn(
                        "This connector uses exponential backoff with jitter for retries, "
                                + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                                + "backoff time greater than {} hours.",
                        OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG, config.maxRetry(),
                        OpenSearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs(),
                        TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
            }

            this.client = new OpenSearchClient(config, getErrantRecordReporter());
            this.recordConverter = new RecordConverter(config);
        } catch (final ConfigException e) {
            throw new ConnectException("Couldn't start OpensearchSinkTask due to configuration error:", e);
        }
    }

    private ErrantRecordReporter getErrantRecordReporter() {
        try {
            return context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            return null;
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        LOGGER.trace("Putting {} to Opensearch", records);
        for (final var record : records) {
            if (ignoreRecord(record)) {
                LOGGER.debug("Ignoring sink record with key {} and null value for topic/partition/offset {}/{}/{}",
                        record.key(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
                continue;
            }
            tryWriteRecord(record);
        }
    }

    public boolean ignoreRecord(final SinkRecord record) {
        return record.value() == null && config.behaviorOnNullValues() == BehaviorOnNullValues.IGNORE;
    }

    private void tryWriteRecord(final SinkRecord record) {
        final var indexOrDataStreamName = topicToIndexConverter.apply(record.topic());
        ensureIndexOrDataStreamExists(indexOrDataStreamName);
        checkMappingFor(indexOrDataStreamName, record);
        try {
            final var indexRecord = recordConverter.convert(record, indexOrDataStreamName);
            if (Objects.nonNull(indexRecord)) {
                client.index(indexRecord, record);
            }
        } catch (final DataException e) {
            if (config.dropInvalidMessage()) {
                LOGGER.error("Can't convert record from topic {} with partition {} and offset {}. Reason: ",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
            } else {
                throw e;
            }
        }
    }

    private void ensureIndexOrDataStreamExists(final String index) {
        if (!indexCache.contains(index)) {
            if (!client.indexOrDataStreamExists(index)) {
                if (config.dataStreamEnabled()) {
                    if (config.dataStreamExistingIndexTemplateName().isPresent()) {
                        String userProvidedTemplate = config.dataStreamExistingIndexTemplateName().get();
                        if (!client.dataStreamIndexTemplateExists(userProvidedTemplate)) {
                            LOGGER.info("Creating index template {} for data stream {}", userProvidedTemplate, index);
                            client.createIndexTemplateAndDataStream(userProvidedTemplate,
                                    config.dataStreamTimestampField());
                        } else {
                            LOGGER.info("Using existing index template {} for data stream {}", userProvidedTemplate,
                                    index);
                        }
                    } else {
                        LOGGER.info("Create data stream {}", index);
                        client.createIndexTemplateAndDataStream(index, config.dataStreamTimestampField());
                    }
                } else {
                    LOGGER.info("Create index {}", index);
                    client.createIndex(index);
                }
            }
            indexCache.add(index);
        }
    }

    private void checkMappingFor(final String index, final SinkRecord record) {
        if (!config.ignoreSchemaFor(record.topic()) && !indexMappingsCache.contains(index)) {
            if (!client.hasMapping(index)) {
                LOGGER.info("Create mapping for index {} and schema {}", index, record.valueSchema());
                client.createMapping(index, record.valueSchema());
                indexMappingsCache.add(index);
            }
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.trace("Flushing data to Opensearch with the following offsets: {}", offsets);
        client.flush();
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        LOGGER.debug("Closing the task for topic partitions: {}", partitions);
    }

    @Override
    public void stop() throws ConnectException {
        LOGGER.info("Stopping OpensearchSinkTask.");
        if (Objects.nonNull(client)) {
            try {
                client.close();
            } catch (final IOException e) {
                throw new ConnectException(e);
            }
        }
    }

}
