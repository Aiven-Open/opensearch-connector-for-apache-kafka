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
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpensearchSinkTask.class);

    private OpensearchClient client;

    private OpensearchSinkConnectorConfig config;

    private final Set<String> indexCache = new HashSet<>();

    private final Set<String> indexMappingsCache = new HashSet<>();

    private RecordConverter recordConverter;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        start(props, null);
    }

    public void start(final Map<String, String> props, final OpensearchClient client) {
        try {
            LOGGER.info("Starting OpensearchSinkTask.");

            this.config = new OpensearchSinkConnectorConfig(props);

            // Calculate the maximum possible backoff time ...
            final long maxRetryBackoffMs =
                RetryUtil.computeRetryWaitTimeInMillis(config.maxRetry(), config.retryBackoffMs());
            if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
                LOGGER.warn("This connector uses exponential backoff with jitter for retries, "
                        + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                        + "backoff time greater than {} hours.",
                    OpensearchSinkConnectorConfig.MAX_RETRIES_CONFIG, config.maxRetry(),
                    OpensearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs(),
                    TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
            }

            if (client != null) {
                this.client = client;
            }
            this.recordConverter = new RecordConverter(config);
        } catch (final ConfigException e) {
            throw new ConnectException(
                "Couldn't start OpensearchSinkTask due to configuration error:",
                e
            );
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        LOGGER.trace("Putting {} to Opensearch", records);
        for (final var record : records) {
            if (ignoreRecord(record)) {
                LOGGER.debug(
                        "Ignoring sink record with key {} and null value for topic/partition/offset {}/{}/{}",
                        record.key(),
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset());
                continue;
            }
            tryWriteRecord(record);
        }
    }

    public boolean ignoreRecord(final SinkRecord record) {
        return record.value() == null && config.behaviorOnNullValues() == RecordConverter.BehaviorOnNullValues.IGNORE;
    }

    private void tryWriteRecord(final SinkRecord record) {
        final var index = convertTopicToIndexName(record.topic());
        ensureIndexExists(index);
        checkMappingFor(index, record);
        try {
            final var indexRecord = recordConverter.convert(record, index);
            if (Objects.nonNull(indexRecord)) {
                //FIXME add bulk processor here using client
            }
        } catch (final DataException e) {
            if (config.dropInvalidMessage()) {
                LOGGER.error(
                        "Can't convert record from topic {} with partition {} and offset {}. Reason: ",
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset(),
                        e
                );
            } else {
                throw e;
            }
        }
    }

    /**
     * Converts topic name to index name according OS rules:
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
     */
    protected String convertTopicToIndexName(final String topic) {
        var indexName = topic.toLowerCase();
        if (indexName.length() > 255) {
            indexName = indexName.substring(0, 255);
            LOGGER.warn("Topic {} length is more than 255 bytes. The final index name is {}", topic, indexName);
        }
        if (indexName.contains(":")) {
            indexName = indexName.replaceAll(":", "_");
            LOGGER.warn("Topic {} contains :. The final index name is {}", topic, indexName);
        }
        if (indexName.startsWith("-") || indexName.startsWith("_") || indexName.startsWith("+")) {
            indexName = indexName.substring(1);
            LOGGER.warn("Topic {} starts with -, _ or +. The final index name is {}", topic, indexName);
        }
        if (indexName.equals(".") || indexName.equals("..")) {
            indexName = indexName.replace(".", "dot");
            LOGGER.warn("Topic {} name is . or .. . The final index name is {}", topic, indexName);
        }
        return indexName;
    }


    private void ensureIndexExists(final String index) {
        if (!indexCache.contains(index)) {
            LOGGER.info("Create index {}", index);
            client.createIndex(index);
            indexCache.add(index);
        }
    }

    private void checkMappingFor(final String index, final SinkRecord record) {
        if (config.ignoreSchemaFor(record.topic()) && !indexMappingsCache.contains(index)) {
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
        //FIXME bulk process here using client
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
