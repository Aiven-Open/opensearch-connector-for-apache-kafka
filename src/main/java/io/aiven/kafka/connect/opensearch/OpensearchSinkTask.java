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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(OpensearchSinkTask.class);
    private OpensearchWriter writer;
    private OpensearchClient client;

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
            log.info("Starting OpensearchSinkTask.");

            final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);

            // Calculate the maximum possible backoff time ...
            final long maxRetryBackoffMs =
                RetryUtil.computeRetryWaitTimeInMillis(config.maxRetry(), config.retryBackoffMs());
            if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
                log.warn("This connector uses exponential backoff with jitter for retries, "
                        + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                        + "backoff time greater than {} hours.",
                    OpensearchSinkConnectorConfig.MAX_RETRIES_CONFIG, config.maxRetry(),
                    OpensearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs(),
                    TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
            }

            if (client != null) {
                this.client = client;
            }

            final OpensearchWriter.Builder builder = new OpensearchWriter.Builder(this.client)
                .setIgnoreKey(config.ignoreKey(), config.topicIgnoreKey())
                .setIgnoreSchema(config.ignoreSchema(), config.topicIgnoreSchema())
                .setCompactMapEntries(config.useCompactMapEntries())
                .setTopicToIndexMap(config.topicToIndexMap())
                .setFlushTimoutMs(config.flushTimeoutMs())
                .setMaxBufferedRecords(config.maxBufferedRecords())
                .setMaxInFlightRequests(config.maxInFlightRequests())
                .setBatchSize(config.batchSize())
                .setLingerMs(config.lingerMs())
                .setRetryBackoffMs(config.retryBackoffMs())
                .setMaxRetry(config.maxRetry())
                .setDropInvalidMessage(config.dropInvalidMessage())
                .setBehaviorOnNullValues(config.behaviorOnNullValues())
                .setBehaviorOnMalformedDoc(config.behaviorOnMalformedDoc());

            writer = builder.build();
            writer.start();
        } catch (final ConfigException e) {
            throw new ConnectException(
                "Couldn't start OpensearchSinkTask due to configuration error:",
                e
            );
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        log.trace("Putting {} to Opensearch.", records);
        writer.write(records);
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.trace("Flushing data to Opensearch with the following offsets: {}", offsets);
        writer.flush();
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        log.debug("Closing the task for topic partitions: {}", partitions);
    }

    @Override
    public void stop() throws ConnectException {
        log.info("Stopping OpensearchSinkTask.");
        if (writer != null) {
            writer.stop();
        }
        if (client != null) {
            try {
                client.close();
            } catch (final IOException e) {
                throw new ConnectException(e);
            }
        }
    }

}
