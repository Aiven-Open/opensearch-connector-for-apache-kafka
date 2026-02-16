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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSinkTask.class);

    private OpenSearchSinkConnectorConfig config;

    private OpenSearchTaskHandler openSearchTaskHandler;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(final Map<String, String> props) {
        try {
            LOGGER.info("Starting OpensearchSinkTask.");
            this.config = new OpenSearchSinkConnectorConfig(props);
            this.openSearchTaskHandler = new OpenSearchTaskHandler(this.config, getErrantRecordReporter());
        } catch (final ConfigException e) {
            throw new ConnectException("Couldn't start OpensearchSinkTask due to configuration error:", e);
        }
    }

    private ErrantRecordReporter getErrantRecordReporter() {
        ErrantRecordReporter reporter;
        try {
            reporter = context.errantRecordReporter();
        } catch (final NoSuchMethodError | NoClassDefFoundError e) {
            reporter = null;
        }
        if (config.requiresErrantRecordReporter() && reporter == null) {
            throw new ConfigException(
                    String.format("Errant record reporter must be configured when using 'report' option for %s or %s",
                            BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BEHAVIOR_ON_VERSION_CONFLICT_CONFIG));
        }
        return reporter;
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        openSearchTaskHandler.put(records);
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOGGER.trace("Flushing data to OpenSearch with the following offsets: {}", offsets);
        openSearchTaskHandler.flush();
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        LOGGER.debug("Closing the task for topic partitions: {}", partitions);
    }

    @Override
    public void stop() throws ConnectException {
        LOGGER.info("Stopping OpensearchSinkTask.");
        if (Objects.nonNull(openSearchTaskHandler)) {
            openSearchTaskHandler.close();
        }
    }

}
