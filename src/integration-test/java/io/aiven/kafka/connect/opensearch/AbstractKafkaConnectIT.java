/*
 * Copyright 2021 Aiven Oy
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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractKafkaConnectIT extends AbstractIT {

    static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaConnectIT.class);

    static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(60);

    EmbeddedConnectCluster connect;

    final String topicName;

    final String connectorName;

    protected AbstractKafkaConnectIT(final String topicName, final String connectorName) {
        this.topicName = topicName;
        this.connectorName = connectorName;
    }

    @BeforeEach
    void startConnect() {
        connect = new EmbeddedConnectCluster.Builder().name("opensearch-it-connect-cluster").build();
        connect.start();
        connect.kafka().createTopic(topicName);
    }

    @AfterEach
    void stopConnect() {
        try (final var admin = connect.kafka().createAdminClient()) {
            final var result = admin.deleteTopics(List.of(topicName));
            result.all().get();
        } catch (final ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        connect.stop();
    }

    long waitForConnectorToStart(final String name, final int numTasks) throws InterruptedException {
        TestUtils.waitForCondition(() -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
                CONNECTOR_STARTUP_DURATION_MS, "Connector tasks did not start in time.");
        return System.currentTimeMillis();
    }

    Optional<Boolean> assertConnectorAndTasksRunning(final String connectorName, final int numTasks) {
        try {
            final var info = connect.connectorStatus(connectorName);
            final boolean result = info != null && info.tasks().size() >= numTasks
                    && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                    && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
            return Optional.of(result);
        } catch (final Exception e) {
            LOGGER.error("Could not check connector state info.");
            return Optional.empty();
        }
    }

    static Map<String, String> connectorProperties(String topicName) {
        final var props = new HashMap<>(getDefaultProperties());
        props.put(CONNECTOR_CLASS_CONFIG, OpenSearchSinkConnector.class.getName());
        props.put(TOPICS_CONFIG, topicName);
        props.put(TASKS_MAX_CONFIG, Integer.toString(1));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put("value.converter." + SCHEMAS_ENABLE_CONFIG, "false");
        props.put(KEY_IGNORE_CONFIG, "true");
        props.put(SCHEMA_IGNORE_CONFIG, "true");
        props.put(MAX_BUFFERED_RECORDS_CONFIG, "1");
        return props;
    }

    void writeRecords(final int numRecords, String topicNameToWrite) {
        for (int i = 0; i < numRecords; i++) {
            connect.kafka().produce(topicNameToWrite, String.valueOf(i), String.format("{\"doc_num\":%d}", i));
        }
    }

}
