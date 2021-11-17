/*
 * Copyright 2021 Aiven Oy
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpensearchSinkConnectorIT extends AbstractIT {

    static final Logger LOGGER = LoggerFactory.getLogger(OpensearchSinkConnectorIT.class);

    static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(60);

    static final String TOPIC_NAME = "super_topic";

    static final String CONNECTOR_NAME = "os-sink-connector";

    EmbeddedConnectCluster connect;

    @BeforeEach
    void startConnect() {
        connect = new EmbeddedConnectCluster.Builder()
                .name("elasticsearch-it-connect-cluster")
                .build();
        connect.start();
        connect.kafka().createTopic(TOPIC_NAME);
    }

    @AfterEach
    void stopConnect() {
        connect.stop();
    }

    long waitForConnectorToStart(final String name, final int numTasks) throws InterruptedException {
        TestUtils.waitForCondition(
                () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
                CONNECTOR_STARTUP_DURATION_MS,
                "Connector tasks did not start in time."
        );
        return System.currentTimeMillis();
    }

    Optional<Boolean> assertConnectorAndTasksRunning(final String connectorName, final int numTasks) {
        try {
            final var info = connect.connectorStatus(connectorName);
            final boolean result = info != null
                    && info.tasks().size() >= numTasks
                    && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                    && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
            return Optional.of(result);
        } catch (final Exception e) {
            LOGGER.error("Could not check connector state info.");
            return Optional.empty();
        }
    }

    Map<String, String> connectorProperties()  {
        final var props = new HashMap<>(getDefaultProperties());
        props.put(CONNECTOR_CLASS_CONFIG, OpensearchSinkConnector.class.getName());
        props.put(TOPICS_CONFIG, TOPIC_NAME);
        props.put(TASKS_MAX_CONFIG, Integer.toString(1));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put("value.converter." + SCHEMAS_ENABLE_CONFIG, "false");
        props.put(KEY_IGNORE_CONFIG, "true");
        props.put(SCHEMA_IGNORE_CONFIG, "true");
        return props;
    }

    @Test
    public void testConnector() throws Exception {
        connect.configureConnector(CONNECTOR_NAME, connectorProperties());
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(10);

        waitForRecords(10);

        for (final var hit : search()) {
            final var id = (Integer) hit.getSourceAsMap().get("doc_num");
            assertNotNull(id);
            assertTrue(id < 10);
            assertEquals(TOPIC_NAME, hit.getIndex());
        }
    }

    void writeRecords(final int numRecords) {
        for (int i  = 0; i < numRecords; i++) {
            connect.kafka().produce(
                    TOPIC_NAME,
                    String.valueOf(i),
                    String.format("{\"doc_num\":%d}", i)
            );
        }
    }


}
