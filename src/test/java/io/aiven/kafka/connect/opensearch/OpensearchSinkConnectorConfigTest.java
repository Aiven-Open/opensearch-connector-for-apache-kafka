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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class OpensearchSinkConnectorConfigTest {

    private Map<String, String> props;

    @BeforeEach
    public void setup() {
        props = new HashMap<>();
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
    }

    @Test
    public void testDefaultHttpTimeoutsConfig() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);
        assertEquals(config.getInt(OpensearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG), 3000);
        assertEquals(config.getInt(OpensearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG), 1000);
    }

    @Test
    public void testSetHttpTimeoutsConfig() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpensearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG, "10000");
        props.put(OpensearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG, "15000");
        final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);
        assertEquals(config.getInt(OpensearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG), 10000);
        assertEquals(config.getInt(OpensearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG), 15000);
    }

    @Test
    public void testThrowsConfigExceptionForWrongUrls() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "ttp://asdsad");
        assertThrows(ConfigException.class, () -> new OpensearchSinkConnectorConfig(props));
    }

    @Test
    public void testAddConfigDefs() {
        props.put("custom.property.1", "10");
        props.put("custom.property.2", "http://localhost:9000");
        final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);
        assertEquals(config.getInt("custom.property.1"), 10);
        assertEquals(config.getString("custom.property.2"), "http://localhost:9000");
    }
    
    public static class CustomConfigDefContributor implements ConfigDefContributor {
        @Override
        public void addConfig(final ConfigDef config) {
            config
                .define(
                    "custom.property.1",
                    Type.INT,
                    null,
                    Importance.MEDIUM,
                    "Custom string property 1 description",
                    "custom",
                    0,
                    Width.SHORT,
                    "Custom string property 1"
                )
                .define(
                    "custom.property.2",
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "Custom string property 2 description",
                    "custom",
                    1,
                    Width.SHORT,
                    "Custom string property 2"
                );
        }
    }
}
