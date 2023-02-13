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
import java.util.Locale;
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
    void testWrongIndexWriteMethod() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD, "aaa");
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
        props.put(
                OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG,
                DocumentIDStrategy.RECORD_KEY.toString()
        );

        assertThrows(ConfigException.class, () -> new OpensearchSinkConnectorConfig(props));

        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(
                OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT)
        );
        assertThrows(ConfigException.class, () -> new OpensearchSinkConnectorConfig(props));

        props.remove(OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED);
        props.remove(OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD);
        final var defaultIndexWriteMethod = new OpensearchSinkConnectorConfig(props);
        assertEquals(IndexWriteMethod.INSERT, defaultIndexWriteMethod.indexWriteMethod());

        props.put(
                OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT)
        );
        final var upsertIndexWriteMethod = new OpensearchSinkConnectorConfig(props);
        assertEquals(IndexWriteMethod.INSERT, defaultIndexWriteMethod.indexWriteMethod());
    }

    @Test
    public void testThrowsConfigExceptionForWrongUrls() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "ttp://asdsad");
        assertThrows(ConfigException.class, () -> new OpensearchSinkConnectorConfig(props));
    }

    @Test
    public void testAddConfigDefs() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put("custom.property.1", "10");
        props.put("custom.property.2", "http://localhost:9000");
        final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);
        assertEquals(config.getInt("custom.property.1"), 10);
        assertEquals(config.getString("custom.property.2"), "http://localhost:9000");
    }

    @Test
    void testWrongKeyIgnoreIdStrategyConfigSettingsForIndexWriteMethodUspert() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(
                OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT)
        );
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.NONE.name());

        assertThrows(ConfigException.class, () -> new OpensearchSinkConnectorConfig(props));
    }

    public static class CustomConfigDefContributor implements ConfigDefContributor {
        @Override
        public void addConfig(final ConfigDef config) {
            config.define(
                    "custom.property.1",
                    Type.INT,
                    null,
                    Importance.MEDIUM,
                    "Custom string property 1 description",
                    "custom",
                    0,
                    Width.SHORT,
                    "Custom string property 1"
            ).define(
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

    @Test
    public void docIdStrategyValidator() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, "something");
        assertThrows(ConfigException.class, () -> new OpensearchSinkConnectorConfig(props));
    }

    @Test
    public void docIdStrategies() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        for (final var strategy : DocumentIDStrategy.values()) {
            props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, strategy.toString());
            final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);
            assertEquals(strategy, config.documentIdStrategy("anyTopic"));
        }
    }

    @Test
    public void docIdStrategyWithoutKeyIgnoreIdStrategy() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false");
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.NONE.toString());
        final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);
        assertEquals(DocumentIDStrategy.RECORD_KEY, config.documentIdStrategy("anyTopic"));
    }

    @Test
    public void docIdStrategyWithoutKeyIgnoreWithTopicKeyIgnore() {
        final DocumentIDStrategy keyIgnoreStrategy = DocumentIDStrategy.NONE;
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false");
        props.put(OpensearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG, "topic1,topic2");
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, keyIgnoreStrategy.toString());
        final OpensearchSinkConnectorConfig config = new OpensearchSinkConnectorConfig(props);

        assertEquals(keyIgnoreStrategy, config.documentIdStrategy("topic1"));
        assertEquals(keyIgnoreStrategy, config.documentIdStrategy("topic2"));
        assertEquals(DocumentIDStrategy.RECORD_KEY, config.documentIdStrategy("otherTopic"));
    }

    @Test
    public void dataStreamConfig() {
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_PREFIX, "aaaa");

        final var defaultConfig = new OpensearchSinkConnectorConfig(props);

        assertEquals("aaaa", defaultConfig.dataStreamPrefix().get());
        assertEquals("@timestamp", defaultConfig.dataStreamTimestampField());

        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_PREFIX, "bbbb");
        props.put(OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD, "custom_timestamp");
        final var customConfig = new OpensearchSinkConnectorConfig(props);
        assertEquals("bbbb", customConfig.dataStreamPrefix().get());
        assertEquals("custom_timestamp", customConfig.dataStreamTimestampField());
    }

    @Test
    void convertTopicToIndexName() {
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://l:9200")
        );
        final var longTopicName = "a".repeat(260);
        assertEquals("a".repeat(255), config.topicToIndexNameConverter().apply(longTopicName));

        final var colonTopicName = "a:b:c";
        assertEquals("a_b_c", config.topicToIndexNameConverter().apply(colonTopicName));

        final var minusTopicName = "-minusTopicName";
        assertEquals("minustopicname", config.topicToIndexNameConverter().apply(minusTopicName));

        final var plusTopicName = "+plusTopicName";
        assertEquals("plustopicname", config.topicToIndexNameConverter().apply(plusTopicName));

        final var underscoreTopicName = "_underscoreTopicName";
        assertEquals("underscoretopicname", config.topicToIndexNameConverter().apply(underscoreTopicName));

        final var dotTopicName = ".";
        assertEquals("dot", config.topicToIndexNameConverter().apply(dotTopicName));

        final var dotDotTopicName = "..";
        assertEquals("dotdot", config.topicToIndexNameConverter().apply(dotDotTopicName));

    }

    @Test
    void convertTopicToDataStreamName() {
        final var config = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://l:9200",
                        OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                        OpensearchSinkConnectorConfig.DATA_STREAM_PREFIX, "aaaaa"
                )
        );
        assertEquals("aaaaa-bbbbb", config.topicToIndexNameConverter().apply("bbbbb"));

        final var noDsPrefixConfig = new OpensearchSinkConnectorConfig(
                Map.of(
                        OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://l:9200",
                        OpensearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true"
                )
        );
        assertEquals("bbbbb", noDsPrefixConfig.topicToIndexNameConverter().apply("bbbbb"));
    }

}
