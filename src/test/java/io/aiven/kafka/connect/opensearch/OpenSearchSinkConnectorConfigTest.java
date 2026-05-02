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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.SSL_CONFIG_PREFIX;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.SSL_CONFIG_TRUST_ALL_CERTIFICATES;
import static org.apache.kafka.common.config.SslConfigs.SSL_CIPHER_SUITES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;

import io.aiven.kafka.connect.opensearch.basicauth.OpenSearchBasicAuthConfigDefContributor;
import io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor;

import com.fasterxml.jackson.core.JsonPointer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenSearchSinkConnectorConfigTest {

    private Map<String, String> props;

    @BeforeEach
    public void setup() {
        props = new HashMap<>();
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
    }

    @Test
    public void testDefaultHttpTimeoutsConfig() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        final OpenSearchSinkConnectorConfig config = new OpenSearchSinkConnectorConfig(props);
        assertEquals(config.getInt(OpenSearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG), 3000);
        assertEquals(config.getInt(OpenSearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG), 1000);
    }

    @Test
    public void testSetHttpTimeoutsConfig() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG, "10000");
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG, "15000");
        final OpenSearchSinkConnectorConfig config = new OpenSearchSinkConnectorConfig(props);
        assertEquals(config.getInt(OpenSearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG), 10000);
        assertEquals(config.getInt(OpenSearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG), 15000);
    }

    @Test
    void testWrongIndexWriteMethod() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.INDEX_WRITE_METHOD, "aaa");
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG,
                DocumentIDStrategy.RECORD_KEY.toString());

        assertThrows(ConfigException.class, () -> new OpenSearchSinkConnectorConfig(props));

        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true");
        props.put(OpenSearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT));
        assertThrows(ConfigException.class, () -> new OpenSearchSinkConnectorConfig(props));

        props.remove(OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED);
        props.remove(OpenSearchSinkConnectorConfig.INDEX_WRITE_METHOD);
        final var defaultIndexWriteMethod = new OpenSearchSinkConnectorConfig(props);
        assertEquals(IndexWriteMethod.INSERT, defaultIndexWriteMethod.indexWriteMethod());

        props.put(OpenSearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT));
        final var upsertIndexWriteMethod = new OpenSearchSinkConnectorConfig(props);
        assertEquals(IndexWriteMethod.INSERT, defaultIndexWriteMethod.indexWriteMethod());
    }

    @Test
    public void testThrowsConfigExceptionForWrongUrls() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "ttp://asdsad");
        assertThrows(ConfigException.class, () -> new OpenSearchSinkConnectorConfig(props));
    }

    @Test
    void testWrongKeyIgnoreIdStrategyConfigSettingsForIndexWriteMethodUspert() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT));
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.NONE.name());

        assertThrows(ConfigException.class, () -> new OpenSearchSinkConnectorConfig(props));
    }

    @Test
    public void docIdStrategyValidator() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, "something");
        assertThrows(ConfigException.class, () -> new OpenSearchSinkConnectorConfig(props));
    }

    @Test
    public void docIdStrategies() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        for (final var strategy : DocumentIDStrategy.values()) {
            props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, strategy.toString());
            final OpenSearchSinkConnectorConfig config = new OpenSearchSinkConnectorConfig(props);
            assertEquals(strategy, config.documentIdStrategy("anyTopic"));
        }
    }

    @Test
    public void docIdStrategyWithoutKeyIgnoreIdStrategy() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false");
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, DocumentIDStrategy.NONE.toString());
        final OpenSearchSinkConnectorConfig config = new OpenSearchSinkConnectorConfig(props);
        assertEquals(DocumentIDStrategy.RECORD_KEY, config.documentIdStrategy("anyTopic"));
    }

    @Test
    public void docIdStrategyWithoutKeyIgnoreWithTopicKeyIgnore() {
        final DocumentIDStrategy keyIgnoreStrategy = DocumentIDStrategy.NONE;
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false");
        props.put(OpenSearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG, "topic1,topic2");
        props.put(OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG, keyIgnoreStrategy.toString());
        final OpenSearchSinkConnectorConfig config = new OpenSearchSinkConnectorConfig(props);

        assertEquals(keyIgnoreStrategy, config.documentIdStrategy("topic1"));
        assertEquals(keyIgnoreStrategy, config.documentIdStrategy("topic2"));
        assertEquals(DocumentIDStrategy.RECORD_KEY, config.documentIdStrategy("otherTopic"));
    }

    @Test
    public void dataStreamConfig() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX, "aaaa");

        final var defaultConfig = new OpenSearchSinkConnectorConfig(props);

        assertEquals("aaaa", defaultConfig.dataStreamPrefix().get());
        assertEquals("@timestamp", defaultConfig.dataStreamTimestampField());

        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX, "bbbb");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD, "custom_timestamp");
        final var customConfig = new OpenSearchSinkConnectorConfig(props);
        assertEquals("bbbb", customConfig.dataStreamPrefix().get());
        assertEquals("custom_timestamp", customConfig.dataStreamTimestampField());
    }

    @Test
    public void testDefaultSslConfig() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX, "aaaa");

        final var defaultConfig = new OpenSearchSinkConnectorConfig(props);
        assertEquals(SslConfigs.DEFAULT_SSL_PROTOCOL, defaultConfig.sslProtocol());
        assertNull(defaultConfig.cipherSuitesConfig());
        assertEquals(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS,
                String.join(",", List.of(defaultConfig.sslEnableProtocols())));
        assertEquals(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, defaultConfig.trustStoreType());
        assertTrue(defaultConfig.trustStorePath().isEmpty());
        assertNull(defaultConfig.trustStorePassword());

        assertEquals(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, defaultConfig.trustStoreType());
        assertTrue(defaultConfig.keyStorePath().isEmpty());
        assertNull(defaultConfig.keyStorePassword());
        assertNull(defaultConfig.keyPassword());

        assertFalse(defaultConfig.disableHostnameVerification());
        assertFalse(defaultConfig.trustAllCertificates());
    }

    @Test
    public void testSetSslConfig() {
        props.put(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost");
        props.put(OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX, "aaaa");

        props.put(SSL_CONFIG_PREFIX + SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put(SSL_CONFIG_PREFIX + SSL_CIPHER_SUITES_CONFIG, "A,B,C");
        props.put(SSL_CONFIG_PREFIX + SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.1");
        props.put(SSL_CONFIG_PREFIX + SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        props.put(SSL_CONFIG_PREFIX + SSL_TRUSTSTORE_LOCATION_CONFIG, "/d/e");
        props.put(SSL_CONFIG_PREFIX + SSL_TRUSTSTORE_PASSWORD_CONFIG, "trust_store_password");

        props.put(SSL_CONFIG_PREFIX + SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        props.put(SSL_CONFIG_PREFIX + SSL_KEYSTORE_LOCATION_CONFIG, "/f/g");
        props.put(SSL_CONFIG_PREFIX + SSL_KEYSTORE_PASSWORD_CONFIG, "key_store_password");
        props.put(SSL_CONFIG_PREFIX + SSL_KEY_PASSWORD_CONFIG, "key_password");
        props.put(SSL_CONFIG_PREFIX + SSL_CONFIG_TRUST_ALL_CERTIFICATES, "true");
        props.put(SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        final var defaultConfig = new OpenSearchSinkConnectorConfig(props);
        assertEquals("TLSv1.2", defaultConfig.sslProtocol());
        assertArrayEquals(new String[] { "A", "B", "C" }, defaultConfig.cipherSuitesConfig());
        assertEquals("TLSv1.1", String.join(",", List.of(defaultConfig.sslEnableProtocols())));

        assertEquals("PKCS12", defaultConfig.trustStoreType());
        assertTrue(defaultConfig.trustStorePath().isPresent());
        assertEquals(Path.of("/d/e"), defaultConfig.trustStorePath().get());
        assertEquals("trust_store_password", String.valueOf(defaultConfig.trustStorePassword()));

        assertEquals("PKCS12", defaultConfig.keyStoreType());
        assertTrue(defaultConfig.keyStorePath().isPresent());
        assertEquals(Path.of("/f/g"), defaultConfig.keyStorePath().get());
        assertEquals("key_store_password", String.valueOf(defaultConfig.keyStorePassword()));
        assertEquals("key_password", String.valueOf(defaultConfig.keyPassword()));

        assertTrue(defaultConfig.disableHostnameVerification());
        assertTrue(defaultConfig.trustAllCertificates());
    }

    @Test
    void convertTopicToIndexName() {
        final var config = new OpenSearchSinkConnectorConfig(
                Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://l:9200"));
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
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://l:9200", OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true",
                OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX, "aaaaa"));
        assertEquals("aaaaa-bbbbb", config.topicToIndexNameConverter().apply("bbbbb"));

        final var noDsPrefixConfig = new OpenSearchSinkConnectorConfig(
                Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://l:9200",
                        OpenSearchSinkConnectorConfig.DATA_STREAM_ENABLED, "true"));
        assertEquals("bbbbb", noDsPrefixConfig.topicToIndexNameConverter().apply("bbbbb"));
    }

    @Test
    void testDefaultRoutingSettings() {
        final var config = new OpenSearchSinkConnectorConfig(
                Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://l:9200"));

        assertEquals(RoutingType.NONE, config.routingType());
        assertNull(config.routingRecordValuePath());
    }

    @Test
    void testSetKeyRoutingSettings() {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://l:9200", OpenSearchSinkConnectorConfig.ROUTING_TYPE, RoutingType.KEY.toString()));

        assertEquals(RoutingType.KEY, config.routingType());
        assertNull(config.routingRecordValuePath());
    }

    @Test
    void testSetValueRoutingSettings() {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://l:9200", OpenSearchSinkConnectorConfig.ROUTING_TYPE, RoutingType.VALUE.toString(),
                OpenSearchSinkConnectorConfig.ROUTING_RECORD_VALUE_PATH, "/a/b/c"));

        assertEquals(RoutingType.VALUE, config.routingType());
        assertNotNull(config.routingRecordValuePath());
        assertEquals(JsonPointer.compile("/a/b/c"), config.routingRecordValuePath());
    }

    @Test
    void testThrowExceptionForWrongValueRoutingSettings() {
        final var e = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchSinkConnectorConfig.ROUTING_TYPE, RoutingType.VALUE.toString())));
        assertEquals(
                "The 'routing.type.record.value.path' setting must be configured when using the 'value' routing type",
                e.getMessage());
    }

    @Test
    void testThrowExceptionForWrongRoutingJSONPointerValueSettings() {
        final var e = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchSinkConnectorConfig.ROUTING_TYPE, RoutingType.VALUE.toString(),
                        OpenSearchSinkConnectorConfig.ROUTING_RECORD_VALUE_PATH, "f\\g")));
        assertEquals("Invalid input: JSON Pointer expression must start with '/': \"f\\g\"", e.getMessage());
    }

    @Test
    void testThrowExceptionForWrongRoutingJSONPointerValueSettingsAndBehaviorOnNullValuesDelete() {
        final var e = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchSinkConnectorConfig.ROUTING_TYPE, RoutingType.VALUE.toString(),
                        OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
                        BehaviorOnNullValues.DELETE.toString(), OpenSearchSinkConnectorConfig.ROUTING_RECORD_VALUE_PATH,
                        "/f/g")));
        assertEquals(
                "The 'routing.type.record.value.path' can't be used together with 'behavior.on.null.values' set to 'delete'",
                e.getMessage());
    }

    @Test
    void testBothBasicAuthSettingsAndAwsCredsSet() {
        final var withBasicAuthAndAwsBasicAuth = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchBasicAuthConfigDefContributor.CONNECTION_USERNAME_CONFIG, "a",
                        OpenSearchBasicAuthConfigDefContributor.CONNECTION_PASSWORD_CONFIG, "b",
                        OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG, "c",
                        OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG, "d")));
        assertEquals("More than one authenticated configurator is applied for the client. Only one is allowed",
                withBasicAuthAndAwsBasicAuth.getMessage());

        final var withBasicAuthAndAwsStsAuth = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchBasicAuthConfigDefContributor.CONNECTION_USERNAME_CONFIG, "a",
                        OpenSearchBasicAuthConfigDefContributor.CONNECTION_PASSWORD_CONFIG, "b",
                        OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_ARN_CONFIG, "e",
                        OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_SESSION_NAME_CONFIG, "f")));
        assertEquals("More than one authenticated configurator is applied for the client. Only one is allowed",
                withBasicAuthAndAwsStsAuth.getMessage());
    }

    @Test
    void testWrongSig4AuthSettings() {
        final var allCredsException = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG, "aaaa",
                        OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG, "bbbb",
                        OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_ARN_CONFIG, "cccc",
                        OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_SESSION_NAME_CONFIG, "dddd")));
        assertEquals("Found both AWS access and STS role credentials. Only one can be selected",
                allCredsException.getMessage());

        final var noRegionException = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG, "aaaa",
                        OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG, "bbbb",
                        OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG, "dddd")));
        assertEquals("Invalid value null for configuration aws.region", noRegionException.getMessage());

        final var noServiceException = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG, "aaaa",
                        OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG, "bbbb",
                        OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG, "dddd")));
        assertEquals("Invalid value null for configuration aws.service.signing.name", noServiceException.getMessage());

    }

    @Test
    void testWrongSig4AuthSettingsAndToManyHost() {
        final var hostsCredsException = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200,http://l:9200,http://l:9200",
                        OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG, "a",
                        OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG, "b",
                        OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG, "c",
                        OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG, "d")));
        assertEquals(
                "Invalid value http://l:9200,http://l:9200,http://l:9200 "
                        + "for configuration aws.service.signing.name: "
                        + "Only one OpenSearch endpoint, without scheme should be provided",
                hostsCredsException.getMessage());
    }

    @Test
    void testWrongSig4AuthSettingsAndHost() {
        final var hostsCredsException = assertThrows(ConfigException.class,
                () -> new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://l:9200", OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG, "a",
                        OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG, "b",
                        OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG, "c",
                        OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG, "d")));
        assertEquals(
                "Invalid value http://l:9200 " + "for configuration aws.service.signing.name: "
                        + "Only one OpenSearch endpoint, without scheme should be provided",
                hostsCredsException.getMessage());
    }

    @Test
    void testValidSig4AuthSettings() {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "a.b.region.com", OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG, "a",
                OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG, "b",
                OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG, "some_service",
                OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG, "region"));

        assertEquals("a.b.region.com", config.connectionUrls().getFirst());
        assertEquals("a", config.getString(OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG));
        assertEquals("b", config.getPassword(OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG).value());
        assertEquals("region", config.getString(OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG));
        assertEquals("some_service",
                config.getString(OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG));
    }
}
