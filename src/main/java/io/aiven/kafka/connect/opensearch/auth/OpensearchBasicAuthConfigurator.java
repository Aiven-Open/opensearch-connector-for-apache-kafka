/*
 * Copyright 2019 Aiven Oy
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
package io.aiven.kafka.connect.opensearch.auth;

import static io.aiven.kafka.connect.opensearch.auth.SSLContextBuilder.SUPPORTED_PROTOCOLS;

import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig;
import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;
import io.aiven.kafka.connect.opensearch.spi.OpensearchClientConfigurator;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

/**
 * Adds basic authentication to the {@index HttpAsyncClientBuilder} for Opensearch client if configured.
 */
public class OpensearchBasicAuthConfigurator implements OpensearchClientConfigurator, ConfigDefContributor {

    private final static String SSL_SETTINGS_GROUP_NAME = "SSL Client Settings";

    public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
    private static final String CONNECTION_USERNAME_DOC = "The username used to authenticate with OpenSearch. "
            + "The default is the null, and authentication will only be performed if "
            + " both the username and password are non-null.";
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "The password used to authenticate with OpenSearch. "
            + "The default is the null, and authentication will only be performed if "
            + " both the username and password are non-null.";

    public static final String CLIENT_SSL_PROTOCOL_TYPE = "connection.ssl.protocol.type";
    public static final String CLIENT_SSL_PROTOCOL_TYPE_DOC = "SSL protocol type. Default value is "
            + SSLContextBuilder.TLS_1_3 + ", supported are: " + SSLContextBuilder.TLS_1_2 + ", "
            + SSLContextBuilder.TLS_1_3;

    public static final String CLIENT_SSL_CA_CERTIFICATE_LOCATION = "connection.ssl.ca.certificate.location";
    private static final String CLIENT_SSL_CA_CERTIFICATE_LOCATION_DOC = "Path to X.509 root CAs file (PEM format)";

    public static final String CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION = "connection.ssl.access.certificate.location";
    private static final String CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION_DOC = "Path to X.509 user access certificate file (PEM format)";

    public static final String CLIENT_SSL_ACCESS_KEY_LOCATION = "connection.ssl.access.key.location";
    private static final String CLIENT_SSL_ACCESS_KEY_LOCATION_DOC = "Path to the user certificate’s keys (PKCS #8) file (PEM format)";

    public static final String CLIENT_SSL_ACCESS_KEY_PASSWORD = "connection.ssl.access.key.password";
    private static final String CLIENT_SSL_ACCESS_KEY_PASSWORD_DOC = "User access key password";

    public static final String CLIENT_SSL_TRUSTSTORE_LOCATION = "connection.ssl.truststore.location";
    private static final String CLIENT_SSL_TRUSTSTORE_LOCATION_DOC = "Path to the Truststore file (JKS format)";

    public static final String CLIENT_SSL_TRUSTSTORE_PASSWORD = "connection.ssl.truststore.password";
    private static final String CLIENT_SSL_TRUSTSTORE_PASSWORD_DOC = "Truststore password";

    public static final String CLIENT_SSL_KEYSTORE_LOCATION = "connection.ssl.keystore.location";
    private static final String CLIENT_SSL_KEYSTORE_LOCATION_DOC = "Path to the Keystore file (PKCS12/PFX format)";

    public static final String CLIENT_SSL_KEYSTORE_TYPE = "connection.ssl.keystore.type";
    private static final String CLIENT_SSL_KEYSTORE_TYPE_DOC = "Keystore type. The default is JKS. Supported values are: JKS, PKCS12 or PFX";

    public static final String CLIENT_SSL_KEYSTORE_PASSWORD = "connection.ssl.keystore.password";
    private static final String CLIENT_SSL_KEYSTORE_PASSWORD_DOC = "Keystore password";

    private static final String SUPPORTED_SSL_PROTOCOLS_MESSAGE = String.join(", ", SUPPORTED_PROTOCOLS);

    @Override
    public boolean apply(final OpensearchSinkConnectorConfig config, final HttpAsyncClientBuilder builder) {
        if (!isAuthenticatedConnection(config)) {
            return false;
        }

        final var credentialsProvider = new BasicCredentialsProvider();
        for (final var httpHost : config.httpHosts()) {
            credentialsProvider.setCredentials(new AuthScope(httpHost),
                    new UsernamePasswordCredentials(connectionUsername(config), connectionPassword(config).value()));
        }
        SSLContextBuilder.buildSSLContext(config).map(builder::setSSLContext);
        return true;
    }

    @Override
    public void addConfig(final ConfigDef config) {
        int order = -1;
        config.define(CONNECTION_USERNAME_CONFIG, Type.STRING, null, Importance.MEDIUM, CONNECTION_USERNAME_DOC,
                "Authentication", order++, Width.SHORT, "Connection Username")
                .define(CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, null, Importance.MEDIUM, CONNECTION_PASSWORD_DOC,
                        "Authentication", order++, Width.SHORT, "Connection Password");

        // Common SSL settings
        config.define(CLIENT_SSL_PROTOCOL_TYPE, Type.STRING, SSLContextBuilder.TLS_1_3, new ConfigDef.Validator() {
            @Override
            public void ensureValid(String name, Object value) {
                assert value instanceof String;
                final var s = (String) value;
                if (!SSLContextBuilder.TLS_1_3.equalsIgnoreCase(s) && !SSLContextBuilder.TLS_1_2.equalsIgnoreCase(s)) {
                    throw new ConfigException("Unsupported SSL protocol type " + s + ". Supported are: "
                            + SUPPORTED_SSL_PROTOCOLS_MESSAGE);
                }
            }

            @Override
            public String toString() {
                return SUPPORTED_SSL_PROTOCOLS_MESSAGE;
            }
        }, Importance.MEDIUM, CLIENT_SSL_PROTOCOL_TYPE_DOC, SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT,
                "SSL protocol type")
                .define(CLIENT_SSL_ACCESS_KEY_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
                        CLIENT_SSL_ACCESS_KEY_PASSWORD_DOC, SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT,
                        "User access key password");
        // PEM Certificates settings
        config.define(CLIENT_SSL_CA_CERTIFICATE_LOCATION, Type.STRING, null, Importance.MEDIUM,
                CLIENT_SSL_CA_CERTIFICATE_LOCATION_DOC, SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT, "Root CAs")
                .define(CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION, Type.STRING, null, Importance.MEDIUM,
                        CLIENT_SSL_ACCESS_CERTIFICATE_LOCATION_DOC, SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT,
                        "User access certificate")
                .define(CLIENT_SSL_ACCESS_KEY_LOCATION, Type.STRING, null, Importance.MEDIUM,
                        CLIENT_SSL_ACCESS_KEY_LOCATION_DOC, SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT,
                        "User certificate’s key");
        // KeyStore and TrustStore files settings
        config.define(CLIENT_SSL_TRUSTSTORE_LOCATION, Type.STRING, null, Importance.MEDIUM,
                CLIENT_SSL_TRUSTSTORE_LOCATION_DOC, SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT,
                "Trust store location")
                .define(CLIENT_SSL_TRUSTSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
                        CLIENT_SSL_TRUSTSTORE_PASSWORD_DOC, SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT,
                        "Trust store password")
                .define(CLIENT_SSL_KEYSTORE_LOCATION, Type.STRING, null, Importance.MEDIUM,
                        CLIENT_SSL_KEYSTORE_LOCATION_DOC, SSL_SETTINGS_GROUP_NAME, order, Width.SHORT,
                        "Key store location")
                .define(CLIENT_SSL_KEYSTORE_TYPE, Type.STRING, "JKS", Importance.MEDIUM, CLIENT_SSL_KEYSTORE_TYPE_DOC,
                        SSL_SETTINGS_GROUP_NAME, order++, Width.SHORT, "Key store type")
                .define(CLIENT_SSL_KEYSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
                        CLIENT_SSL_KEYSTORE_PASSWORD_DOC, SSL_SETTINGS_GROUP_NAME, order + 1, Width.SHORT,
                        "Key store password");

    }

    private static boolean isAuthenticatedConnection(final OpensearchSinkConnectorConfig config) {
        return Objects.nonNull(connectionUsername(config)) && Objects.nonNull(connectionPassword(config));
    }

    private static String connectionUsername(final OpensearchSinkConnectorConfig config) {
        return config.getString(CONNECTION_USERNAME_CONFIG);
    }

    private static Password connectionPassword(final OpensearchSinkConnectorConfig config) {
        return config.getPassword(CONNECTION_PASSWORD_CONFIG);
    }

}
