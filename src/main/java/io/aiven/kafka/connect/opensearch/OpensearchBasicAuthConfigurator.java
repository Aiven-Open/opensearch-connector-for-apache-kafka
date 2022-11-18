/*
 * Copyright 2019 Aiven Oy
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

import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;
import io.aiven.kafka.connect.opensearch.spi.OpensearchClientConfigurator;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

/**
 * Adds basic authentication to the {@index HttpAsyncClientBuilder} for Opensearch client 
 * if configured.
 */
public class OpensearchBasicAuthConfigurator implements OpensearchClientConfigurator, ConfigDefContributor {
    public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
    private static final String CONNECTION_USERNAME_DOC =
            "The username used to authenticate with OpenSearch. "
                    + "The default is the null, and authentication will only be performed if "
                    + " both the username and password are non-null.";
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC =
            "The password used to authenticate with OpenSearch. "
                    + "The default is the null, and authentication will only be performed if "
                    + " both the username and password are non-null.";

    @Override
    public boolean apply(final OpensearchSinkConnectorConfig config, final HttpAsyncClientBuilder builder) {
        if (!isAuthenticatedConnection(config)) {
            return false;
        }

        final var credentialsProvider = new BasicCredentialsProvider();
        for (final var httpHost : config.httpHosts()) {
            credentialsProvider.setCredentials(
                    new AuthScope(httpHost),
                    new UsernamePasswordCredentials(
                            connectionUsername(config),
                            connectionPassword(config).value()
                    )
            );
        }

        builder.setDefaultCredentialsProvider(credentialsProvider);
        return true;
    }

    @Override
    public void addConfig(final ConfigDef config) {
        config
            .define(
                CONNECTION_USERNAME_CONFIG,
                Type.STRING,
                null,
                Importance.MEDIUM,
                CONNECTION_USERNAME_DOC,
                "Authentication",
                0,
                Width.SHORT,
                "Connection Username"
            ).define(
                CONNECTION_PASSWORD_CONFIG,
                Type.PASSWORD,
                null,
                Importance.MEDIUM,
                CONNECTION_PASSWORD_DOC,
                "Authentication",
                1,
                Width.SHORT,
                "Connection Password");
    }

    private static boolean isAuthenticatedConnection(final OpensearchSinkConnectorConfig config) {
        return Objects.nonNull(connectionUsername(config))
                && Objects.nonNull(connectionPassword(config));
    }
    
    private static String connectionUsername(final OpensearchSinkConnectorConfig config) {
        return config.getString(CONNECTION_USERNAME_CONFIG);
    }

    private static Password connectionPassword(final OpensearchSinkConnectorConfig config) {
        return config.getPassword(CONNECTION_PASSWORD_CONFIG);
    }

}
