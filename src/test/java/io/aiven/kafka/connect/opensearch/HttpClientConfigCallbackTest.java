/*
 * Copyright 2026 Aiven Oy
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

import static io.aiven.kafka.connect.opensearch.basicauth.OpenSearchBasicAuthConfigDefContributor.CONNECTION_PASSWORD_CONFIG;
import static io.aiven.kafka.connect.opensearch.basicauth.OpenSearchBasicAuthConfigDefContributor.CONNECTION_USERNAME_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_ARN_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_SESSION_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.sig4.Sig4HttpRequestInterceptor;

import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HttpClientConfigCallbackTest {

    @Mock
    HttpAsyncClientBuilder asyncClientBuilder;

    @BeforeEach
    public void setup() {
        when(asyncClientBuilder.setConnectionManager(any(PoolingAsyncClientConnectionManager.class)))
                .thenReturn(asyncClientBuilder);
    }

    @AfterEach
    public void tearDown() {
        reset(asyncClientBuilder);
    }

    @Test
    public void failsIfMoreThanOneAuthConfigApplied() {
        assertThrows(ConfigException.class, () -> {
            final var c = new OpenSearchClient.HttpClientConfigCallback(new OpenSearchSinkConnectorConfig(Map.of(
                    OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost", CONNECTION_USERNAME_CONFIG,
                    "a", CONNECTION_PASSWORD_CONFIG, "b", AWS_ACCESS_KEY_ID_CONFIG, "id", AWS_SECRET_ACCESS_KEY_CONFIG,
                    "secret", AWS_REGION_CONFIG, "us-east-1", AWS_SERVICE_SIGNING_NAME_CONFIG, "some_service_name")));
            c.customizeHttpClient(asyncClientBuilder);
        });
    }

    @Test
    public void canReadBasicAuthOnlyConfig() {
        final var c = new OpenSearchClient.HttpClientConfigCallback(
                new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://localhost", CONNECTION_USERNAME_CONFIG, "a", CONNECTION_PASSWORD_CONFIG, "b")));
        c.customizeHttpClient(asyncClientBuilder);

        verify(asyncClientBuilder, times(1)).setDefaultCredentialsProvider(any(BasicCredentialsProvider.class));
    }

    @Test
    public void canReadSig4OnlyConfigForBasicAwsCreds() {
        final var c = new OpenSearchClient.HttpClientConfigCallback(
                new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                        "http://localhost", AWS_ACCESS_KEY_ID_CONFIG, "id", AWS_SECRET_ACCESS_KEY_CONFIG, "secret",
                        AWS_REGION_CONFIG, "us-east-1", AWS_SERVICE_SIGNING_NAME_CONFIG, "some_service_name")));
        c.customizeHttpClient(asyncClientBuilder);

        verify(asyncClientBuilder, times(1)).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }

    @Test
    public void canReadSig4OnlyConfigForStsAwsCreds() {
        final var c = new OpenSearchClient.HttpClientConfigCallback(new OpenSearchSinkConnectorConfig(
                Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost", AWS_STS_ROLE_ARN_CONFIG,
                        "a::b:c", AWS_STS_ROLE_SESSION_NAME_CONFIG, "sadsada", AWS_REGION_CONFIG, "us-east-1",
                        AWS_SERVICE_SIGNING_NAME_CONFIG, "some_service_name")));
        c.customizeHttpClient(asyncClientBuilder);

        verify(asyncClientBuilder, times(1)).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }
}
