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
package io.aiven.kafka.connect.opensearch.sig4;

import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_REGION_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_SERVICE_SIGNING_NAME_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_ARN_CONFIG;
import static io.aiven.kafka.connect.opensearch.sig4.OpenSearchSigV4ConfigDefContributor.AWS_STS_ROLE_SESSION_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.regions.Region;

@ExtendWith(MockitoExtension.class)
public class OpenSearchSigV4ConfiguratorTest {

    @Test
    void testConfigMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpenSearchSinkConnectorConfig(
                Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost"));
        final OpenSearchSigV4ClientConfigurator configurator = new OpenSearchSigV4ClientConfigurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder, never()).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }

    @Test
    void testAccessKeyIdMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", AWS_SECRET_ACCESS_KEY_CONFIG, "secret", AWS_REGION_CONFIG, "us-east-1"));
        final OpenSearchSigV4ClientConfigurator configurator = new OpenSearchSigV4ClientConfigurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder, never()).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }

    @Test
    void testSecretAccessKeyMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", AWS_ACCESS_KEY_ID_CONFIG, "id", AWS_REGION_CONFIG, "us-east-1"));
        final OpenSearchSigV4ClientConfigurator configurator = new OpenSearchSigV4ClientConfigurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder, never()).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }

    @Test
    void testRegionMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", AWS_ACCESS_KEY_ID_CONFIG, "id", AWS_SECRET_ACCESS_KEY_CONFIG, "secret"));
        final OpenSearchSigV4ClientConfigurator configurator = new OpenSearchSigV4ClientConfigurator();
        assertThrows(ConfigException.class, () -> configurator.apply(config, httpBuilder));

        verify(httpBuilder, never()).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }

    @Test
    void testServiceSigningName(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", AWS_ACCESS_KEY_ID_CONFIG, "id", AWS_SECRET_ACCESS_KEY_CONFIG, "secret",
                AWS_REGION_CONFIG, Region.EU_CENTRAL_1.toString()));
        final OpenSearchSigV4ClientConfigurator configurator = new OpenSearchSigV4ClientConfigurator();
        assertThrows(ConfigException.class, () -> configurator.apply(config, httpBuilder));

        verify(httpBuilder, never()).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }

    @Test
    void testImpossibleToSetBothCredentials(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG,
                "http://localhost", AWS_ACCESS_KEY_ID_CONFIG, "id", AWS_SECRET_ACCESS_KEY_CONFIG, "secret",
                AWS_REGION_CONFIG, "us-east-1", AWS_SERVICE_SIGNING_NAME_CONFIG, "some_service_name",
                AWS_STS_ROLE_ARN_CONFIG, "a::b:c", AWS_STS_ROLE_SESSION_NAME_CONFIG, "sadsada"));
        final OpenSearchSigV4ClientConfigurator configurator = new OpenSearchSigV4ClientConfigurator();
        assertThrows(ConfigException.class, () -> configurator.apply(config, httpBuilder));

        verify(httpBuilder, never()).addRequestInterceptorLast(any(Sig4HttpRequestInterceptor.class));
    }

}
