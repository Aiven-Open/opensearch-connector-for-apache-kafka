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

import java.util.Map;

import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class OpensearchSigV4ConfiguratorTest {

    @Test
    void testApplyInterceptor(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpensearchSinkConnectorConfig(
            Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSigV4Configurator.AWS_ACCESS_KEY_ID_CONFIG, "id",
                OpensearchSigV4Configurator.AWS_SECRET_ACCESS_KEY_CONFIG, "secret",
                OpensearchSigV4Configurator.AWS_REGION_CONFIG, "us-east-1"
            )
        );
        final OpensearchSigV4Configurator configurator = new OpensearchSigV4Configurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder).addInterceptorLast(any(HttpRequestInterceptor.class));
    }

    @Test
    void testConfigMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpensearchSinkConnectorConfig(
            Map.of(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost")
        );
        final OpensearchSigV4Configurator configurator = new OpensearchSigV4Configurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder, never()).addInterceptorLast(any(HttpRequestInterceptor.class));
    }

    @Test
    void testAccessKeyIdMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpensearchSinkConnectorConfig(
            Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSigV4Configurator.AWS_SECRET_ACCESS_KEY_CONFIG, "secret",
                OpensearchSigV4Configurator.AWS_REGION_CONFIG, "us-east-1"
            )
        );
        final OpensearchSigV4Configurator configurator = new OpensearchSigV4Configurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder, never()).addInterceptorLast(any(HttpRequestInterceptor.class));
    }

    @Test
    void testSecretAccessKeyMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpensearchSinkConnectorConfig(
            Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSigV4Configurator.AWS_ACCESS_KEY_ID_CONFIG, "id",
                OpensearchSigV4Configurator.AWS_REGION_CONFIG, "us-east-1"
            )
        );
        final OpensearchSigV4Configurator configurator = new OpensearchSigV4Configurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder, never()).addInterceptorLast(any(HttpRequestInterceptor.class));
    }

    @Test
    void testRegionMissing(final @Mock HttpAsyncClientBuilder httpBuilder) {
        final var config = new OpensearchSinkConnectorConfig(
            Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSigV4Configurator.AWS_ACCESS_KEY_ID_CONFIG, "id",
                OpensearchSigV4Configurator.AWS_SECRET_ACCESS_KEY_CONFIG, "secret"
            )
        );
        final OpensearchSigV4Configurator configurator = new OpensearchSigV4Configurator();
        configurator.apply(config, httpBuilder);

        verify(httpBuilder, never()).addInterceptorLast(any(HttpRequestInterceptor.class));
    }
}
