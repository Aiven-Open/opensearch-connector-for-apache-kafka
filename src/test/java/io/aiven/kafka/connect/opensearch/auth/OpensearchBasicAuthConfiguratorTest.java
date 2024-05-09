/*
 * Copyright 2024 Aiven Oy
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

import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.aiven.kafka.connect.opensearch.auth.OpensearchBasicAuthConfigurator.CLIENT_SSL_PROTOCOL_TYPE;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig;

import org.junit.jupiter.api.Test;

class OpensearchBasicAuthConfiguratorTest {

    @Test
    public void shouldFailIfSLSProtocolIsUnknown() {
        final var config = new OpensearchBasicAuthConfigurator();
        assertThrows(ConfigException.class,
                () -> config.apply(
                        new OpensearchSinkConnectorConfig(
                                Map.of(CONNECTION_URL_CONFIG, "http://localhost:9200", CLIENT_SSL_PROTOCOL_TYPE, "A")),
                        null));
    }

}
