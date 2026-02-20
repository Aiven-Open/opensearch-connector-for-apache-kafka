/*
 * Copyright 2021 Aiven Oy
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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.aiven.kafka.connect.opensearch.basicauth.OpenSearchBasicAuthConfigDefContributor.CONNECTION_PASSWORD_CONFIG;
import static io.aiven.kafka.connect.opensearch.basicauth.OpenSearchBasicAuthConfigDefContributor.CONNECTION_USERNAME_CONFIG;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.test.TestUtils;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.HealthStatus;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.testcontainers.OpenSearchContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class AbstractIT {

    static OpenSearchContainer<?> openSearchContainer;

    OpenSearchClient opensearchClient;

    @BeforeAll
    static void beforeAll() {
        openSearchContainer = new OpenSearchContainer<>(getOpenSearchImage());
        openSearchContainer.start();
    }

    @AfterAll
    static void afterAll() {
        if (openSearchContainer != null) {
            openSearchContainer.stop();
        }
    }

    @BeforeEach
    void setup() throws Exception {
        final var props = new HashMap<>(getDefaultProperties());
        if (openSearchContainer.isSecurityEnabled())
            props.put("connection.trust.all.certificates", "true");
        final var config = new OpenSearchSinkConnectorConfig(props);
        opensearchClient = new OpenSearchClient(ApacheHttpClient5TransportBuilder.builder(config.httpHosts())
                .setHttpClientConfigCallback(new HttpClientConfigCallback(config))
                .build());
        TestUtils.waitForCondition(() -> {
            try {
                return Set.of(HealthStatus.Green, HealthStatus.Yellow)
                        .contains(opensearchClient.cluster().health().status());
            } catch (final Exception e) {
                return false;
            }
        }, TimeUnit.MINUTES.toMillis(1L), "Cluster hasn't finished formation");
    }

    static Map<String, String> getDefaultProperties() {
        return Map.of(CONNECTION_URL_CONFIG, openSearchContainer.getHttpHostAddress(), CONNECTION_USERNAME_CONFIG,
                "admin", CONNECTION_PASSWORD_CONFIG, openSearchContainer.getPassword(), READ_TIMEOUT_MS_CONFIG,
                "10000");
    }

    protected void waitForRecords(final String indexName, final int expectedRecords) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                opensearchClient.indices().refresh(r -> r.index(indexName));
                return expectedRecords == opensearchClient.count(c -> c.index(indexName)).count();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }, TimeUnit.MINUTES.toMillis(1L),
                String.format("Could not find expected documents (%d) in time.", expectedRecords));
    }

    protected static String getOpenSearchImage() {
        return "opensearchproject/opensearch:" + getOpenSearchVersion();
    }

    protected static String getOpenSearchVersion() {
        return System.getProperty("opensearch.testcontainers.image-version", "2.19.4");
    }
}
