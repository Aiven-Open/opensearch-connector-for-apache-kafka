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

import static io.aiven.kafka.connect.opensearch.OpensearchBasicAuthConfigurator.CONNECTION_PASSWORD_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchBasicAuthConfigurator.CONNECTION_USERNAME_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.test.TestUtils;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.search.SearchHits;
import org.opensearch.testcontainers.OpensearchContainer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class AbstractIT {

    @Container
    static OpensearchContainer opensearchContainer = new OpensearchContainer(getOpenSearchImage());

    OpensearchClient opensearchClient;

    @BeforeEach
    void setup() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(getDefaultProperties());
        opensearchClient = new OpensearchClient(config);
    }

    protected Map<String, String> getDefaultProperties() {
        return Map.of(CONNECTION_URL_CONFIG, opensearchContainer.getHttpHostAddress(), CONNECTION_USERNAME_CONFIG,
                "admin", CONNECTION_PASSWORD_CONFIG, "admin");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Objects.nonNull(opensearchClient)) {
            opensearchClient.close();
        }
    }

    protected SearchHits search(final String indexName) throws IOException {
        return opensearchClient.client.search(new SearchRequest(indexName), RequestOptions.DEFAULT).getHits();
    }

    protected void waitForRecords(final String indexName, final int expectedRecords) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                return expectedRecords == opensearchClient.client
                        .count(new CountRequest(indexName), RequestOptions.DEFAULT)
                        .getCount();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }, TimeUnit.MINUTES.toMillis(1L),
                String.format("Could not find expected documents (%d) in time.", expectedRecords));
    }

    private static String getOpenSearchImage() {
        return "opensearchproject/opensearch:" + getOpenSearchVersion();
    }

    protected static String getOpenSearchVersion() {
        return System.getProperty("opensearch.testcontainers.image-version", "2.0.0");
    }
}
