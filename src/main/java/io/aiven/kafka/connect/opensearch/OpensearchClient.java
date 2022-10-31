/*
 * Copyright 2020 Aiven Oy
 * Copyright 2018 Confluent Inc.
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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.PutMappingRequest;

import io.aiven.kafka.connect.opensearch.spi.ClientsConfiguratorProvider;
import io.aiven.kafka.connect.opensearch.spi.OpensearchClientConfigurator;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpensearchClient.class);

    private static final String RESOURCE_ALREADY_EXISTS_EXCEPTION = "resource_already_exists_exception";

    private static final String RESOURCE_ALREADY_EXISTS_AS_ALIAS = "already exists as alias";

    private static final String RESOURCE_ALREADY_EXISTS_AS_DATASTREAM =
        "creates data streams only, use create data stream api instead";

    private static final String DEFAULT_OS_VERSION = "1.0.0";

    private final OpensearchSinkConnectorConfig config;

    private final BulkProcessor bulkProcessor;

    /* visible for testing */
    protected final RestHighLevelClient client;

    public OpensearchClient(final OpensearchSinkConnectorConfig config) {
        this(
                new RestHighLevelClient(
                        RestClient.builder(config.httpHosts())
                                .setHttpClientConfigCallback(
                                        new HttpClientConfigCallback(config)
                                )
                ),
                config
        );
    }

    protected OpensearchClient(final RestHighLevelClient client, final OpensearchSinkConnectorConfig config) {
        this.client = client;
        this.config = config;
        this.bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);
        this.bulkProcessor.start();
    }

    public String getVersion() {
        return withRetry("get version", () -> {
            try {
                final var version = client.info(RequestOptions.DEFAULT).getVersion().getNumber();
                return Objects.isNull(version) ? DEFAULT_OS_VERSION : version;
            } catch (final Exception e) {
                // Insufficient privileges to get version number.
                // Since OS comes with the security plugin, we need to take such behave into account
                LOGGER.warn("Couldn't get OS version. Use default " + DEFAULT_OS_VERSION, e);
                return DEFAULT_OS_VERSION;
            }
        });
    }

    public boolean indexExists(final String index) {
        return withRetry(
                String.format("check index %s exists", index),
                () -> client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)
        );
    }

    public boolean createIndex(final String index) {
        return withRetry(
                String.format("create index %s", index),
                () -> {
                    try {
                        client.indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
                        return true;
                    } catch (final OpenSearchStatusException | IOException e) {
                        if (!(e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION)
                            || e.getMessage().contains(RESOURCE_ALREADY_EXISTS_AS_ALIAS)
                            || e.getMessage().contains(RESOURCE_ALREADY_EXISTS_AS_DATASTREAM))) {
                            throw e;
                        }
                        return false;
                    }
                });
    }

    public void createMapping(final String index, final Schema schema) {
        final var request = new PutMappingRequest(index).source(Mapping.buildMappingFor(schema));
        withRetry(
                String.format("create mapping for index %s with schema %s", index, schema),
                () -> client.indices().putMapping(request, RequestOptions.DEFAULT)
        );
    }

    public boolean hasMapping(final String index) {
        final var request = new GetMappingsRequest().indices(index);
        final var response = withRetry(
                "",
                () -> client.indices().getMapping(request, RequestOptions.DEFAULT)
        );
        final var mappings = response.mappings().get(index);
        return Objects.nonNull(mappings)
                && Objects.nonNull(mappings.sourceAsMap())
                && !mappings.sourceAsMap().isEmpty();
    }

    public void index(final DocWriteRequest<?> indexRequest) {
        bulkProcessor.add(indexRequest, config.flushTimeoutMs());
    }

    public void flush() {
        bulkProcessor.flush(config.flushTimeoutMs());
    }

    public void close() throws IOException {
        try {
            bulkProcessor.flush(config.flushTimeoutMs());
        } catch (final Exception e) {
            LOGGER.warn("Failed to flush during stop", e);
        }
        bulkProcessor.stop();
        bulkProcessor.awaitStop(config.flushTimeoutMs());
        if (Objects.nonNull(client)) {
            client.close();
        }
    }

    private static class HttpClientConfigCallback implements RestClientBuilder.HttpClientConfigCallback {

        private final OpensearchSinkConnectorConfig config;

        private HttpClientConfigCallback(final OpensearchSinkConnectorConfig config) {
            this.config = config;
        }

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(final HttpAsyncClientBuilder httpClientBuilder) {
            final var requestConfig = RequestConfig.custom()
                    .setConnectTimeout(config.connectionTimeoutMs())
                    .setConnectionRequestTimeout(config.readTimeoutMs())
                    .setSocketTimeout(config.readTimeoutMs())
                    .build();

            final Collection<OpensearchClientConfigurator> configurators = ClientsConfiguratorProvider
                .forOpensearch(config);
            configurators.forEach(configurator -> configurator.apply(config, httpClientBuilder));
            
            httpClientBuilder
                    .setConnectionManager(createConnectionManager())
                    .setDefaultRequestConfig(requestConfig);

            return httpClientBuilder;
        }

        private PoolingNHttpClientConnectionManager createConnectionManager() {
            try {
                final var ioReactorConfig = IOReactorConfig.custom()
                        .setConnectTimeout(config.connectionTimeoutMs())
                        .setSoTimeout(config.readTimeoutMs())
                        .build();

                final var sslStrategy = new SSLIOSessionStrategy(
                        SSLContexts.custom().loadTrustMaterial(new TrustSelfSignedStrategy()).build(),
                        new NoopHostnameVerifier());
                final var registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                        .register("http", NoopIOSessionStrategy.INSTANCE)
                        .register("https", sslStrategy)
                        .build();
                final var connectionManager =
                        new PoolingNHttpClientConnectionManager(
                                new DefaultConnectingIOReactor(ioReactorConfig),
                                registry
                        );
                final var maxPerRoute = Math.max(10, config.maxInFlightRequests() * 2);
                connectionManager.setDefaultMaxPerRoute(maxPerRoute);
                connectionManager.setMaxTotal(maxPerRoute * config.httpHosts().length);
                return connectionManager;
            } catch (final IOReactorException
                    | NoSuchAlgorithmException
                    | KeyStoreException
                    | KeyManagementException e) {
                throw new ConnectException("Unable to open ElasticsearchClient.", e);
            }
        }

    }

    public <T> T withRetry(final String callName, final Callable<T> callable) {
        return RetryUtil.callWithRetry(callName, callable, config.maxRetry(), config.retryBackoffMs());
    }


}
