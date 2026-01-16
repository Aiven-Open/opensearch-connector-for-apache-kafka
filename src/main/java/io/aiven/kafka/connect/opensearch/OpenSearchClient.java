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

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.ComposableIndexTemplateExistRequest;
import org.opensearch.client.indices.CreateDataStreamRequest;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.PutComposableIndexTemplateRequest;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.opensearch.cluster.metadata.DataStream.TimestampField;

import io.aiven.kafka.connect.opensearch.spi.ClientsConfiguratorProvider;
import io.aiven.kafka.connect.opensearch.spi.OpenSearchClientConfigurator;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchClient.class);

    public static final String DATA_STREAM_TEMPLATE_NAME_PATTERN = "%s-connector-data-stream-template";

    private static final String RESOURCE_ALREADY_EXISTS_EXCEPTION = "resource_already_exists_exception";

    private static final String RESOURCE_ALREADY_EXISTS_AS_ALIAS = "already exists as alias";

    private static final String RESOURCE_ALREADY_EXISTS_AS_DATASTREAM = "creates data streams only, use create data stream api instead";

    private static final String DEFAULT_OS_VERSION = "1.0.0";

    private final OpenSearchSinkConnectorConfig config;

    private final BulkProcessor bulkProcessor;

    /* visible for testing */
    protected final RestHighLevelClient client;

    public OpenSearchClient(final OpenSearchSinkConnectorConfig config) {
        this(config, null);
    }

    public OpenSearchClient(final OpenSearchSinkConnectorConfig config, final ErrantRecordReporter reporter) {
        this(new RestHighLevelClient(RestClient.builder(config.httpHosts())
                .setHttpClientConfigCallback(new HttpClientConfigCallback(config))), config, reporter);
    }

    protected OpenSearchClient(final RestHighLevelClient client, final OpenSearchSinkConnectorConfig config,
            final ErrantRecordReporter reporter) {
        this.client = client;
        this.config = config;
        this.bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config, reporter);
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

    public boolean indexOrDataStreamExists(final String index) {
        return withRetry(String.format("check index %s exists", index),
                () -> client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT));
    }

    protected boolean dataStreamIndexTemplateExists(final String dataStreamIndexTemplate) {
        return withRetry(String.format("check index template exists %s", dataStreamIndexTemplate),
                () -> client.indices()
                        .existsIndexTemplate(new ComposableIndexTemplateExistRequest(dataStreamIndexTemplate),
                                RequestOptions.DEFAULT));
    }

    protected boolean createDataStreamIndexTemplate(final String dataStreamName,
            final String dataStreamTimestampField) {
        final var dataStreamIndexTemplate = String.format(DATA_STREAM_TEMPLATE_NAME_PATTERN, dataStreamName);
        if (!dataStreamIndexTemplateExists(dataStreamIndexTemplate)) {
            return withRetry(String.format("create index template %s", dataStreamIndexTemplate), () -> {
                try {
                    client.indices()
                            .putIndexTemplate(
                                    new PutComposableIndexTemplateRequest().name(dataStreamIndexTemplate)
                                            .indexTemplate(new ComposableIndexTemplate(List.of(dataStreamName), null,
                                                    null, 200L, null, null,
                                                    new DataStreamTemplate(
                                                            new TimestampField(dataStreamTimestampField)))),
                                    RequestOptions.DEFAULT);
                } catch (final OpenSearchStatusException | IOException e) {
                    if (!(e.getMessage().contains(RESOURCE_ALREADY_EXISTS_EXCEPTION))) {
                        throw e;
                    }
                    return false;
                }
                return true;
            });
        }
        return true;
    }

    public boolean createIndexTemplateAndDataStream(final String dataStreamName,
            final String dataStreamTimestampField) {
        if (createDataStreamIndexTemplate(dataStreamName, dataStreamTimestampField)) {
            return withRetry(String.format("create data stream %s", dataStreamName), () -> {
                try {
                    client.indices()
                            .createDataStream(new CreateDataStreamRequest(dataStreamName), RequestOptions.DEFAULT);
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
        return false;
    }

    public boolean createIndex(final String index) {
        return withRetry(String.format("create index %s", index), () -> {
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
        withRetry(String.format("create mapping for index %s with schema %s", index, schema),
                () -> client.indices().putMapping(request, RequestOptions.DEFAULT));
    }

    public boolean hasMapping(final String index) {
        final var request = new GetMappingsRequest().indices(index);
        final var response = withRetry("", () -> client.indices().getMapping(request, RequestOptions.DEFAULT));
        final var mappings = response.mappings().get(index);
        return Objects.nonNull(mappings) && Objects.nonNull(mappings.sourceAsMap())
                && !mappings.sourceAsMap().isEmpty();
    }

    public void index(final DocWriteRequest<?> indexRequest, final SinkRecord record) {
        bulkProcessor.add(indexRequest, record, config.flushTimeoutMs());
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

    private record HttpClientConfigCallback(
            OpenSearchSinkConnectorConfig config) implements RestClientBuilder.HttpClientConfigCallback {

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(final HttpAsyncClientBuilder httpClientBuilder) {
            final Collection<OpenSearchClientConfigurator> configurators = ClientsConfiguratorProvider
                    .forOpensearch(config);
            configurators.forEach(configurator -> {
                if (configurator.apply(config, httpClientBuilder)) {
                    LOGGER.debug("Successfully applied {} configurator to OpensearchClient",
                            configurator.getClass().getName());
                }
            });

            httpClientBuilder.setConnectionManager(createConnectionManager())
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setConnectionRequestTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS)
                            .build());

            return httpClientBuilder;
        }

        private PoolingAsyncClientConnectionManager createConnectionManager() {
            try {
                final var maxPerRoute = Math.max(10, config.maxInFlightRequests() * 2);
                return PoolingAsyncClientConnectionManagerBuilder.create()
                        .setTlsStrategy(ClientTlsStrategyBuilder.create()
                                .setTlsVersions(TLS.V_1_2, TLS.V_1_3)
                                .setSslContext(SSLContexts.custom().loadTrustMaterial(new TrustAllStrategy()).build())
                                .build())
                        .setDefaultConnectionConfig(ConnectionConfig.custom()
                                .setConnectTimeout(config.connectionTimeoutMs(), TimeUnit.MILLISECONDS)
                                .setSocketTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS)
                                .build())
                        .setMaxConnPerRoute(maxPerRoute)
                        .setMaxConnTotal(maxPerRoute * config.httpHosts().length)
                        .build();
            } catch (final NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
                throw new ConnectException("Unable to open OpenSearchClient.", e);
            }
        }

    }

    public <T> T withRetry(final String callName, final Callable<T> callable) {
        return RetryUtil.callWithRetry(callName, callable, config.maxRetry(), config.retryBackoffMs());
    }

}
