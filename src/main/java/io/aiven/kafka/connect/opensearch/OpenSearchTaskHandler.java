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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._helpers.bulk.BulkIngester;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.WaitForActiveShards;
import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch.indices.CreateDataStreamRequest;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.PutIndexTemplateRequest;
import org.opensearch.client.transport.BackoffPolicy;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import io.aiven.kafka.connect.opensearch.request.BulkOperationBuilder;
import io.aiven.kafka.connect.opensearch.request.ConnectorBulkListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchTaskHandler {

    public static final String DATA_STREAM_TEMPLATE_NAME_PATTERN = "%s-connector-data-stream-template";

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchTaskHandler.class);

    private final Set<String> indexCache = new HashSet<>();

    private final OpenSearchClient client;

    private final OpenSearchSinkConnectorConfig config;

    private final BulkOperationBuilder bulkOperationBuilder;

    private final BulkIngester<SinkRecord> bulkIngester;

    public OpenSearchTaskHandler(final OpenSearchSinkConnectorConfig config,
            final ErrantRecordReporter errantRecordReporter) {
        this.config = config;
        this.client = new OpenSearchClient(ApacheHttpClient5TransportBuilder.builder(this.config.httpHosts())
                .setHttpClientConfigCallback(new HttpClientConfigCallback(this.config))
                .build());
        this.bulkIngester = BulkIngester.of(b -> b.client(client)
                .backoffPolicy(BackoffPolicy.exponentialBackoff(config.retryBackoffMs(), config.maxRetry()))
                .maxOperations(this.config.maxBufferedRecords())
                .listener(new ConnectorBulkListener(config, errantRecordReporter)));
        this.bulkOperationBuilder = new BulkOperationBuilder(config);
    }

    public void put(final Collection<SinkRecord> records) {
        LOGGER.trace("Putting {} to OpenSearch", records);
        for (final var record : records) {
            try {
                ensureIndexOrDataStreamExists(config.topicToIndexNameConverter().apply(record.topic()), record);
                final var op = bulkOperationBuilder.buildFor(record);
                if (op != null) {
                    bulkIngester.add(op, record);
                }
            } catch (final DataException e) {
                if (config.dropInvalidMessage()) {
                    LOGGER.error("Couldn't convert record from topic {} with partition {} and offset {}. Reason: ",
                            record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
                } else {
                    throw e;
                }
            }
        }
    }

    private void ensureIndexOrDataStreamExists(final String indexName, final SinkRecord record) {
        if (!indexCache.contains(indexName)) {
            if (!config.dataStreamEnabled()) {
                createIndex(indexName, record);
            } else {
                createDataStreamIndex(indexName, record);
            }
            indexCache.add(indexName);
        }
    }

    private void createIndex(final String indexName, final SinkRecord record) {
        try {
            LOGGER.info("Creating index {}", indexName);
            final var createIndexRequestBuilder = CreateIndexRequest.builder().index(indexName);
            indexMapping(record).ifPresentOrElse(im -> createIndexRequestBuilder.mappings(m -> m.withJson(im)),
                    () -> createIndexRequestBuilder.mappings(m -> m.dynamic(DynamicMapping.True)));
            client.indices()
                    .create(createIndexRequestBuilder
                            .waitForActiveShards(WaitForActiveShards.builder().count(1).build())
                            .build());
        } catch (final OpenSearchException oe) {
            if ("resource_already_exists_exception".equals(oe.error().type())) {
                LOGGER.info("Index {} already exists", indexName);
            } else {
                throw new ConnectException("Couldn't create index " + indexName, oe);
            }
        } catch (final IOException e) {
            throw new RetriableException("Couldn't create index " + indexName, e);
        }
    }

    private void createDataStreamIndex(final String indexName, final SinkRecord record) {
        config.dataStreamExistingIndexTemplateName().ifPresentOrElse(userProvidedTemplate -> {
            var exists = false;
            try {
                exists = client.indices()
                        .existsIndexTemplate(ExistsIndexTemplateRequest.builder().name(userProvidedTemplate).build())
                        .value();
            } catch (final OpenSearchException oe) {
                throw new ConnectException("Couldn't check that template " + userProvidedTemplate + " exists", oe);
            } catch (final IOException e) {
                throw new RetriableException("Couldn't check that template " + userProvidedTemplate + " exists", e);
            }
            if (!exists)
                createDataStreamIndexTemplate(userProvidedTemplate, indexName);

        }, () -> createDataStreamIndexTemplate(String.format(DATA_STREAM_TEMPLATE_NAME_PATTERN, indexName), indexName));

        try {
            LOGGER.info("Creating data stream {}", indexName);
            client.indices().createDataStream(CreateDataStreamRequest.builder().name(indexName).build());
        } catch (final OpenSearchException oe) {
            if ("resource_already_exists_exception".equals(oe.error().type())) {
                LOGGER.info("Data stream {} already exists", indexName);
            } else {
                throw new ConnectException("Couldn't create index " + indexName, oe);
            }
        } catch (IOException e) {
            throw new RetriableException("Couldn't create data stream", e);
        }
        indexMapping(record).ifPresent(im -> {
            try {
                client.indices().putMapping(b -> b.index(indexName).source(s -> s.withJson(im)));
            } catch (final OpenSearchException oe) {
                throw new ConnectException("Couldn't create mapping for " + indexName, oe);
            } catch (final IOException e) {
                throw new RetriableException("Couldn't create mapping for " + indexName, e);
            }
        });

    }

    private void createDataStreamIndexTemplate(final String templateName, final String indexName) {
        try {
            LOGGER.info("Creating index template {} for data stream {}", templateName, indexName);
            client.indices()
                    .putIndexTemplate(PutIndexTemplateRequest.builder()
                            .name(templateName)
                            .priority(200)
                            .indexPatterns(indexName)
                            .dataStream(ds -> ds.timestampField(t -> t.name(config.dataStreamTimestampField())))
                            .build());
        } catch (final OpenSearchException oe) {
            if ("resource_already_exists_exception".equals(oe.error().type())) {
                LOGGER.info("Index template {} already exists", templateName);
            } else {
                throw new ConnectException("Couldn't create index " + indexName, oe);
            }
        } catch (IOException e) {
            throw new ConnectException("Couldn't create index template for datastream", e);
        }
    }

    private Optional<ByteArrayInputStream> indexMapping(final SinkRecord record) {
        return !config.ignoreSchemaFor(record.topic())
                ? Optional.of(Mapping.buildMappingFor(record.valueSchema()))
                : Optional.empty();
    }

    public void flush() {
        bulkIngester.flush();
    }

    public void close() {
        bulkIngester.close();
    }

}
