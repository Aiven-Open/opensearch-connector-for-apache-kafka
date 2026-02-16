/*
 * Copyright 2023 Aiven Oy
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
package io.aiven.kafka.connect.opensearch.request;

import static java.util.Objects.isNull;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkOperationBase;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.util.BinaryData;
import org.opensearch.client.util.ContentType;

import io.aiven.kafka.connect.opensearch.BehaviorOnNullValues;
import io.aiven.kafka.connect.opensearch.DocumentIDStrategy;
import io.aiven.kafka.connect.opensearch.IndexWriteMethod;
import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;
import io.aiven.kafka.connect.opensearch.OpenSearchTaskHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BulkOperationBuilder {

    private final OpenSearchSinkConnectorConfig config;

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchTaskHandler.class);

    public BulkOperationBuilder(final OpenSearchSinkConnectorConfig config) {
        this.config = config;
    }

    public BulkOperation buildFor(final SinkRecord record) {
        final var indexName = config.topicToIndexNameConverter().apply(record.topic());
        if (record.value() == null) {
            switch (config.behaviorOnNullValues()) {
                case IGNORE :
                    LOGGER.debug("Ignoring sink record with key {} and null value for topic/partition/offset {}/{}/{}",
                            record.key(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
                    return null;
                case DELETE :
                    if (record.key() == null) {
                        // Since the record key is used as the ID of the index to delete and we don't have a key
                        // for this record, we can't delete anything anyways, so we ignore the record.
                        // We can also disregard the value of the ignoreKey parameter, since even if it's true
                        // the resulting index we'd try to delete would be based solely off topic/partition/
                        // offset information for the SinkRecord. Since that information is guaranteed to be
                        // unique per message, we can be confident that there wouldn't be any corresponding
                        // index present in ES to delete anyways.
                        return null;
                    }
                    // Will proceed as normal, ultimately creating an with a null payload
                    break;
                case FAIL :
                default :
                    throw new DataException(String.format(
                            "Sink record with key of %s and null value encountered for topic/partition/offset "
                                    + "%s/%s/%s (to ignore future records like this change the configuration property "
                                    + "'%s' from '%s' to '%s')",
                            record.key(), record.topic(), record.kafkaPartition(), record.kafkaOffset(),
                            OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, BehaviorOnNullValues.FAIL,
                            BehaviorOnNullValues.IGNORE));
            }
        }
        final var documentIDStrategy = config.documentIdStrategy(record.topic());
        final var documentId = documentIDStrategy.documentId(record);
        final BulkOperation bulkOperation;
        if (isNull(record.value())) {
            final var deleteOperationBuilder = new DeleteOperation.Builder().id(documentId).index(indexName);
            if (!config.dataStreamEnabled()) {
                addVersionIfAny(deleteOperationBuilder, documentIDStrategy, record);
            }
            bulkOperation = BulkOperation.of(b -> b.delete(deleteOperationBuilder.build()));
        } else {
            final var payload = DocumentPayload.buildPayload(config, record);
            final var binaryData = BinaryData.of(payload, ContentType.APPLICATION_JSON);
            if (config.indexWriteMethod() == IndexWriteMethod.UPSERT) {
                bulkOperation = BulkOperation.of(b -> b.update(u -> u.id(documentId)
                        .document(binaryData)
                        .upsert(binaryData)
                        .docAsUpsert(true)
                        .index(indexName)
                        .retryOnConflict(Math.min(config.maxInFlightRequests(), 3))));
            } else {
                if (config.dataStreamEnabled()) {
                    bulkOperation = BulkOperation
                            .of(b -> b.create(c -> c.id(documentId).index(indexName).document(binaryData)));
                } else {
                    final var indexOperationBuilder = new IndexOperation.Builder<>().id(documentId)
                            .index(indexName)
                            .document(binaryData);
                    addVersionIfAny(indexOperationBuilder, documentIDStrategy, record);
                    bulkOperation = BulkOperation.of(b -> b.index(indexOperationBuilder.build()));
                }
            }
        }
        return bulkOperation;
    }

    private void addVersionIfAny(final BulkOperationBase.AbstractBuilder<?> operationBuilder,
            final DocumentIDStrategy documentIDStrategy, final SinkRecord record) {
        if (documentIDStrategy == DocumentIDStrategy.RECORD_KEY) {
            operationBuilder.version(record.kafkaOffset());
            operationBuilder.versionType(VersionType.External);
        } else {
            operationBuilder.versionType(VersionType.Internal);
        }
    }

}
