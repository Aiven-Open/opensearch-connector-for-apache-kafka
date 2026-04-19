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
package io.aiven.kafka.connect.opensearch.request;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.client.opensearch._helpers.bulk.BulkListener;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;

import io.aiven.kafka.connect.opensearch.BehaviorOnMalformedDoc;
import io.aiven.kafka.connect.opensearch.BehaviorOnVersionConflict;
import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectorBulkListener implements BulkListener<SinkRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorBulkListener.class);

    private final static Set<String> MALFORMED_DOC_ERRORS = Set.of("strict_dynamic_mapping_exception",
            "mapper_parsing_exception", "illegal_argument_exception", "action_request_validation_exception");

    private final static Set<String> VERSION_CONFLICT_ERRORS = Set.of("version_conflict_engine_exception");

    private final BehaviorOnMalformedDoc behaviorOnMalformedDoc;

    private final BehaviorOnVersionConflict behaviorOnVersionConflict;

    private final ErrantRecordReporter errantRecordReporter;

    private final ReentrantLock bulkFinishedLock = new ReentrantLock();

    private final Condition bulkFinishedCondition = bulkFinishedLock.newCondition();

    private volatile boolean bulkFinished = false;

    public ConnectorBulkListener(final OpenSearchSinkConnectorConfig config,
            final ErrantRecordReporter errantRecordReporter) {
        this.behaviorOnMalformedDoc = config.behaviorOnMalformedDoc();
        this.behaviorOnVersionConflict = config.behaviorOnVersionConflict();
        this.errantRecordReporter = errantRecordReporter;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request, List<SinkRecord> records) {
        bulkFinished = false;
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, List<SinkRecord> records, BulkResponse response) {
        finishBulk(() -> {
            if (response.errors()) {
                final var items = response.items();
                for (var i = 0; i < items.size(); i++) {
                    final var itm = items.get(i);
                    if (itm.error() == null)
                        continue;
                    if (MALFORMED_DOC_ERRORS.contains(itm.error().type())) {
                        handleMalformedDoc(executionId, response.items().size(), itm.error().reason(), records.get(i));
                    } else if (VERSION_CONFLICT_ERRORS.contains(itm.error().type())) {
                        handleVersionConflict(executionId, response.items().size(), itm.error().reason(),
                                records.get(i));
                    } else {
                        LOGGER.error("Bulk {} has errors. Item #{}. Reason: {}", executionId, i,
                                itm.error().toJsonString());
                    }
                }
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        });
    }

    private void handleMalformedDoc(final long executionId, final int itemsSize, final String reason,
            final SinkRecord sinkRecord) {
        // if the OpenSearch request failed because of a malformed document,
        // the behavior is configurable.
        switch (behaviorOnMalformedDoc) {
            case IGNORE :
                LOGGER.debug(
                        "Encountered an illegal document error when executing bulk {} of {}"
                                + " records. Ignoring and will not index record. Error was {}",
                        executionId, itemsSize, reason);
                break;
            case REPORT :
                LOGGER.warn("Encountered a version conflict when executing batch {} of {}"
                        + " records. Reporting this error to the errant record reporter" + " and will not index record."
                        + " Error message: {}", executionId, itemsSize, reason);
                errantRecordReporter.report(sinkRecord, new Exception(reason));
                break;
            case WARN :
                LOGGER.warn(
                        "Encountered an illegal document error when executing batch {} of {}"
                                + " records. Ignoring and will not index record. Error was {}",
                        executionId, itemsSize, reason);
                break;
            case ERROR :
            default :
                LOGGER.error(
                        "Encountered an illegal document error when executing batch {} of {}"
                                + " records. Error was {} (to ignore future records like this"
                                + " change the configuration property '{}' from '{}' to '{}').",
                        executionId, itemsSize, reason, OpenSearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
                        BehaviorOnMalformedDoc.ERROR, BehaviorOnMalformedDoc.IGNORE);
        }
    }

    private void handleVersionConflict(final long executionId, final int itemsSize, final String reason,
            final SinkRecord sinkRecord) {
        // if the OpenSearch request failed because of a version conflict,
        // the behavior is configurable.
        switch (behaviorOnVersionConflict) {
            case IGNORE :
                LOGGER.debug(
                        "Encountered a version conflict when executing batch {} of {}"
                                + " records. Ignoring and will keep an existing record. Error was {}",
                        executionId, itemsSize, reason);
                break;
            case REPORT :
                final String errorMessage = String.format("Encountered a version conflict when executing batch %s of %s"
                        + " records. Reporting this error to the errant record reporter and will"
                        + " keep an existing record." + " Error message: %s", executionId, itemsSize, reason);
                errantRecordReporter.report(sinkRecord, new Exception(errorMessage));
                break;
            case WARN :
                LOGGER.warn(
                        "Encountered a version conflict when executing batch {} of {}"
                                + " records. Ignoring and will keep an existing record. Error was {}",
                        executionId, itemsSize, reason);
                break;
            case ERROR :
            default :
                LOGGER.error(
                        "Encountered a version conflict when executing batch {} of {}"
                                + " records. Error was {} (to ignore version conflicts you may consider"
                                + " changing the configuration property '{}' from '{}' to '{}').",
                        executionId, itemsSize, reason,
                        OpenSearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, BehaviorOnMalformedDoc.ERROR,
                        BehaviorOnMalformedDoc.IGNORE);
        }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, List<SinkRecord> voids, Throwable failure) {
        LOGGER.error("Bulk {} has failed. Error is", executionId, failure);
        finishBulk(() -> Boolean.FALSE);
    }

    private void finishBulk(final Supplier<Boolean> handler) {
        bulkFinishedLock.lock();
        try {
            bulkFinished = handler.get();
        } finally {
            bulkFinishedCondition.signalAll();
            bulkFinishedLock.unlock();
        }
    }

    public boolean success() {
        bulkFinishedLock.lock();
        try {
            while (!bulkFinished) {
                try {
                    bulkFinishedCondition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ConnectException(e);
                }
            }
            return bulkFinished;
        } finally {
            bulkFinishedLock.unlock();
        }
    }

}
