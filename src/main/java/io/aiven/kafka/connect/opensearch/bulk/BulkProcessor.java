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
package io.aiven.kafka.connect.opensearch.bulk;

import static io.aiven.kafka.connect.opensearch.bulk.RetryUtil.callWithRetry;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

import io.aiven.kafka.connect.opensearch.BehaviorOnMalformedDoc;
import io.aiven.kafka.connect.opensearch.BehaviorOnVersionConflict;
import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import org.apache.hc.core5.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkProcessor.class);

    private static final AtomicLong BATCH_ID_GEN = new AtomicLong();

    private final Time time;
    private final OpenSearchClient client;
    private final int maxBufferedRecords;
    private final int batchSize;
    private final long lingerMs;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final BehaviorOnMalformedDoc behaviorOnMalformedDoc;
    private final BehaviorOnVersionConflict behaviorOnVersionConflict;

    private final Thread farmer;
    private final ExecutorService executor;

    // thread-safe state, can be mutated safely without synchronization,
    // but may be part of synchronized(this) wait() conditions so need to notifyAll() on changes
    private volatile boolean stopRequested = false;
    private volatile boolean flushRequested = false;
    private final AtomicReference<ConnectException> error = new AtomicReference<>();

    // shared state, synchronized on (this), may be part of wait() conditions so need notifyAll() on
    // changes
    private final Deque<BulkOperationWrapper> unsentRecords;
    private int inFlightRecords = 0;

    private final ErrantRecordReporter reporter;

    public BulkProcessor(final Time time, final OpenSearchClient client, final OpenSearchSinkConnectorConfig config) {
        this(time, client, config, null);
    }

    public BulkProcessor(final Time time, final OpenSearchClient client, final OpenSearchSinkConnectorConfig config,
            final ErrantRecordReporter reporter) {
        this.time = time;
        this.client = client;

        this.maxBufferedRecords = config.maxBufferedRecords();
        this.batchSize = config.batchSize();
        this.lingerMs = config.lingerMs();
        this.maxRetries = config.maxRetry();
        this.retryBackoffMs = config.retryBackoffMs();
        this.behaviorOnMalformedDoc = config.behaviorOnMalformedDoc();
        this.behaviorOnVersionConflict = config.behaviorOnVersionConflict();
        this.reporter = reporter;

        unsentRecords = new ArrayDeque<>(maxBufferedRecords);

        final ThreadFactory threadFactory = makeThreadFactory();
        farmer = threadFactory.newThread(farmerTask());
        executor = Executors.newFixedThreadPool(config.maxInFlightRequests(), threadFactory);

        if (!config.ignoreKey() && config.behaviorOnVersionConflict() == BehaviorOnVersionConflict.FAIL) {
            LOGGER.warn(
                    "The {} is set to `false` which assumes external version and optimistic locking."
                            + " You may consider changing the configuration property '{}' from '{}' to '{}' or '{}'"
                            + " to deal with possible version conflicts.",
                    OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG,
                    OpenSearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, BehaviorOnMalformedDoc.FAIL,
                    BehaviorOnMalformedDoc.IGNORE, BehaviorOnMalformedDoc.WARN);
        }
    }

    private ThreadFactory makeThreadFactory() {
        final AtomicInteger threadCounter = new AtomicInteger();
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> {
            LOGGER.error("Uncaught exception in BulkProcessor thread {}", t, e);
            failAndStop(e);
        };
        return new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final int threadId = threadCounter.getAndIncrement();
                final int objId = System.identityHashCode(this);
                final Thread t = new Thread(r, String.format("BulkProcessor@%d-%d", objId, threadId));
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
                return t;
            }
        };
    }

    private Runnable farmerTask() {
        return () -> {
            LOGGER.debug("Starting farmer task");
            try {
                while (!stopRequested) {
                    submitBatchWhenReady();
                }
            } catch (final InterruptedException e) {
                throw new ConnectException(e);
            }
            LOGGER.debug("Finished farmer task");
        };
    }

    // Visible for testing
    synchronized Future<BulkResponse> submitBatchWhenReady() throws InterruptedException {
        for (long waitStartTimeMs = time.milliseconds(), elapsedMs = 0; !stopRequested
                && !canSubmit(elapsedMs); elapsedMs = time.milliseconds() - waitStartTimeMs) {
            // when linger time has already elapsed, we still have to ensure the other submission
            // conditions hence the wait(0) in that case
            wait(Math.max(0, lingerMs - elapsedMs));
        }
        // at this point, either stopRequested or canSubmit
        return stopRequested ? null : submitBatch();
    }

    private synchronized Future<BulkResponse> submitBatch() {
        assert !unsentRecords.isEmpty();
        final int batchableSize = Math.min(batchSize, unsentRecords.size());
        final var batch = new ArrayList<BulkOperationWrapper>(batchableSize);
        for (int i = 0; i < batchableSize; i++) {
            batch.add(unsentRecords.removeFirst());
        }
        inFlightRecords += batchableSize;
        return executor.submit(new BulkTask(batch, maxRetries, retryBackoffMs));
    }

    /**
     * Submission is possible when there are unsent records and:
     * <ul>
     * <li>flush is called, or</li>
     * <li>the linger timeout passes, or</li>
     * <li>there are sufficient records to fill a batch</li>
     * </ul>
     */
    private synchronized boolean canSubmit(final long elapsedMs) {
        return !unsentRecords.isEmpty()
                && (flushRequested || elapsedMs >= lingerMs || unsentRecords.size() >= batchSize);
    }

    /**
     * Start concurrently creating and sending batched requests using the client.
     */
    public void start() {
        farmer.start();
    }

    /**
     * Initiate shutdown.
     *
     * <p>
     * Pending buffered records are not automatically flushed, so call {@link #flush(long)} before this method if this
     * is desirable.
     */
    public void stop() {
        LOGGER.trace("stop");
        stopRequested = true;
        synchronized (this) {
            // shutdown the pool under synchronization to avoid rejected submissions
            executor.shutdown();
            notifyAll();
        }
    }

    /**
     * Block upto {@code timeoutMs} till shutdown is complete.
     *
     * <p>
     * This should only be called after a previous {@link #stop()} invocation.
     */
    public void awaitStop(final long timeoutMs) {
        LOGGER.trace("awaitStop {}", timeoutMs);
        assert stopRequested;
        try {
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new ConnectException("Timed-out waiting for executor termination");
            }
        } catch (final InterruptedException e) {
            throw new ConnectException(e);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * @return whether {@link #stop()} has been requested
     */
    public boolean isStopping() {
        return stopRequested;
    }

    /**
     * @return whether any task failed with an error
     */
    public boolean isFailed() {
        return error.get() != null;
    }

    /**
     * @return {@link #isTerminal()} or {@link #isFailed()}
     */
    public boolean isTerminal() {
        return isStopping() || isFailed();
    }

    /**
     * Throw a {@link ConnectException} if {@link #isStopping()}.
     */
    public void throwIfStopping() {
        if (stopRequested) {
            throw new ConnectException("Stopping");
        }
    }

    /**
     * Throw the relevant {@link ConnectException} if {@link #isFailed()}.
     */
    public void throwIfFailed() {
        if (isFailed()) {
            throw error.get();
        }
    }

    /**
     * {@link #throwIfFailed()} and {@link #throwIfStopping()}
     */
    public void throwIfTerminal() {
        throwIfFailed();
        throwIfStopping();
    }

    /**
     * Add a record, may block upto {@code timeoutMs} if at capacity with respect to {@code maxBufferedRecords}.
     *
     * <p>
     * If any task has failed prior to or while blocked in the add, or if the timeout expires while blocked,
     * {@link ConnectException} will be thrown.
     */
    public synchronized void add(final BulkOperation bulkOperation, final SinkRecord sinkRecord, final long timeoutMs) {
        throwIfTerminal();

        if (bufferedRecords() >= maxBufferedRecords) {
            final long addStartTimeMs = time.milliseconds();
            for (long elapsedMs = time.milliseconds() - addStartTimeMs; !isTerminal() && elapsedMs < timeoutMs
                    && bufferedRecords() >= maxBufferedRecords; elapsedMs = time.milliseconds() - addStartTimeMs) {
                try {
                    wait(timeoutMs - elapsedMs);
                } catch (final InterruptedException e) {
                    throw new ConnectException(e);
                }
            }
            throwIfTerminal();
            if (bufferedRecords() >= maxBufferedRecords) {
                throw new ConnectException("Add timeout expired before buffer availability");
            }
        }

        unsentRecords.addLast(new BulkOperationWrapper(bulkOperation, sinkRecord));
        notifyAll();
    }

    /**
     * Request a flush and block upto {@code timeoutMs} until all pending records have been flushed.
     *
     * <p>
     * If any task has failed prior to or during the flush, {@link ConnectException} will be thrown with that error.
     */
    public void flush(final long timeoutMs) {
        LOGGER.trace("flush {}", timeoutMs);
        final long flushStartTimeMs = time.milliseconds();
        try {
            flushRequested = true;
            synchronized (this) {
                notifyAll();
                for (long elapsedMs = time.milliseconds() - flushStartTimeMs; !isTerminal() && elapsedMs < timeoutMs
                        && bufferedRecords() > 0; elapsedMs = time.milliseconds() - flushStartTimeMs) {
                    wait(timeoutMs - elapsedMs);
                }
                throwIfTerminal();
                if (bufferedRecords() > 0) {
                    throw new ConnectException("Flush timeout expired with unflushed records: " + bufferedRecords());
                }
            }
        } catch (final InterruptedException e) {
            throw new ConnectException(e);
        } finally {
            flushRequested = false;
        }
    }

    private final class BulkTask implements Callable<BulkResponse> {

        final long batchId = BATCH_ID_GEN.incrementAndGet();

        final List<BulkOperationWrapper> batch;

        final int maxRetries;

        final long retryBackoffMs;

        BulkTask(final List<BulkOperationWrapper> batch, final int maxRetries, final long retryBackoffMs) {
            this.batch = batch;
            this.maxRetries = maxRetries;
            this.retryBackoffMs = retryBackoffMs;
        }

        @Override
        public BulkResponse call() throws Exception {
            try {
                final var rsp = execute();
                LOGGER.debug("Successfully executed batch {} of {} records", batchId, batch.size());
                onBatchCompletion(batch.size());
                return rsp;
            } catch (final Exception e) {
                failAndStop(e);
                throw e;
            }
        }

        private void sendToErrantRecordReporter(final String errorMessage, final SinkRecord batchRecord) {
            LOGGER.debug(errorMessage);
            reporter.report(batchRecord, new Exception(errorMessage));
        }

        private BulkResponse execute() throws Exception {
            class RetriableError extends RuntimeException {
                private static final long serialVersionUID = 1L;

                public RetriableError(final String errorMessage) {
                    super(errorMessage);
                }

                public RetriableError(final Throwable cause) {
                    super(cause);
                }
            }

            return callWithRetry("bulk processing", () -> {
                try {
                    final var response = client.bulk(BulkRequest.of(b -> b.operations(
                            batch.stream().map(BulkOperationWrapper::bulkOperation).collect(Collectors.toList()))));
                    if (!response.errors()) {
                        // We only logged failures, so log the success immediately after a failure ...
                        LOGGER.debug("Completed batch {} of {} records", batchId, batch.size());
                        return response;
                    }
                    for (var i = 0; i < response.items().size(); i++) {
                        final var item = response.items().get(i);
                        if (item.error() != null) {
                            if (item.status() == HttpStatus.SC_TOO_MANY_REQUESTS) {
                                if (responseContainsMalformedDocError(item)) {
                                    handleMalformedDoc(item, i);
                                } else if (responseContainsVersionConflict(item)) {
                                    handleVersionConflict(item, i);
                                } else {
                                    throw new RetriableError("One of the item in the bulk response failed. Reason: "
                                            + item.error().reason());
                                }
                            } else {
                                throw new ConnectException("One of the item in the bulk response aborted. Reason: "
                                        + item.error().reason());
                            }
                        }
                    }
                    return response;
                } catch (final IOException e) {
                    LOGGER.warn("Failed to send bulk request from batch {} of {} records", batchId, batch.size(), e);
                    throw new RetriableError(e);
                }
            }, maxRetries, retryBackoffMs, RetriableError.class);
        }

        private void handleVersionConflict(final BulkResponseItem bulkResponseItem, final int idx) {
            // if the OpenSearch request failed because of a version conflict,
            // the behavior is configurable.
            switch (behaviorOnVersionConflict) {
                case IGNORE :
                    LOGGER.debug(
                            "Encountered a version conflict when executing batch {} of {}"
                                    + " records. Ignoring and will keep an existing record. Error was {}",
                            batchId, batch.size(), bulkResponseItem.error().reason());
                    break;
                case REPORT :
                    final String errorMessage = String.format(
                            "Encountered a version conflict when executing batch %s of %s"
                                    + " records. Reporting this error to the errant record reporter and will"
                                    + " keep an existing record."
                                    + " Rest status: %s, Action id: %s, Error message: %s",
                            batchId, batch.size(), bulkResponseItem.status(), idx, bulkResponseItem.error().reason());
                    sendToErrantRecordReporter(errorMessage, batch.get(idx).sinkRecord());
                    break;
                case WARN :
                    LOGGER.warn(
                            "Encountered a version conflict when executing batch {} of {}"
                                    + " records. Ignoring and will keep an existing record. Error was {}",
                            batchId, batch.size(), bulkResponseItem.error().reason());
                    break;
                case FAIL :
                default :
                    LOGGER.error(
                            "Encountered a version conflict when executing batch {} of {}"
                                    + " records. Error was {} (to ignore version conflicts you may consider"
                                    + " changing the configuration property '{}' from '{}' to '{}').",
                            batchId, batch.size(), bulkResponseItem.error().reason(),
                            OpenSearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG,
                            BehaviorOnMalformedDoc.FAIL, BehaviorOnMalformedDoc.IGNORE);
                    throw new ConnectException("One of the item in the bulk response failed. Reason: "
                            + bulkResponseItem.error().reason());
            }
        }

        private void handleMalformedDoc(final BulkResponseItem bulkResponseItem, final int idx) {
            // if the OpenSearch request failed because of a malformed document,
            // the behavior is configurable.
            switch (behaviorOnMalformedDoc) {
                case IGNORE :
                    LOGGER.debug(
                            "Encountered an illegal document error when executing batch {} of {}"
                                    + " records. Ignoring and will not index record. Error was {}",
                            batchId, batch.size(), bulkResponseItem.error().reason());
                    break;
                case REPORT :
                    final String errorMessage = String.format(
                            "Encountered a version conflict when executing batch %s of %s"
                                    + " records. Reporting this error to the errant record reporter"
                                    + " and will not index record."
                                    + " Rest status: %s, Action id: %s, Error message: %s",
                            batchId, batch.size(), bulkResponseItem.status(), idx, bulkResponseItem.error().reason());
                    sendToErrantRecordReporter(errorMessage, batch.get(idx).sinkRecord());
                    break;
                case WARN :
                    LOGGER.warn(
                            "Encountered an illegal document error when executing batch {} of {}"
                                    + " records. Ignoring and will not index record. Error was {}",
                            batchId, batch.size(), bulkResponseItem.error().reason());
                    break;
                case FAIL :
                default :
                    LOGGER.error(
                            "Encountered an illegal document error when executing batch {} of {}"
                                    + " records. Error was {} (to ignore future records like this"
                                    + " change the configuration property '{}' from '{}' to '{}').",
                            batchId, batch.size(), bulkResponseItem.error().reason(),
                            OpenSearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
                            BehaviorOnMalformedDoc.FAIL, BehaviorOnMalformedDoc.IGNORE);
                    throw new ConnectException("Bulk request failed: " + bulkResponseItem.error().reason());
            }
        }
    }

    private static final class BulkOperationWrapper {

        private final BulkOperation bulkOperation;
        private final SinkRecord sinkRecord;

        BulkOperationWrapper(final BulkOperation bulkOperation, final SinkRecord sinkRecord) {
            this.bulkOperation = bulkOperation;
            this.sinkRecord = sinkRecord;
        }

        public BulkOperation bulkOperation() {
            return bulkOperation;
        }

        public SinkRecord sinkRecord() {
            return sinkRecord;
        }
    }

    private boolean responseContainsVersionConflict(final BulkResponseItem bulkItemResponse) {
        return bulkItemResponse.status() == HttpStatus.SC_CONFLICT
                || bulkItemResponse.error().reason().contains("version_conflict_engine_exception");
    }

    private boolean responseContainsMalformedDocError(final BulkResponseItem bulkItemResponse) {
        final var reason = bulkItemResponse.error().reason();
        return reason.contains("strict_dynamic_mapping_exception") || reason.contains("mapper_parsing_exception")
                || reason.contains("illegal_argument_exception")
                || reason.contains("action_request_validation_exception");
    }

    private synchronized void onBatchCompletion(final int batchSize) {
        inFlightRecords -= batchSize;
        assert inFlightRecords >= 0;
        notifyAll();
    }

    private void failAndStop(final Throwable t) {
        error.compareAndSet(null, toConnectException(t));
        stop();
    }

    /**
     * @return sum of unsent and in-flight record counts
     */
    public synchronized int bufferedRecords() {
        return unsentRecords.size() + inFlightRecords;
    }

    private static ConnectException toConnectException(final Throwable t) {
        if (t instanceof ConnectException) {
            return (ConnectException) t;
        } else {
            return new ConnectException(t);
        }
    }

}
