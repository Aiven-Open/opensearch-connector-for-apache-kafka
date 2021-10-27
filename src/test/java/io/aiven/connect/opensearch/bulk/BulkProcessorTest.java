/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.connect.opensearch.bulk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.aiven.connect.opensearch.bulk.BulkProcessor.BehaviorOnMalformedDoc;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled
public class BulkProcessorTest {

    private static class Expectation {
        final List<Integer> request;
        final BulkResponse response;

        private Expectation(final List<Integer> request, final BulkResponse response) {
            this.request = request;
            this.response = response;
        }
    }

    private static final class Client implements BulkClient<Integer, List<Integer>> {
        private final Queue<Expectation> expectQ = new LinkedList<>();
        private volatile boolean executeMetExpectations = true;

        @Override
        public List<Integer> bulkRequest(final List<Integer> batch) {
            final List<Integer> ids = new ArrayList<>(batch.size());
            for (final Integer id : batch) {
                ids.add(id);
            }
            return ids;
        }

        public void expect(final List<Integer> ids, final BulkResponse response) {
            expectQ.add(new Expectation(ids, response));
        }

        public boolean expectationsMet() {
            return expectQ.isEmpty() && executeMetExpectations;
        }

        @Override
        public BulkResponse execute(final List<Integer> request) throws IOException {
            final Expectation expectation;
            try {
                expectation = expectQ.remove();
                assertEquals(expectation.request, request);
            } catch (final Throwable t) {
                executeMetExpectations = false;
                throw t;
            }
            executeMetExpectations &= true;
            return expectation.response;
        }
    }

    Client client;

    @BeforeEach
    public void createClient() {
        client = new Client();
    }

    @AfterEach
    public void checkClient() {
        assertTrue(client.expectationsMet());
    }

    @Test
    public void batchingAndLingering() throws InterruptedException, ExecutionException {
        final int maxBufferedRecords = 100;
        final int maxInFlightBatches = 5;
        final int batchSize = 5;
        final int lingerMs = 5;
        final int maxRetries = 0;
        final int retryBackoffMs = 0;
        final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;

        final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                Time.SYSTEM,
                client,
                maxBufferedRecords,
                maxInFlightBatches,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnMalformedDoc
        );

        final int addTimeoutMs = 10;
        bulkProcessor.add(1, addTimeoutMs);
        bulkProcessor.add(2, addTimeoutMs);
        bulkProcessor.add(3, addTimeoutMs);
        bulkProcessor.add(4, addTimeoutMs);
        bulkProcessor.add(5, addTimeoutMs);
        bulkProcessor.add(6, addTimeoutMs);
        bulkProcessor.add(7, addTimeoutMs);
        bulkProcessor.add(8, addTimeoutMs);
        bulkProcessor.add(9, addTimeoutMs);
        bulkProcessor.add(10, addTimeoutMs);
        bulkProcessor.add(11, addTimeoutMs);
        bulkProcessor.add(12, addTimeoutMs);

        client.expect(Arrays.asList(1, 2, 3, 4, 5), BulkResponse.success());
        client.expect(Arrays.asList(6, 7, 8, 9, 10), BulkResponse.success());
        client.expect(Arrays.asList(11, 12), BulkResponse.success()); // batch not full, but upon linger timeout
        assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
        assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
        assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    }

    @Test
    public void flushing() {
        final int maxBufferedRecords = 100;
        final int maxInFlightBatches = 5;
        final int batchSize = 5;
        final int lingerMs = 100000; // super high on purpose to make sure flush is what's causing the request
        final int maxRetries = 0;
        final int retryBackoffMs = 0;
        final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;

        final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                Time.SYSTEM,
                client,
                maxBufferedRecords,
                maxInFlightBatches,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnMalformedDoc
        );

        client.expect(Arrays.asList(1, 2, 3), BulkResponse.success());

        bulkProcessor.start();

        final int addTimeoutMs = 10;
        bulkProcessor.add(1, addTimeoutMs);
        bulkProcessor.add(2, addTimeoutMs);
        bulkProcessor.add(3, addTimeoutMs);

        assertFalse(client.expectationsMet());

        final int flushTimeoutMs = 100;
        bulkProcessor.flush(flushTimeoutMs);
    }

    @Test
    public void addBlocksWhenBufferFull() {
        final int maxBufferedRecords = 1;
        final int maxInFlightBatches = 1;
        final int batchSize = 1;
        final int lingerMs = 10;
        final int maxRetries = 0;
        final int retryBackoffMs = 0;
        final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;

        final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                Time.SYSTEM,
                client,
                maxBufferedRecords,
                maxInFlightBatches,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnMalformedDoc
        );

        final int addTimeoutMs = 10;
        bulkProcessor.add(42, addTimeoutMs);
        assertEquals(1, bulkProcessor.bufferedRecords());
        bulkProcessor.add(43, addTimeoutMs);
        fail();
    }

    @Test
    public void retriableErrors() throws InterruptedException, ExecutionException {
        final int maxBufferedRecords = 100;
        final int maxInFlightBatches = 5;
        final int batchSize = 2;
        final int lingerMs = 5;
        final int maxRetries = 3;
        final int retryBackoffMs = 1;
        final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;

        client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retiable error"));
        client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error again"));
        client.expect(Arrays.asList(42, 43), BulkResponse.success());

        final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                Time.SYSTEM,
                client,
                maxBufferedRecords,
                maxInFlightBatches,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnMalformedDoc
        );

        final int addTimeoutMs = 10;
        bulkProcessor.add(42, addTimeoutMs);
        bulkProcessor.add(43, addTimeoutMs);

        assertTrue(bulkProcessor.submitBatchWhenReady().get().succeeded);
    }

    @Test
    public void retriableErrorsHitMaxRetries() throws InterruptedException, ExecutionException {
        final int maxBufferedRecords = 100;
        final int maxInFlightBatches = 5;
        final int batchSize = 2;
        final int lingerMs = 5;
        final int maxRetries = 2;
        final int retryBackoffMs = 1;
        final String errorInfo = "a final retriable error again";
        final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;

        client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retiable error"));
        client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, "a retriable error again"));
        client.expect(Arrays.asList(42, 43), BulkResponse.failure(true, errorInfo));

        final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                Time.SYSTEM,
                client,
                maxBufferedRecords,
                maxInFlightBatches,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnMalformedDoc
        );

        final int addTimeoutMs = 10;
        bulkProcessor.add(42, addTimeoutMs);
        bulkProcessor.add(43, addTimeoutMs);

        try {
            bulkProcessor.submitBatchWhenReady().get();
            fail();
        } catch (final ExecutionException e) {
            assertTrue(e.getCause().getMessage().contains(errorInfo));
        }
    }

    @Test
    public void unretriableErrors() throws InterruptedException {
        final int maxBufferedRecords = 100;
        final int maxInFlightBatches = 5;
        final int batchSize = 2;
        final int lingerMs = 5;
        final int maxRetries = 3;
        final int retryBackoffMs = 1;
        final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.DEFAULT;

        final String errorInfo = "an unretriable error";
        client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo));

        final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                Time.SYSTEM,
                client,
                maxBufferedRecords,
                maxInFlightBatches,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnMalformedDoc
        );

        final int addTimeoutMs = 10;
        bulkProcessor.add(42, addTimeoutMs);
        bulkProcessor.add(43, addTimeoutMs);

        try {
            bulkProcessor.submitBatchWhenReady().get();
            fail();
        } catch (final ExecutionException e) {
            assertTrue(e.getCause().getMessage().contains(errorInfo));
        }
    }

    @Test
    public void failOnMalformedDoc() throws InterruptedException {
        final int maxBufferedRecords = 100;
        final int maxInFlightBatches = 5;
        final int batchSize = 2;
        final int lingerMs = 5;
        final int maxRetries = 3;
        final int retryBackoffMs = 1;
        final BehaviorOnMalformedDoc behaviorOnMalformedDoc = BehaviorOnMalformedDoc.FAIL;

        final String errorInfo =
                " [{\"type\":\"mapper_parsing_exception\","
                        + "\"reason\":\"failed to parse\","
                        + "\"caused_by\":{\"type\":\"illegal_argument_exception\","
                        + "\"reason\":\"object\n"
                        + " field starting or ending with a [.] "
                        + "makes object resolution "
                        + "ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
        client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo));

        final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                Time.SYSTEM,
                client,
                maxBufferedRecords,
                maxInFlightBatches,
                batchSize,
                lingerMs,
                maxRetries,
                retryBackoffMs,
                behaviorOnMalformedDoc
        );

        bulkProcessor.start();

        bulkProcessor.add(42, 1);
        bulkProcessor.add(43, 1);

        try {
            final int flushTimeoutMs = 1000;
            bulkProcessor.flush(flushTimeoutMs);
            fail();
        } catch (final ConnectException e) {
            // expected
            assertTrue(e.getMessage().contains("mapper_parsing_exception"));
        }
    }

    @Test
    public void ignoreOrWarnOnMalformedDoc() throws InterruptedException {
        final int maxBufferedRecords = 100;
        final int maxInFlightBatches = 5;
        final int batchSize = 2;
        final int lingerMs = 5;
        final int maxRetries = 3;
        final int retryBackoffMs = 1;

        // Test both IGNORE and WARN options
        // There is no difference in logic between IGNORE and WARN, except for the logging.
        // Test to ensure they both work the same logically
        final List<BehaviorOnMalformedDoc> behaviorsToTest = Arrays.asList(
                BehaviorOnMalformedDoc.WARN,
                BehaviorOnMalformedDoc.IGNORE
        );

        for (final BehaviorOnMalformedDoc behaviorOnMalformedDoc : behaviorsToTest) {
            final String errorInfo =
                    " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\","
                            + "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n"
                            + " field starting or ending with a [.] "
                            + "makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
            client.expect(Arrays.asList(42, 43), BulkResponse.failure(false, errorInfo));

            final BulkProcessor<Integer, ?> bulkProcessor = new BulkProcessor<>(
                    Time.SYSTEM,
                    client,
                    maxBufferedRecords,
                    maxInFlightBatches,
                    batchSize,
                    lingerMs,
                    maxRetries,
                    retryBackoffMs,
                    behaviorOnMalformedDoc
            );

            bulkProcessor.start();

            bulkProcessor.add(42, 1);
            bulkProcessor.add(43, 1);

            try {
                final int flushTimeoutMs = 1000;
                bulkProcessor.flush(flushTimeoutMs);
            } catch (final ConnectException e) {
                fail(e.getMessage());
            }
        }
    }
}
