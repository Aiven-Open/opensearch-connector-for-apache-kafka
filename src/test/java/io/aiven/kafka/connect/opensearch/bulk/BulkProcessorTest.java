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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.OperationType;

import io.aiven.kafka.connect.opensearch.BehaviorOnMalformedDoc;
import io.aiven.kafka.connect.opensearch.BehaviorOnVersionConflict;
import io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig;

import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

@ExtendWith(MockitoExtension.class)
public class BulkProcessorTest {

    private static class Expectation {
        final List<BulkOperation> operations;
        final BulkResponse response;

        private Expectation(final List<BulkOperation> requests, final BulkResponse response) {
            this.operations = requests;
            this.response = response;
        }
    }

    private static final class ClientAnswer implements Answer<BulkResponse> {

        private final Queue<Expectation> expectQ = new LinkedList<>();

        @Override
        public BulkResponse answer(final InvocationOnMock invocation) throws Throwable {
            final Expectation expectation;
            try {
                final var request = invocation.getArgument(0, BulkRequest.class);
                final var bulkRequestSources = request.operations()
                        .stream()
                        .map(BulkOperation::index)
                        .map(o -> String.valueOf((byte) o.document()))
                        .collect(Collectors.toList());
                expectation = expectQ.remove();
                assertEquals(request.operations().size(), expectation.operations.size());
                assertEquals(expectation.operations.stream()
                        .map(BulkOperation::index)
                        .map(o -> String.valueOf((byte) o.document()))
                        .collect(Collectors.toList()), bulkRequestSources);
            } catch (final Throwable t) {
                throw t;
            }
            return expectation.response;
        }

        public void expect(final List<BulkOperation> operations, final BulkResponse response) {
            expectQ.add(new Expectation(operations, response));
        }

        public boolean expectationsMet() {
            return expectQ.isEmpty();
        }

    }

    @Test
    public void batchingAndLingering(final @Mock OpenSearchClient client)
            throws IOException, InterruptedException, ExecutionException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "5",
                LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "0", READ_TIMEOUT_MS_CONFIG, "0",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.DEFAULT.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);

        final int addTimeoutMs = 10;
        bulkProcessor.add(newIndexRequest(1), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(2), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(3), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(4), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(5), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(6), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(7), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(8), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(9), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(10), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(11), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(12), newSinkRecord(), addTimeoutMs);

        clientAnswer.expect(List.of(newIndexRequest(1), newIndexRequest(2), newIndexRequest(3), newIndexRequest(4),
                newIndexRequest(5)), successResponse());
        clientAnswer.expect(List.of(newIndexRequest(6), newIndexRequest(7), newIndexRequest(8), newIndexRequest(9),
                newIndexRequest(10)), successResponse());
        clientAnswer.expect(List.of(newIndexRequest(11), newIndexRequest(12)), successResponse());

        // batch not full, but upon linger timeout
        assertFalse(bulkProcessor.submitBatchWhenReady().get().errors());
        assertFalse(bulkProcessor.submitBatchWhenReady().get().errors());
        assertFalse(bulkProcessor.submitBatchWhenReady().get().errors());

        verify(client, times(3)).bulk(any(BulkRequest.class));
        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void flushing(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "5",
                // super high on purpose to make sure flush is what's causing the request
                LINGER_MS_CONFIG, "100000", MAX_RETRIES_CONFIG, "0", READ_TIMEOUT_MS_CONFIG, "0",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.DEFAULT.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);

        clientAnswer.expect(List.of(newIndexRequest(1), newIndexRequest(2), newIndexRequest(3)), successResponse());

        bulkProcessor.start();

        final int addTimeoutMs = 10;
        bulkProcessor.add(newIndexRequest(1), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(2), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(3), newSinkRecord(), addTimeoutMs);

        assertFalse(clientAnswer.expectationsMet());

        final int flushTimeoutMs = 100;
        bulkProcessor.flush(flushTimeoutMs);

        verify(client, times(1)).bulk(any(BulkRequest.class));
        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void addBlocksWhenBufferFull(final @Mock OpenSearchClient client) {
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "1", MAX_IN_FLIGHT_REQUESTS_CONFIG, "1", BATCH_SIZE_CONFIG, "1",
                LINGER_MS_CONFIG, "10", MAX_RETRIES_CONFIG, "0", READ_TIMEOUT_MS_CONFIG, "0",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.DEFAULT.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);

        final int addTimeoutMs = 10;
        bulkProcessor.add(newIndexRequest(42), newSinkRecord(), addTimeoutMs);
        assertEquals(1, bulkProcessor.bufferedRecords());
        assertThrows(ConnectException.class,
                () -> bulkProcessor.add(newIndexRequest(43), newSinkRecord(), addTimeoutMs));
    }

    @Test
    public void retryableErrors(final @Mock OpenSearchClient client)
            throws IOException, InterruptedException, ExecutionException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);

        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "3",
                LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.DEFAULT.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);

        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse());
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse());
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), successResponse());

        final int addTimeoutMs = 10;
        bulkProcessor.add(newIndexRequest(42), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(43), newSinkRecord(), addTimeoutMs);

        assertFalse(bulkProcessor.submitBatchWhenReady().get().errors());

        verify(client, times(3)).bulk(any(BulkRequest.class));
        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void retryableErrorsHitMaxRetries(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);

        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "2", READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.DEFAULT.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);

        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse());
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse());
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse());

        final int addTimeoutMs = 10;
        bulkProcessor.add(newIndexRequest(42), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(43), newSinkRecord(), addTimeoutMs);

        assertThrows(ExecutionException.class, () -> bulkProcessor.submitBatchWhenReady().get());
        verify(client, times(3)).bulk(any(BulkRequest.class));
        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void nonRetryableErrors(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);

        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.DEFAULT.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse(true));

        final int addTimeoutMs = 10;
        bulkProcessor.add(newIndexRequest(42), newSinkRecord(), addTimeoutMs);
        bulkProcessor.add(newIndexRequest(43), newSinkRecord(), addTimeoutMs);

        assertThrows(ExecutionException.class, () -> bulkProcessor.submitBatchWhenReady().get());
        verify(client, times(1)).bulk(any(BulkRequest.class));
        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void failOnMalformedDoc(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);
        final String errorInfo = " [{\"type\":\"mapper_parsing_exception\"," + "\"reason\":\"failed to parse\","
                + "\"caused_by\":{\"type\":\"illegal_argument_exception\"," + "\"reason\":\"object\n"
                + " field starting or ending with a [.] " + "makes object resolution "
                + "ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.FAIL.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse(errorInfo));

        bulkProcessor.start();

        bulkProcessor.add(newIndexRequest(42), newSinkRecord(), 1);
        bulkProcessor.add(newIndexRequest(43), newSinkRecord(), 1);

        assertThrows(ConnectException.class, () -> bulkProcessor.flush(1000));
        verify(client, times(1)).bulk(any(BulkRequest.class));
        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void ignoreOrWarnOnMalformedDoc(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);

        // Test both IGNORE and WARN options
        // There is no difference in logic between IGNORE and WARN, except for the logging.
        // Test to ensure they both work the same logically
        final List<BehaviorOnMalformedDoc> behaviorsToTest = List.of(BehaviorOnMalformedDoc.WARN,
                BehaviorOnMalformedDoc.IGNORE);
        for (final BehaviorOnMalformedDoc behaviorOnMalformedDoc : behaviorsToTest) {
            final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                    MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                    LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1",
                    BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, behaviorOnMalformedDoc.toString()));
            final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\","
                    + "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n"
                    + " field starting or ending with a [.] "
                    + "makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
            final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);
            clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse(errorInfo));

            bulkProcessor.start();

            bulkProcessor.add(newIndexRequest(42), newSinkRecord(), 1);
            bulkProcessor.add(newIndexRequest(43), newSinkRecord(), 1);

            final int flushTimeoutMs = 1000;
            bulkProcessor.flush(flushTimeoutMs);

            assertTrue(clientAnswer.expectationsMet());
        }
    }

    @Test
    public void failOnVersionConfict(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);
        final String errorInfo = " [{\"type\":\"version_conflict_engine_exception\","
                + "\"reason\":\"[1]: version conflict, current version [3] is higher or"
                + " equal to the one provided [3]\"" + "}]";
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, BehaviorOnVersionConflict.FAIL.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse(errorInfo));

        bulkProcessor.start();

        bulkProcessor.add(newIndexRequest(42), newSinkRecord(), 1);
        bulkProcessor.add(newIndexRequest(43), newSinkRecord(), 1);

        assertThrows(ConnectException.class, () -> bulkProcessor.flush(1000));
        verify(client, times(1)).bulk(any(BulkRequest.class));
        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void ignoreOnVersionConfict(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);
        final String errorInfo = " [{\"type\":\"version_conflict_engine_exception\","
                + "\"reason\":\"[1]: version conflict, current version [3] is higher or"
                + " equal to the one provided [3]\"" + "}]";
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "5", MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, BehaviorOnVersionConflict.IGNORE.toString()));
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config);
        clientAnswer.expect(List.of(newIndexRequest(42), newIndexRequest(43)), failedResponse(errorInfo));

        bulkProcessor.start();

        bulkProcessor.add(newIndexRequest(42), newSinkRecord(), 1);
        bulkProcessor.add(newIndexRequest(43), newSinkRecord(), 1);
        bulkProcessor.flush(1000);

        assertTrue(clientAnswer.expectationsMet());
    }

    @Test
    public void reportToDlqWhenVersionConflictBehaviorIsReport(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);

        final var dlqReporter = mock(ErrantRecordReporter.class);
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, BehaviorOnMalformedDoc.REPORT.toString()));

        final String errorInfo = " [{\"type\":\"version_conflict_engine_exception\","
                + "\"reason\":\"[1]: version conflict, current version [3] is higher or"
                + " equal to the one provided [3]\"" + "}]";

        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config, dlqReporter);
        clientAnswer.expect(List.of(newIndexRequest(111)), failedResponse(errorInfo, false));

        bulkProcessor.start();

        bulkProcessor.add(newIndexRequest(111), newSinkRecord(), 1);

        final int flushTimeoutMs = 1000;
        bulkProcessor.flush(flushTimeoutMs);

        assertTrue(clientAnswer.expectationsMet());
        verify(dlqReporter, times(1)).report(any(SinkRecord.class), any(Throwable.class));
    }

    @Test
    public void reportToDlqWhenMalformedDocBehaviorIsReport(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);

        final var dlqReporter = mock(ErrantRecordReporter.class);
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.REPORT.toString()));
        final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\","
                + "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n"
                + " field starting or ending with a [.] "
                + "makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config, dlqReporter);
        clientAnswer.expect(List.of(newIndexRequest(111)), failedResponse(errorInfo, false));

        bulkProcessor.start();

        bulkProcessor.add(newIndexRequest(111), newSinkRecord(), 1);

        final int flushTimeoutMs = 1000;
        bulkProcessor.flush(flushTimeoutMs);

        assertTrue(clientAnswer.expectationsMet());
        verify(dlqReporter, times(1)).report(any(SinkRecord.class), any(Throwable.class));
    }

    @Test
    public void doNotReportToDlqWhenReportIsNotConfigured(final @Mock OpenSearchClient client) throws IOException {
        final var clientAnswer = new ClientAnswer();
        when(client.bulk(any(BulkRequest.class))).thenAnswer(clientAnswer);

        final var dlqReporter = mock(ErrantRecordReporter.class);
        final var config = new OpenSearchSinkConnectorConfig(Map.of(CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100", MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "1000", MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, BehaviorOnMalformedDoc.WARN.toString()));
        final String errorInfo = " [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse\","
                + "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"object\n"
                + " field starting or ending with a [.] "
                + "makes object resolution ambiguous: [avjpz{{.}}wjzse{{..}}gal9d]\"}}]";
        final var bulkProcessor = new BulkProcessor(Time.SYSTEM, client, config, dlqReporter);
        clientAnswer.expect(List.of(newIndexRequest(222)), failedResponse(errorInfo, false));

        bulkProcessor.start();

        bulkProcessor.add(newIndexRequest(222), newSinkRecord(), 1);

        final int flushTimeoutMs = 1000;
        bulkProcessor.flush(flushTimeoutMs);

        assertTrue(clientAnswer.expectationsMet());
        verify(dlqReporter, never()).report(any(SinkRecord.class), any(Throwable.class));
    }

    private SinkRecord newSinkRecord() {
        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("test_field", ThreadLocalRandom.current().nextInt());
        return new SinkRecord("test_topic", 0, Schema.STRING_SCHEMA, ThreadLocalRandom.current().nextLong(), null,
                valueMap, ThreadLocalRandom.current().nextInt());
    }

    BulkOperation newIndexRequest(final int body) {
        return BulkOperation.of(
                b -> b.index(IndexOperation.of(io -> io.id("some_id").document(Integer.valueOf(body).byteValue()))));
        // new IndexRequest("idx").id("some_id").source(body, XContentType.JSON);
    }

    private BulkResponse successResponse() {
        return BulkResponse.of(b -> b.errors(false).took(100L).items(List.of()));
    }

    private BulkResponse failedResponse() {
        return failedResponse("", false);
    }

    private BulkResponse failedResponse(final String failureMessage) {
        return failedResponse(failureMessage, false);
    }

    private BulkResponse failedResponse(final boolean abortable) {
        return failedResponse("", abortable);
    }

    private BulkResponse failedResponse(final String failureMessage, final boolean abortable) {
        final var filedResponse = BulkResponseItem
                .of(b -> b.error(ErrorCause.builder().type("some_type").reason(failureMessage).build())
                        .operationType(OperationType.Index)
                        .index("some_index")
                        .status(abortable ? HttpStatus.SC_BAD_REQUEST : HttpStatus.SC_TOO_MANY_REQUESTS));
        return BulkResponse.of(b -> b.errors(true).took(100L).items(List.of(filedResponse)));
    }

}
