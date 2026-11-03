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

package io.aiven.kafka.connect.opensearch;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BulkProcessorFailureTest {
    @Test
    void backgroundErrorPreventsFlushAndFurtherAdmission() throws Exception {
        final var client = mock(RestHighLevelClient.class);
        final var failure = new OutOfMemoryError("bulk allocation failed");
        when(client.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(failure);
        final var processor = new BulkProcessor(Time.SYSTEM, client, new OpensearchSinkConnectorConfig(Map.of(
                "connection.url", "http://localhost", "batch.size", "1", "max.retries", "0")));
        processor.start();
        try {
            processor.add(new IndexRequest("test"), null, 1000);
            assertSame(failure, assertThrows(ConnectException.class, () -> processor.flush(1000)).getCause());
            assertSame(failure, assertThrows(ConnectException.class, () -> processor.flush(1000)).getCause());
            assertSame(failure, assertThrows(ConnectException.class,
                    () -> processor.add(new IndexRequest("test"), null, 1000)).getCause());
        } finally {
            processor.stop();
            processor.awaitStop(1000);
        }
    }

    @Test
    void backgroundErrorWakesWaitingAddAndFlush() throws Exception {
        final var client = mock(RestHighLevelClient.class);
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var failure = new OutOfMemoryError("bulk allocation failed");
        when(client.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenAnswer(invocation -> {
            entered.countDown();
            release.await();
            throw failure;
        });
        final var processor = new BulkProcessor(Time.SYSTEM, client, new OpensearchSinkConnectorConfig(Map.of(
                "connection.url", "http://localhost", "batch.size", "1", "max.buffered.records", "1")));
        final var add = new FutureTask<>(() -> assertThrows(ConnectException.class,
                () -> processor.add(new IndexRequest("test"), null, 30000)));
        final var flush = new FutureTask<>(() -> assertThrows(ConnectException.class, () -> processor.flush(30000)));
        final var adding = new Thread(add);
        final var flushing = new Thread(flush);
        processor.start();
        try {
            processor.add(new IndexRequest("test"), null, 1000);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            // Start flush first because it notifies existing waiters.
            flushing.start();
            awaitWaiting(flushing);
            adding.start();
            awaitWaiting(adding);
            release.countDown();
            assertSame(failure, add.get(5, TimeUnit.SECONDS).getCause());
            assertSame(failure, flush.get(5, TimeUnit.SECONDS).getCause());
        } finally {
            release.countDown();
            processor.stop();
            processor.awaitStop(1000);
            adding.join(1000);
            flushing.join(1000);
        }
    }

    @Test
    void firstFailureSurvivesAnotherInFlightFailureAndStop() throws Exception {
        final var client = mock(RestHighLevelClient.class);
        final var entered = new CountDownLatch(2);
        final var firstRelease = new CountDownLatch(1);
        final var secondRelease = new CountDownLatch(1);
        final var first = new OutOfMemoryError("first failure");
        final var second = new IllegalStateException("second failure");
        when(client.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenAnswer(invocation -> {
            final BulkRequest request = invocation.getArgument(0);
            entered.countDown();
            if (request.requests().get(0).id().equals("first")) {
                firstRelease.await();
                throw first;
            }
            secondRelease.await();
            throw second;
        });
        final var processor = new BulkProcessor(Time.SYSTEM, client, new OpensearchSinkConnectorConfig(Map.of(
                "connection.url", "http://localhost", "batch.size", "1", "max.in.flight.requests", "2")));
        processor.start();
        try {
            processor.add(new IndexRequest("test").id("first"), null, 1000);
            processor.add(new IndexRequest("test").id("second"), null, 1000);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            firstRelease.countDown();
            assertSame(first, assertThrows(ConnectException.class, () -> processor.flush(5000)).getCause());
            secondRelease.countDown();
            processor.awaitStop(5000);
            assertSame(first, assertThrows(ConnectException.class, processor::throwIfFailed).getCause());
            assertSame(first, assertThrows(ConnectException.class, () -> processor.flush(1000)).getCause());
        } finally {
            firstRelease.countDown();
            secondRelease.countDown();
            processor.stop();
            processor.awaitStop(1000);
        }
    }

    private static void awaitWaiting(final Thread thread) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (thread.getState() == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(1);
        }
        fail("Thread did not enter a timed wait");
    }
}
