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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.errors.ConnectException;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OpensearchClientCloseTest {
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void closesTransportAndPreservesShutdownFailure(final boolean interrupted, final boolean closeFails)
            throws Exception {
        final var transport = mock(RestHighLevelClient.class);
        final var closeFailure = new IOException("transport close failed");
        if (closeFails) {
            doThrow(closeFailure).when(transport).close();
        }
        final var entered = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        when(transport.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenAnswer(invocation -> {
            entered.countDown();
            release.await();
            return mock(BulkResponse.class);
        });
        final var client = new OpensearchClient(transport, new OpensearchSinkConnectorConfig(Map.of(
                "connection.url", "http://localhost", "batch.size", "1", "flush.timeout.ms", "50")), null);
        try {
            client.index(new IndexRequest("test"), null);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            final var failure = assertThrows(ConnectException.class, client::close);
            assertEquals(interrupted, Thread.currentThread().isInterrupted());
            if (closeFails) {
                assertArrayEquals(new Throwable[]{closeFailure}, failure.getSuppressed());
            }
            verify(transport).close();
        } finally {
            Thread.interrupted();
            release.countDown();
        }
    }
}
