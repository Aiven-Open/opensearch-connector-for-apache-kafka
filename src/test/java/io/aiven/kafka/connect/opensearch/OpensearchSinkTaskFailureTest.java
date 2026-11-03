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

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class OpensearchSinkTaskFailureTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void putPropagatesBackgroundFailureEvenWithoutWritableRecords(final boolean filtered) throws Exception {
        final var failure = new OutOfMemoryError("bulk allocation failed");
        try (final var clients = mockConstruction(RestHighLevelClient.class, (client, context) ->
                when(client.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenThrow(failure))) {
            final var task = new OpensearchSinkTask();
            task.initialize(mock(SinkTaskContext.class));
            task.start(Map.of(
                    "connection.url", "http://localhost", "batch.size", "1", "max.retries", "0",
                    "existing.resource.type", "index", "topic.to.existing.resource.mapping", "test:test",
                    "schema.ignore", "true", "behavior.on.null.values", "ignore", "flush.timeout.ms", "1000"));
            try {
                task.put(List.of(new SinkRecord("test", 0, Schema.STRING_SCHEMA, "key",
                        Schema.STRING_SCHEMA, "{}", 0L)));
                assertSame(failure, assertThrows(ConnectException.class, () -> task.flush(Map.of())).getCause());
                final List<SinkRecord> records = filtered
                        ? List.of(new SinkRecord("test", 0, null, null, null, null, 1L)) : List.of();
                assertSame(failure, assertThrows(ConnectException.class, () -> task.put(records)).getCause());
            } finally {
                task.stop();
            }
        }
    }
}
