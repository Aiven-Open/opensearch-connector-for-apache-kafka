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

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.opensearch.action.DocWriteRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.EXISTING_RESOURCE_TYPE;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.TOPIC_TO_EXISTING_RESOURCE_MAPPING;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OpensearchSinkTaskTest {

    @Test
    void failToStartWhenReportIsConfiguredAndErrantRecordReporterIsMissing(final @Mock SinkTaskContext context) {

        final var props = Map.of(
                CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100",
                MAX_IN_FLIGHT_REQUESTS_CONFIG, "5",
                BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "1000",
                MAX_RETRIES_CONFIG, "3",
                READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, BulkProcessor.BehaviorOnMalformedDoc.REPORT.toString()
        );
        when(context.errantRecordReporter()).thenReturn(null);
        final var task = new OpensearchSinkTask();
        task.initialize(context);
        final Exception exception = assertThrows(ConnectException.class, () -> task.start(props));
        assertTrue(exception.getCause().getMessage().contains("Errant record reporter must be configured"));
    }

    @Test
    void failToStartWhenReportIsConfiguredAndErrantRecordReporterIsNotSupported(final @Mock SinkTaskContext context) {

        final var props = Map.of(
                CONNECTION_URL_CONFIG, "http://localhost",
                MAX_BUFFERED_RECORDS_CONFIG, "100",
                MAX_IN_FLIGHT_REQUESTS_CONFIG, "5",
                BATCH_SIZE_CONFIG, "2",
                LINGER_MS_CONFIG, "1000",
                MAX_RETRIES_CONFIG, "3",
                READ_TIMEOUT_MS_CONFIG, "1",
                BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, BulkProcessor.BehaviorOnMalformedDoc.REPORT.toString()
        );
        when(context.errantRecordReporter()).thenThrow(new NoSuchMethodError());
        final var task = new OpensearchSinkTask();
        task.initialize(context);
        final Exception exception = assertThrows(ConnectException.class, () -> task.start(props));
        assertTrue(exception.getCause().getMessage().contains("Errant record reporter must be configured"));
    }

    @Test
    void existingResourceRoutesRecordToMappedIndexAndPushesMapping() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(
                CONNECTION_URL_CONFIG, "http://localhost",
                EXISTING_RESOURCE_TYPE, ExistingResourceType.INDEX_ALIAS.toString(),
                TOPIC_TO_EXISTING_RESOURCE_MAPPING, "src-topic:dest-alias"
        ));
        final var client = mock(OpensearchClient.class);
        when(client.hasMapping("dest-alias")).thenReturn(false);
        final var converter = mock(RecordConverter.class);
        final var writeRequest = mock(DocWriteRequest.class);
        final var record = new SinkRecord("src-topic", 0, null, null, Schema.STRING_SCHEMA, "{}", 0L);
        when(converter.convert(record, "dest-alias")).thenReturn(writeRequest);

        final var task = new OpensearchSinkTask();
        injectTaskState(task, config, client, converter);

        task.put(List.of(record));

        verify(client).hasMapping("dest-alias");
        verify(client).createMapping(eq("dest-alias"), any(Schema.class));
        verify(client).index(eq(writeRequest), eq(record));
    }

    @Test
    void existingResourceSkipsMappingWhenSchemaIgnored() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(
                CONNECTION_URL_CONFIG, "http://localhost",
                EXISTING_RESOURCE_TYPE, ExistingResourceType.INDEX_ALIAS.toString(),
                TOPIC_TO_EXISTING_RESOURCE_MAPPING, "src-topic:dest-alias",
                OpensearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG, "true"
        ));
        final var client = mock(OpensearchClient.class);
        final var converter = mock(RecordConverter.class);
        final var writeRequest = mock(DocWriteRequest.class);
        final var record = new SinkRecord("src-topic", 0, null, null, Schema.STRING_SCHEMA, "{}", 0L);
        when(converter.convert(record, "dest-alias")).thenReturn(writeRequest);

        final var task = new OpensearchSinkTask();
        injectTaskState(task, config, client, converter);

        task.put(List.of(record));

        verify(client, never()).hasMapping(any());
        verify(client, never()).createMapping(any(), any());
        verify(client, never()).createIndex(any());
        verify(client, never()).indexOrDataStreamExists(any());
        verify(client).index(eq(writeRequest), eq(record));
    }

    @Test
    void existingResourceThrowsWhenTopicNotMapped() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(
                CONNECTION_URL_CONFIG, "http://localhost",
                EXISTING_RESOURCE_TYPE, ExistingResourceType.INDEX_ALIAS.toString(),
                TOPIC_TO_EXISTING_RESOURCE_MAPPING, "mapped-topic:dest-alias"
        ));
        final var client = mock(OpensearchClient.class);
        final var converter = mock(RecordConverter.class);
        final var record = new SinkRecord("other-topic", 0, null, null, Schema.STRING_SCHEMA, "{}", 0L);

        final var task = new OpensearchSinkTask();
        injectTaskState(task, config, client, converter);

        final ConnectException ex = assertThrows(ConnectException.class, () -> task.put(List.of(record)));
        assertTrue(ex.getMessage().contains("`other-topic` is not mapped to resource"),
                "Unexpected exception message: " + ex.getMessage());
        verify(client, never()).index(any(), any());
    }

    @Test
    void existingResourceCachesMappingPerIndex() throws Exception {
        final var config = new OpensearchSinkConnectorConfig(Map.of(
                CONNECTION_URL_CONFIG, "http://localhost",
                EXISTING_RESOURCE_TYPE, ExistingResourceType.INDEX_ALIAS.toString(),
                TOPIC_TO_EXISTING_RESOURCE_MAPPING, "src-topic:dest-alias"
        ));
        final var client = mock(OpensearchClient.class);
        when(client.hasMapping("dest-alias")).thenReturn(false);
        final var converter = mock(RecordConverter.class);
        final var writeRequest = mock(DocWriteRequest.class);
        final var record1 = new SinkRecord("src-topic", 0, null, null, Schema.STRING_SCHEMA, "{}", 0L);
        final var record2 = new SinkRecord("src-topic", 0, null, null, Schema.STRING_SCHEMA, "{}", 1L);
        when(converter.convert(any(SinkRecord.class), eq("dest-alias"))).thenReturn(writeRequest);

        final var task = new OpensearchSinkTask();
        injectTaskState(task, config, client, converter);

        task.put(List.of(record1, record2));

        verify(client).createMapping(eq("dest-alias"), any(Schema.class));
        verify(client).index(eq(writeRequest), eq(record1));
        verify(client).index(eq(writeRequest), eq(record2));
    }

    private static void injectTaskState(final OpensearchSinkTask task,
                                        final OpensearchSinkConnectorConfig config,
                                        final OpensearchClient client,
                                        final RecordConverter converter) throws Exception {
        setField(task, "config", config);
        setField(task, "client", client);
        setField(task, "recordConverter", converter);
        setField(task, "topicToIndexConverter", config.topicToIndexNameConverter());
    }

    private static void setField(final Object target, final String name, final Object value) throws Exception {
        final Field field = OpensearchSinkTask.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
