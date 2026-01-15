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
package io.aiven.kafka.connect.opensearch;

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OpenSearchSinkTaskTest {

    @Test
    void failToStartWhenReportIsConfiguredAndErrantRecordReporterIsMissing(final @Mock SinkTaskContext context) {

        final var props = Map.of(CONNECTION_URL_CONFIG, "http://localhost", MAX_BUFFERED_RECORDS_CONFIG, "100",
                MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2", LINGER_MS_CONFIG, "1000",
                MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1", BEHAVIOR_ON_VERSION_CONFLICT_CONFIG,
                BulkProcessor.BehaviorOnMalformedDoc.REPORT.toString());
        when(context.errantRecordReporter()).thenReturn(null);
        final var task = new OpenSearchSinkTask();
        task.initialize(context);
        final Exception exception = assertThrows(ConnectException.class, () -> task.start(props));
        assertTrue(exception.getCause().getMessage().contains("Errant record reporter must be configured"));
    }

    @Test
    void failToStartWhenReportIsConfiguredAndErrantRecordReporterIsNotSupported(final @Mock SinkTaskContext context) {

        final var props = Map.of(CONNECTION_URL_CONFIG, "http://localhost", MAX_BUFFERED_RECORDS_CONFIG, "100",
                MAX_IN_FLIGHT_REQUESTS_CONFIG, "5", BATCH_SIZE_CONFIG, "2", LINGER_MS_CONFIG, "1000",
                MAX_RETRIES_CONFIG, "3", READ_TIMEOUT_MS_CONFIG, "1", BEHAVIOR_ON_VERSION_CONFLICT_CONFIG,
                BulkProcessor.BehaviorOnMalformedDoc.REPORT.toString());
        when(context.errantRecordReporter()).thenThrow(new NoSuchMethodError());
        final var task = new OpenSearchSinkTask();
        task.initialize(context);
        final Exception exception = assertThrows(ConnectException.class, () -> task.start(props));
        assertTrue(exception.getCause().getMessage().contains("Errant record reporter must be configured"));
    }
}
