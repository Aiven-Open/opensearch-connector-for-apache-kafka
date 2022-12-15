/*
 * Copyright 2019 Aiven Oy
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.action.delete.DeleteRequest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestBuilderTest {

    private String key;
    private String topic;
    private int partition;
    private long offset;
    private String index;
    private Schema schema;

    RequestBuilder requestBuilder;

    @BeforeEach
    public void setUp() {
        requestBuilder = createPayloadBuilder(true, BehaviorOnNullValues.DEFAULT);
        key = "key";
        topic = "topic";
        partition = 0;
        offset = 0;
        index = "index";
        schema = SchemaBuilder
                .struct()
                .name("struct")
                .field("string", Schema.STRING_SCHEMA)
                .build();
    }

    @Test
    public void ignoreOnNullValue() {
        requestBuilder = createPayloadBuilder(true, BehaviorOnNullValues.IGNORE);
        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        assertNull(requestBuilder.build(index, sinkRecord));
    }

    @Test
    public void deleteOnNullValue() {
        requestBuilder = createPayloadBuilder(true, BehaviorOnNullValues.DELETE);

        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        final var deleteRecord = requestBuilder.build(index, sinkRecord);

        assertTrue(deleteRecord instanceof DeleteRequest);
        assertEquals(key, deleteRecord.id());
        assertEquals(index, deleteRecord.index());
    }

    @Test
    public void ignoreDeleteOnNullValueWithNullKey() {
        requestBuilder = createPayloadBuilder(true, BehaviorOnNullValues.DELETE);
        key = null;

        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        assertNull(requestBuilder.build(index, sinkRecord));
    }

    @Test
    public void failOnNullValue() {
        requestBuilder = createPayloadBuilder(true, BehaviorOnNullValues.FAIL);

        final SinkRecord sinkRecord = createSinkRecordWithValue(null);
        assertThrows(
                DataException.class,
                () -> requestBuilder.build(index, sinkRecord));
    }

    RequestBuilder createPayloadBuilder(final boolean useCompactMapEntries,
                                        final BehaviorOnNullValues behaviorOnNullValues) {
        final var props = Map.of(
                OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost",
                OpensearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
                behaviorOnNullValues.toString(),
                OpensearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG, Boolean.toString(useCompactMapEntries)
        );
        return RequestBuilder.create(new OpensearchSinkConnectorConfig(props));
    }

    public SinkRecord createSinkRecordWithValue(final Object value) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, schema, value, offset);
    }

}
