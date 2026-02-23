/*
 * Copyright 2021 Aiven Oy
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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.KEY_IGNORE_ID_STRATEGY_CONFIG;
import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG;
import static java.util.Objects.isNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.test.TestUtils;

import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class OpenSearchSinkTaskIT extends AbstractIT {

    private static final int PARTITION_1 = 12;

    public static final String TOPIC_NAME = "os-data-test";

    private final static SearchRequest DEFAULT_SEARCH_REQUEST = SearchRequest.of(b -> b.index(TOPIC_NAME));

    @AfterEach
    void tearDown() throws Exception {
        if (opensearchClient.indices().exists(ExistsRequest.builder().index(TOPIC_NAME).build()).value()) {
            opensearchClient.indices().delete(DeleteIndexRequest.builder().index(TOPIC_NAME).build());
            TestUtils.waitForCondition(
                    () -> !opensearchClient.indices().exists(ExistsRequest.builder().index(TOPIC_NAME).build()).value(),
                    TimeUnit.MINUTES.toMillis(1), "Index has not been deleted yet.");
        }
    }

    @Test
    public void testBytes() throws Exception {

        final Schema structSchema = SchemaBuilder.struct()
                .name("struct")
                .field("bytes", SchemaBuilder.BYTES_SCHEMA)
                .build();

        final Struct struct = new Struct(structSchema);
        struct.put("bytes", new byte[] { 42 });

        runTask(getDefaultTaskProperties(true, BehaviorOnNullValues.DEFAULT),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", structSchema, struct, 0)));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, 1);

        final var searchResults = opensearchClient.search(DEFAULT_SEARCH_REQUEST, Map.class).hits();
        for (final var hit : searchResults.hits()) {
            assertEquals(Base64.getEncoder().encodeToString(new byte[] { 42 }), hit.source().get("bytes"));
        }
    }

    @Test
    public void testDecimal() throws Exception {
        final int scale = 2;
        final byte[] bytes = ByteBuffer.allocate(4).putInt(2).array();
        final var decimal = new BigDecimal(new BigInteger(bytes), scale);

        final Schema structSchema = SchemaBuilder.struct()
                .name("struct")
                .field("decimal", Decimal.schema(scale))
                .build();

        final Struct struct = new Struct(structSchema);
        struct.put("decimal", decimal);
        runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.DEFAULT),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", structSchema, struct, 0)));
        final var searchResults = opensearchClient.search(DEFAULT_SEARCH_REQUEST, Map.class).hits();
        for (final var hit : searchResults.hits()) {
            assertEquals(0.02d, hit.source().get("decimal"));
        }
        assertIndexAndMapping();
    }

    @Test
    public void testCompatible() throws Exception {
        final var schema = createSchema();
        final var record = createRecord(schema);
        final var otherSchema = createOtherSchema();
        final var otherRecord = createOtherRecord(otherSchema);

        final var opensearchSinkTask = new OpenSearchSinkTask();
        try {
            final var mockContext = mock(SinkTaskContext.class);
            opensearchSinkTask.initialize(mockContext);
            opensearchSinkTask.start(getDefaultTaskProperties(true, BehaviorOnNullValues.DEFAULT));
            opensearchSinkTask.put(
                    List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", schema, record, 0),
                            new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", schema, record, 1)));
            assertTrue(opensearchClient.indices().exists(ExistsRequest.builder().index(TOPIC_NAME).build()).value());
            opensearchSinkTask.flush(null);
            waitForRecords(TOPIC_NAME, 2);
            opensearchSinkTask.put(List.of(
                    new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", otherSchema, otherRecord, 2),
                    new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", otherSchema, otherRecord, 3)));
            opensearchSinkTask.flush(null);
            waitForRecords(TOPIC_NAME, 4);
        } finally {
            opensearchSinkTask.stop();
        }
    }

    @Test
    @Disabled("We need to verify do we need such functionality or not")
    public void testIncompatible() throws Exception {
        final var schema = createSchema();
        final var record = createRecord(schema);
        final var otherSchema = createOtherSchema();
        final var otherRecord = createOtherRecord(otherSchema);
        assertThrows(ConnectException.class,
                () -> runTask(getDefaultTaskProperties(true, BehaviorOnNullValues.DEFAULT), List.of(
                        new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", otherSchema, otherRecord,
                                0),
                        new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", schema, record, 1))));
    }

    @Test
    public void testDeleteOnNullValue() throws Exception {
        final var key1 = "key1";
        final var key2 = "key2";

        final var schema = createSchema();
        final var record = createRecord(schema);

        // First, write a couple of actual (non-null-valued) records
        runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.DELETE),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, key1, schema, record, 0),
                        new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, key2, schema, record, 1)));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, 2);
        // Then, write a record with the same key as the first inserted record but a null value
        runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.DELETE),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, key1, schema, null, 2)));
        waitForRecords(TOPIC_NAME, 1);
    }

    @Test
    public void testDeleteWithNullKey() throws Exception {
        runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.DELETE),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, null, createSchema(), null, 0)));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, 0);
    }

    @Test
    public void testFailOnNullValue() throws Exception {
        assertThrows(ConnectException.class,
                () -> runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.FAIL),
                        List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", createSchema(),
                                null, 0))));
    }

    @Test
    public void testIgnoreNullValue() throws Exception {
        runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.IGNORE),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", createSchema(), null, 0)));
        waitForRecords(TOPIC_NAME, 0);
    }

    @Test
    public void testMap() throws Exception {
        final var structSchema = SchemaBuilder.struct()
                .name("struct")
                .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
                .build();

        final var map = Map.of(1, "One", 2, "Two");

        final var struct = new Struct(structSchema);
        struct.put("map", map);

        runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.DEFAULT),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", structSchema, struct, 0)));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, 1);
    }

    @Test
    public void testStringKeyedMap() throws Exception {
        final var mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

        final Map<String, Integer> map = new HashMap<>();
        map.put("One", 1);
        map.put("Two", 2);

        runTask(getDefaultTaskProperties(false, BehaviorOnNullValues.DEFAULT),
                List.of(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", mapSchema, map, 0)));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, 1);
    }

    @Test
    public void testWriterIgnoreKey() throws Exception {
        runTask(getDefaultTaskProperties(true, BehaviorOnNullValues.DEFAULT), prepareData(2));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, 2);
    }

    @Test
    public void testWriterIgnoreSchema() throws Exception {
        final var props = getDefaultTaskProperties(true, BehaviorOnNullValues.DEFAULT);
        props.put(SCHEMA_IGNORE_CONFIG, "true");
        runTask(getDefaultTaskProperties(true, BehaviorOnNullValues.DEFAULT), prepareData(2));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, 2);
    }

    @Test
    public void testKeyIgnoreStrategy() throws Exception {
        final int numRecords = 5;
        final var props = getDefaultTaskProperties(true, BehaviorOnNullValues.DEFAULT);
        props.put(KEY_IGNORE_ID_STRATEGY_CONFIG, "none");
        runTask(props, prepareData(numRecords));
        assertIndexAndMapping();
        waitForRecords(TOPIC_NAME, numRecords);
    }

    private List<SinkRecord> prepareData(final int numRecords) {
        final List<SinkRecord> records = new ArrayList<>();
        final var schema = createSchema();
        final var record = createRecord(schema);
        for (int i = 0; i < numRecords; ++i) {
            records.add(new SinkRecord(TOPIC_NAME, PARTITION_1, Schema.STRING_SCHEMA, "key", schema, record, i));
        }
        return records;
    }

    Map<String, String> getDefaultTaskProperties(final boolean ignoreKey,
            final BehaviorOnNullValues behaviorOnNullValues) {
        final var props = new HashMap<>(getDefaultProperties());
        props.put(BEHAVIOR_ON_NULL_VALUES_CONFIG, behaviorOnNullValues.name());
        props.put(DROP_INVALID_MESSAGE_CONFIG, "false");
        props.put(KEY_IGNORE_CONFIG, Boolean.toString(ignoreKey));
        return props;
    }

    protected Struct createRecord(final Schema schema) {
        return new Struct(schema).put("user", "John Doe").put("message", "blah-blah-blah-blah");
    }

    protected Schema createSchema() {
        return SchemaBuilder.struct()
                .name("record")
                .field("user", Schema.STRING_SCHEMA)
                .field("message", Schema.STRING_SCHEMA)
                .build();
    }

    protected Schema createOtherSchema() {
        return SchemaBuilder.struct().name("record").field("user", Schema.INT32_SCHEMA).build();
    }

    protected Struct createOtherRecord(final Schema schema) {
        return new Struct(schema).put("user", 10);
    }

    private void runTask(final Map<String, String> props, final List<SinkRecord> records) {
        final var opensearchSinkTask = new OpenSearchSinkTask();
        final var mockContext = mock(SinkTaskContext.class);
        opensearchSinkTask.initialize(mockContext);
        try {
            opensearchSinkTask.start(props);
            opensearchSinkTask.put(records);
            opensearchSinkTask.flush(null);
        } finally {
            opensearchSinkTask.stop();
            reset(mockContext);
        }
    }

    void assertIndexAndMapping() throws IOException {
        assertTrue(opensearchClient.indices().exists(ExistsRequest.builder().index(TOPIC_NAME).build()).value());
        assertTrue(isNull(opensearchClient.indices()
                .getMapping(b -> b.index(List.of(TOPIC_NAME)))
                .get(TOPIC_NAME)
                .mappings()
                .dynamic()));
    }

}
