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

package io.aiven.connect.opensearch;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class OpensearchWriterTest extends OpensearchSinkTestBase {

    private final String key = "key";
    private final Schema schema = createSchema();
    private final Struct record = createRecord(schema);
    private final Schema otherSchema = createOtherSchema();
    private final Struct otherRecord = createOtherRecord(otherSchema);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private boolean ignoreKey;
    private boolean ignoreSchema;

    @Before
    public void setUp() throws Exception {
        ignoreKey = false;
        ignoreSchema = false;

        super.setUp();
    }

    @Test
    public void testWriter() throws Exception {
        final Collection<SinkRecord> records = prepareData(2);
        final OpensearchWriter writer = initWriter(client);
        writeDataAndRefresh(writer, records);

        final Collection<SinkRecord> expected = Collections.singletonList(
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1)
        );
        verifySearchResults(expected);
    }

    @Test
    public void testWriterIgnoreKey() throws Exception {
        ignoreKey = true;

        final Collection<SinkRecord> records = prepareData(2);
        final OpensearchWriter writer = initWriter(client);
        writeDataAndRefresh(writer, records);
        verifySearchResults(records);
    }

    @Test
    public void testWriterIgnoreSchema() throws Exception {
        ignoreKey = true;
        ignoreSchema = true;

        final Collection<SinkRecord> records = prepareData(2);
        final OpensearchWriter writer = initWriter(client);
        writeDataAndRefresh(writer, records);
        verifySearchResults(records);
    }

    @Test
    public void testTopicIndexOverride() throws Exception {
        ignoreKey = true;
        ignoreSchema = true;

        final String indexOverride = "index";

        final Collection<SinkRecord> records = prepareData(2);
        final OpensearchWriter writer = initWriter(
            client,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.singletonMap(TOPIC, indexOverride),
            false,
            DataConverter.BehaviorOnNullValues.IGNORE);
        writeDataAndRefresh(writer, records);
        verifySearchResults(records, indexOverride);
    }

    @Test(expected = ConnectException.class)
    public void testIncompatible() throws Exception {
        ignoreKey = true;

        final Collection<SinkRecord> records = new ArrayList<>();
        SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client);

        writer.write(records);
        Thread.sleep(5000);
        records.clear();

        sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
        records.add(sinkRecord);
        writer.write(records);

        writer.flush();
        fail("should fail because of mapper_parsing_exception");
    }

    @Test
    public void testCompatible() throws Exception {
        ignoreKey = true;

        final Collection<SinkRecord> records = new ArrayList<>();
        final Collection<SinkRecord> expected = new ArrayList<>();

        SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
        records.add(sinkRecord);
        expected.add(sinkRecord);
        sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
        records.add(sinkRecord);
        expected.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client);

        writer.write(records);
        records.clear();

        sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 2);
        records.add(sinkRecord);
        expected.add(sinkRecord);

        sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, otherSchema, otherRecord, 3);
        records.add(sinkRecord);
        expected.add(sinkRecord);

        writeDataAndRefresh(writer, records);
        verifySearchResults(expected);
    }

    @Test
    public void testSafeRedeliveryRegularKey() throws Exception {
        final Struct value0 = new Struct(schema);
        value0.put("user", "foo");
        value0.put("message", "hi");
        final SinkRecord sinkRecord0 =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value0, 0);

        final Struct value1 = new Struct(schema);
        value1.put("user", "foo");
        value1.put("message", "bye");
        final SinkRecord sinkRecord1 =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value1, 1);

        final OpensearchWriter writer = initWriter(client);
        writer.write(Arrays.asList(sinkRecord0, sinkRecord1));
        writer.flush();

        // write the record with earlier offset again
        writeDataAndRefresh(writer, Collections.singleton(sinkRecord0));

        // last write should have been ignored due to version conflict
        verifySearchResults(Collections.singleton(sinkRecord1));
    }

    @Test
    public void testSafeRedeliveryOffsetInKey() throws Exception {
        ignoreKey = true;

        final Struct value0 = new Struct(schema);
        value0.put("user", "foo");
        value0.put("message", "hi");
        final SinkRecord sinkRecord0 =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value0, 0);

        final Struct value1 = new Struct(schema);
        value1.put("user", "foo");
        value1.put("message", "bye");
        final SinkRecord sinkRecord1 =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, value1, 1);

        final List<SinkRecord> records = Arrays.asList(sinkRecord0, sinkRecord1);

        final OpensearchWriter writer = initWriter(client);
        writer.write(records);
        writer.flush();

        // write them again
        writeDataAndRefresh(writer, records);

        // last write should have been ignored due to version conflict
        verifySearchResults(records);
    }

    @Test
    public void testMap() throws Exception {
        final Schema structSchema = SchemaBuilder.struct().name("struct")
            .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
            .build();

        final Map<Integer, String> map = new HashMap<>();
        map.put(1, "One");
        map.put(2, "Two");

        final Struct struct = new Struct(structSchema);
        struct.put("map", map);

        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client);
        writeDataAndRefresh(writer, records);
        verifySearchResults(records);
    }

    @Test
    public void testStringKeyedMap() throws Exception {
        final Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

        final Map<String, Integer> map = new HashMap<>();
        map.put("One", 1);
        map.put("Two", 2);

        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, mapSchema, map, 0);

        final OpensearchWriter writer = initWriter(client);
        writeDataAndRefresh(writer, Collections.singletonList(sinkRecord));

        final Collection<?> expectedRecords =
            Collections.singletonList(new ObjectMapper().writeValueAsString(map));
        verifySearchResults(expectedRecords, TOPIC);
    }

    @Test
    public void testDecimal() throws Exception {
        final int scale = 2;
        final byte[] bytes = ByteBuffer.allocate(4).putInt(2).array();
        final BigDecimal decimal = new BigDecimal(new BigInteger(bytes), scale);

        final Schema structSchema = SchemaBuilder.struct().name("struct")
            .field("decimal", Decimal.schema(scale))
            .build();

        final Struct struct = new Struct(structSchema);
        struct.put("decimal", decimal);

        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client);
        writeDataAndRefresh(writer, records);
        verifySearchResults(records);
    }

    @Test
    public void testBytes() throws Exception {
        final Schema structSchema = SchemaBuilder.struct().name("struct")
            .field("bytes", SchemaBuilder.BYTES_SCHEMA)
            .build();

        final Struct struct = new Struct(structSchema);
        struct.put("bytes", new byte[]{42});

        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client);
        writeDataAndRefresh(writer, records);
        verifySearchResults(records);
    }

    @Test
    public void testIgnoreNullValue() throws Exception {
        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, null, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client, DataConverter.BehaviorOnNullValues.IGNORE);
        writeDataAndRefresh(writer, records);
        // Send an empty list of records to the verify method, since the empty record should have been
        // skipped
        verifySearchResults(new ArrayList<SinkRecord>());
    }

    @Test
    public void testDeleteOnNullValue() throws Exception {
        final String key1 = "key1";
        final String key2 = "key2";

        final OpensearchWriter writer = initWriter(client, DataConverter.BehaviorOnNullValues.DELETE);

        final Collection<SinkRecord> records = new ArrayList<>();

        // First, write a couple of actual (non-null-valued) records
        final SinkRecord insertRecord1 =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key1, schema, record, 0);
        records.add(insertRecord1);
        final SinkRecord insertRecord2 =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key2, otherSchema, otherRecord, 1);
        records.add(insertRecord2);
        // Can't call writeDataAndRefresh(writer, records) since it stops the writer
        writer.write(records);
        writer.flush();
        refresh();
        // Make sure the record made it there successfully
        verifySearchResults(records);

        // Then, write a record with the same key as the first inserted record but a null value
        final SinkRecord deleteRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key1, schema, null, 2);

        // Don't want to resend the first couple of records
        records.clear();
        records.add(deleteRecord);
        writeDataAndRefresh(writer, records);

        // The only remaining record should be the second inserted record
        records.clear();
        records.add(insertRecord2);
        verifySearchResults(records);
    }

    @Test
    public void testIneffectiveDelete() throws Exception {
        // Just a sanity check to make sure things don't blow up if an attempt is made to delete a
        // record that doesn't exist in the first place

        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, null, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client, DataConverter.BehaviorOnNullValues.DELETE);
        writeDataAndRefresh(writer, records);
        verifySearchResults(new ArrayList<SinkRecord>());
    }

    @Test
    public void testDeleteWithNullKey() throws Exception {
        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, null, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client, DataConverter.BehaviorOnNullValues.DELETE);
        writeDataAndRefresh(writer, records);
        verifySearchResults(new ArrayList<SinkRecord>());
    }

    @Test
    public void testFailOnNullValue() throws Exception {
        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, null, 0);
        records.add(sinkRecord);

        final OpensearchWriter writer = initWriter(client, DataConverter.BehaviorOnNullValues.FAIL);
        try {
            writeDataAndRefresh(writer, records);
            fail("should fail because of behavior.on.null.values=fail");
        } catch (final DataException e) {
            // expected
        }
    }

    @Test
    public void testInvalidRecordException() throws Exception {
        ignoreSchema = true;

        final Collection<SinkRecord> records = new ArrayList<>();

        final SinkRecord sinkRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, null, new byte[]{42}, 0);
        records.add(sinkRecord);

        final OpensearchWriter strictWriter = initWriter(client);

        thrown.expect(ConnectException.class);
        thrown.expectMessage("Key is used as document id and can not be null");
        strictWriter.write(records);
    }

    @Test
    public void testDropInvalidRecord() throws Exception {
        ignoreSchema = true;
        final Collection<SinkRecord> inputRecords = new ArrayList<>();
        final Collection<SinkRecord> outputRecords = new ArrayList<>();

        final Schema structSchema = SchemaBuilder.struct().name("struct")
            .field("bytes", SchemaBuilder.BYTES_SCHEMA)
            .build();

        final Struct struct = new Struct(structSchema);
        struct.put("bytes", new byte[]{42});


        final SinkRecord invalidRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, structSchema, struct, 0);
        final SinkRecord validRecord =
            new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, structSchema, struct, 1);

        inputRecords.add(validRecord);
        inputRecords.add(invalidRecord);

        outputRecords.add(validRecord);

        final OpensearchWriter nonStrictWriter = initWriter(client, true);

        writeDataAndRefresh(nonStrictWriter, inputRecords);
        verifySearchResults(outputRecords, ignoreKey, ignoreSchema);
    }

    private Collection<SinkRecord> prepareData(final int numRecords) {
        final Collection<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            final SinkRecord sinkRecord =
                new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, i);
            records.add(sinkRecord);
        }
        return records;
    }

    private OpensearchWriter initWriter(final OpensearchClient client) {
        return initWriter(client, false, DataConverter.BehaviorOnNullValues.IGNORE);
    }

    private OpensearchWriter initWriter(final OpensearchClient client, final boolean dropInvalidMessage) {
        return initWriter(client, dropInvalidMessage, DataConverter.BehaviorOnNullValues.IGNORE);
    }

    private OpensearchWriter initWriter(
        final OpensearchClient client,
        final DataConverter.BehaviorOnNullValues behavior) {
        return initWriter(client, false, behavior);
    }

    private OpensearchWriter initWriter(
        final OpensearchClient client,
        final boolean dropInvalidMessage,
        final DataConverter.BehaviorOnNullValues behavior) {
        return initWriter(
            client,
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptyMap(),
            dropInvalidMessage,
            behavior
        );
    }

    private OpensearchWriter initWriter(
        final OpensearchClient client,
        final Set<String> ignoreKeyTopics,
        final Set<String> ignoreSchemaTopics,
        final Map<String, String> topicToIndexMap,
        final boolean dropInvalidMessage,
        final DataConverter.BehaviorOnNullValues behavior
    ) {
        final OpensearchWriter writer = new OpensearchWriter.Builder(client)
            .setType(TYPE)
            .setIgnoreKey(ignoreKey, ignoreKeyTopics)
            .setIgnoreSchema(ignoreSchema, ignoreSchemaTopics)
            .setTopicToIndexMap(topicToIndexMap)
            .setFlushTimoutMs(10000)
            .setMaxBufferedRecords(10000)
            .setMaxInFlightRequests(1)
            .setBatchSize(2)
            .setLingerMs(1000)
            .setRetryBackoffMs(1000)
            .setMaxRetry(3)
            .setDropInvalidMessage(dropInvalidMessage)
            .setBehaviorOnNullValues(behavior)
            .build();
        writer.start();
        writer.createIndicesForTopics(Collections.singleton(TOPIC));
        return writer;
    }

    private void writeDataAndRefresh(final OpensearchWriter writer, final Collection<SinkRecord> records)
        throws Exception {
        writer.write(records);
        writer.flush();
        writer.stop();
        refresh();
    }

    private void verifySearchResults(final Collection<SinkRecord> records) throws Exception {
        verifySearchResults(records, ignoreKey, ignoreSchema);
    }

    private void verifySearchResults(final Collection<?> records, final String index) throws Exception {
        verifySearchResults(records, index, ignoreKey, ignoreSchema);
    }
}
