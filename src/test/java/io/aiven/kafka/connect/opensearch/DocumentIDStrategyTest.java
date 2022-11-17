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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.index.VersionType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DocumentIDStrategyTest {

    private String key;
    private String topic;
    private int partition;
    private long offset;
    private String index;
    private Schema schema;
    private String value;

    @BeforeEach
    public void setUp() {
        key = "key";
        topic = "topic";
        partition = 5;
        offset = 15;
        index = "index";
        value = "value";
        schema = SchemaBuilder
                .struct()
                .name("struct")
                .field("string", Schema.STRING_SCHEMA)
                .build();
    }
    
    @Test
    public void docIdStrategyNoneWithNullRecord() {
        final DocumentIDStrategy strategy = DocumentIDStrategy.NONE;
        final SinkRecord record = createSinkRecordWithValue(null);
        final DocWriteRequest<?> req = strategy.updateIndexRequest(new IndexRequest(index), record);
        assertNull(req);
    }

    @Test
    public void docIdStrategyNone() {
        final DocumentIDStrategy strategy = DocumentIDStrategy.NONE;
        final SinkRecord record = createSinkRecordWithValue(value);
        final DocWriteRequest<?> req = strategy.updateIndexRequest(new IndexRequest(index), record);
        assertNotNull(req);
        assertNull(req.id());
        assertEquals(VersionType.INTERNAL, req.versionType());
    }

    @Test
    public void docIdStrategyRecordKey() {
        final DocumentIDStrategy strategy = DocumentIDStrategy.RECORD_KEY;
        final SinkRecord record = createSinkRecordWithValue(value);
        final DocWriteRequest<?> req = strategy.updateIndexRequest(new IndexRequest(index), record);
        assertNotNull(req);
        assertEquals(key, req.id());
        assertEquals(VersionType.EXTERNAL, req.versionType());
        assertEquals(record.kafkaOffset(), req.version());
    }

    @Test
    public void docIdStrategyRecordKeyDelete() {
        final DocumentIDStrategy strategy = DocumentIDStrategy.RECORD_KEY;
        final SinkRecord record = createSinkRecordWithValue(value);
        final DocWriteRequest<?> req = strategy.updateDeleteRequest(new DeleteRequest(index), record);
        assertNotNull(req);
        assertEquals(key, req.id());
        assertEquals(VersionType.EXTERNAL, req.versionType());
        assertEquals(record.kafkaOffset(), req.version());
    }

    @Test
    public void docIdStrategyTopicPartitionOffset() {
        final DocumentIDStrategy strategy = DocumentIDStrategy.TOPIC_PARTITION_OFFSET;
        final SinkRecord record = createSinkRecordWithValue(value);
        final DocWriteRequest<?> req = strategy.updateIndexRequest(new IndexRequest(index), record);
        assertNotNull(req);
        assertEquals(record.topic() + "+" + record.kafkaPartition() + "+" + record.kafkaOffset(), req.id());
        assertEquals(VersionType.INTERNAL, req.versionType());
    }

    public SinkRecord createSinkRecordWithValue(final Object value) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, schema, value, offset);
    }
}
