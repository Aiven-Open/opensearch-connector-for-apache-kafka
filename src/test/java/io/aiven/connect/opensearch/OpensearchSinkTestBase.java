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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.common.settings.Settings;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.test.OpenSearchIntegTestCase;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.After;
import org.junit.Before;

public class OpensearchSinkTestBase extends OpenSearchIntegTestCase {

    protected static final String TYPE = "kafka-connect";

    protected static final String TOPIC = "topic";
    protected static final int PARTITION = 12;
    protected static final int PARTITION2 = 13;
    protected static final int PARTITION3 = 14;
    protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);
    protected static final TopicPartition TOPIC_PARTITION3 = new TopicPartition(TOPIC, PARTITION3);

    protected OpensearchClient client;
    private DataConverter converter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = null;
        converter = new DataConverter(true, DataConverter.BehaviorOnNullValues.IGNORE);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (client != null) {
            client.close();
        }
        client = null;
    }

    protected int getPort() {
        assertTrue("There should be at least 1 HTTP endpoint exposed in the test cluster",
                cluster().httpAddresses().length > 0);
        return cluster().httpAddresses()[0].getPort();
    }

    protected Struct createRecord(final Schema schema) {
        final Struct struct = new Struct(schema);
        struct.put("user", "Liquan");
        struct.put("message", "trying out Elastic Search.");
        return struct;
    }

    protected Schema createSchema() {
        return SchemaBuilder.struct().name("record")
                .field("user", Schema.STRING_SCHEMA)
                .field("message", Schema.STRING_SCHEMA)
                .build();
    }

    protected Schema createOtherSchema() {
        return SchemaBuilder.struct().name("record")
                .field("user", Schema.INT32_SCHEMA)
                .build();
    }

    protected Struct createOtherRecord(final Schema schema) {
        final Struct struct = new Struct(schema);
        struct.put("user", 10);
        return struct;
    }

    protected void verifySearchResults(
            final Collection<SinkRecord> records,
            final boolean ignoreKey,
            final boolean ignoreSchema) throws IOException {
        verifySearchResults(records, TOPIC, ignoreKey, ignoreSchema);
    }

    protected void verifySearchResults(
            final Collection<?> records,
            final String index,
            final boolean ignoreKey,
            final boolean ignoreSchema) throws IOException {
        final JsonObject result = client.search("", index, null);

        final JsonArray rawHits = result.getAsJsonObject("hits").getAsJsonArray("hits");

        assertEquals(records.size(), rawHits.size());

        final Map<String, String> hits = new HashMap<>();
        for (int i = 0; i < rawHits.size(); ++i) {
            final JsonObject hitData = rawHits.get(i).getAsJsonObject();
            final String id = hitData.get("_id").getAsString();
            final String source = hitData.get("_source").getAsJsonObject().toString();
            hits.put(id, source);
        }

        for (final Object record : records) {
            if (record instanceof SinkRecord) {
                final IndexableRecord indexableRecord =
                        converter.convertRecord((SinkRecord) record, index, TYPE, ignoreKey, ignoreSchema);
                assertEquals(indexableRecord.payload, hits.get(indexableRecord.key.id));
            } else {
                assertEquals(record, hits.get("key"));
            }
        }
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {
        final int randomPort = randomIntBetween(49152, 65525);
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), randomPort)
                .put("network.host", "127.0.0.1")
                .put("transport.port", randomPort)
                .build();
    }

}
