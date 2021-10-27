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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.test.InternalTestCluster;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.Test;


@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class OpensearchSinkTaskTest extends OpensearchSinkTestBase {

    private static final String TOPIC_IN_CAPS = "AnotherTopicInCaps";
    private static final int PARTITION_113 = 113;
    private static final TopicPartition TOPIC_IN_CAPS_PARTITION = new TopicPartition(TOPIC_IN_CAPS, PARTITION_113);

    private Map<String, String> createProps() {
        final Map<String, String> props = new HashMap<>();
        props.put(OpensearchSinkConnectorConfig.TYPE_NAME_CONFIG, TYPE);
        props.put(OpensearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "localhost");
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
        return props;
    }

    @Test
    public void testPutAndFlush() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        cluster.ensureAtLeastNumDataNodes(3);
        final Map<String, String> props = createProps();

        final OpensearchSinkTask task = new OpensearchSinkTask();
        task.start(props, client);
        task.open(new HashSet<>(Arrays.asList(TOPIC_PARTITION, TOPIC_PARTITION2, TOPIC_PARTITION3)));

        final String key = "key";
        final Schema schema = createSchema();
        final Struct record = createRecord(schema);

        final Collection<SinkRecord> records = new ArrayList<>();

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 0);
        records.add(sinkRecord);

        sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, 1);
        records.add(sinkRecord);

        task.put(records);
        task.flush(null);

        refresh();

        verifySearchResults(records, true, false);
    }

    @Test
    public void testCreateAndWriteToIndexForTopicWithUppercaseCharacters() {
        // We should as well test that writing a record with a previously un seen record will create
        // an index following the required elasticsearch requirements of lowercasing.
        final InternalTestCluster cluster = internalCluster();
        cluster.ensureAtLeastNumDataNodes(3);
        final Map<String, String> props = createProps();

        final OpensearchSinkTask task = new OpensearchSinkTask();

        final String key = "key";
        final Schema schema = createSchema();
        final Struct record = createRecord(schema);

        final SinkRecord sinkRecord = new SinkRecord(TOPIC_IN_CAPS,
            PARTITION_113,
            Schema.STRING_SCHEMA,
            key,
            schema,
            record,
            0);

        try {
            task.start(props, client);
            task.open(new HashSet<>(Collections.singletonList(TOPIC_IN_CAPS_PARTITION)));
            task.put(Collections.singleton(sinkRecord));
        } catch (final Exception ex) {
            fail("A topic name not in lowercase can not be used as index name in Elasticsearch");
        } finally {
            task.stop();
        }
    }
}
