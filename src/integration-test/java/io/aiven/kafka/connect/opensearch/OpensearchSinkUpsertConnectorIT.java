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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class OpensearchSinkUpsertConnectorIT extends AbstractKafkaConnectIT {

    final ObjectMapper objectMapper = new ObjectMapper();

    static final String CONNECTOR_NAME = "os-sink-connector";

    static final String TOPIC_NAME = "os-upsert-topic";

    public OpensearchSinkUpsertConnectorIT() {
        super(TOPIC_NAME, CONNECTOR_NAME);
    }

    @Test
    public void testConnector() throws Exception {
        final var props = connectorProperties(TOPIC_NAME);
        props.put(OpensearchSinkConnectorConfig.INDEX_WRITE_METHOD,
                IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT));
        props.put(OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "false");
        connect.configureConnector(CONNECTOR_NAME, props);
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        writeRecords(3, TOPIC_NAME);

        waitForRecords(TOPIC_NAME, 3);

        final var messages = new ArrayList<Pair<String, Map<String, Object>>>(3);
        for (final var hit : search(TOPIC_NAME)) {
            final var d = hit.getSourceAsMap();
            messages.add(Pair.of(hit.getId(), hit.getSourceAsMap()));
        }

        for (var i = 0; i < messages.size(); i++) {
            final var m = messages.get(i);
            m.getRight().put("another_key", "another_value_" + i);
            connect.kafka().produce(TOPIC_NAME, m.getLeft(), objectMapper.writeValueAsString(m.getRight()));
        }

        connect.kafka().produce(TOPIC_NAME, String.valueOf(11), String.format("{\"doc_num\":%d}", 11));
        connect.kafka().produce(TOPIC_NAME, String.valueOf(12), String.format("{\"doc_num\":%d}", 12));

        waitForRecords(TOPIC_NAME, 5);

        final var foundDocs = new HashMap<Integer, Map<String, Object>>();

        for (final var hit : search(TOPIC_NAME)) {
            final var id = Integer.valueOf(hit.getId());
            foundDocs.put(id, hit.getSourceAsMap());
        }

        assertIterableEquals(List.of(0, 1, 2, 11, 12),
                foundDocs.keySet().stream().sorted().collect(Collectors.toList()));

        for (var i = 0; i < 3; i++) {
            assertEquals(i, foundDocs.get(i).get("doc_num"));
            assertEquals("another_value_" + i, foundDocs.get(i).get("another_key"));
        }

        assertEquals(11, foundDocs.get(11).get("doc_num"));
        assertFalse(foundDocs.get(11).containsKey("another_key"));
        assertEquals(12, foundDocs.get(12).get("doc_num"));
        assertFalse(foundDocs.get(12).containsKey("another_key"));
    }
}
