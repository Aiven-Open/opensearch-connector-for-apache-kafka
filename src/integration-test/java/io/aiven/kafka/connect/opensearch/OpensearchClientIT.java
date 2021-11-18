/*
 * Copyright 2021 Aiven Oy
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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;

import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetMappingsRequest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpensearchClientIT extends AbstractIT {


    @Test
    void getsVersion() {
        assertEquals(opensearchClient.getVersion(), "1.1.0");
    }

    @Test
    void createIndex() {
        assertTrue(opensearchClient.createIndex("index_1"));
        assertTrue(opensearchClient.indexExists("index_1"));
    }

    @Test
    void createIndexDoesNotExist() {
        assertFalse(opensearchClient.indexExists("index_2"));
    }

    @Test
    void createIndexExists() {
        assertFalse(opensearchClient.indexExists("index_2"));
        assertTrue(opensearchClient.createIndex("index_2"));
        assertTrue(opensearchClient.indexExists("index_2"));
    }

    @Test
    void createIndexDoesNotCreateAlreadyExistingIndex() {
        assertTrue(opensearchClient.createIndex("index_3"));
        assertTrue(opensearchClient.indexExists("index_3"));
        assertFalse(opensearchClient.createIndex("index_3"));
    }

    @Test
    void createMapping() throws IOException {
        assertTrue(opensearchClient.createIndex("index_4"));

        final var schema =
                SchemaBuilder.struct()
                        .name("record")
                        .field("name", SchemaBuilder.string().defaultValue("<default_name>").build())
                        .field("value", SchemaBuilder.int32().defaultValue(0).build())
                        .build();

        opensearchClient.createMapping("index_4", schema);
        assertTrue(opensearchClient.hasMapping("index_4"));

        final var response = opensearchClient.client.indices()
                .getMapping(new GetMappingsRequest().indices("index_4"), RequestOptions.DEFAULT)
                .mappings()
                .get("index_4").getSourceAsMap();

        assertTrue(response.containsKey("properties"));

        @SuppressWarnings("unchecked") final var properties = (Map<String, Object>) response.get("properties");
        assertTrue(properties.containsKey("name"));
        assertTrue(properties.containsKey("value"));

        @SuppressWarnings("unchecked") final var nameProperty = (Map<String, Object>) properties.get("name");
        assertEquals("text", nameProperty.get("type"));
        assertNull(nameProperty.get("null_value"));

        @SuppressWarnings("unchecked") final var valueProperty = (Map<String, Object>) properties.get("value");
        assertEquals("integer", valueProperty.get("type"));
        assertEquals(0, valueProperty.get("null_value"));
    }

    @Test
    void hasNoMapping() {
        opensearchClient.createIndex("index_5");
        assertFalse(opensearchClient.hasMapping("index_5"));
    }

}
