/*
 * Copyright 2023 Aiven Oy
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

public class DocumentIDStrategyTest {

    @Test
    void noneTypeAlwaysWithoutKey() {
        final var documentId = DocumentIDStrategy.NONE.documentId(createRecord());
        assertNull(documentId);
    }

    @Test
    void topicPartitionOffsetTypeKey() {
        final var documentId = DocumentIDStrategy.TOPIC_PARTITION_OFFSET.documentId(createRecord());
        assertEquals(String.format("%s+%s+%s", "a", 1, 13), documentId);
    }

    @Test
    void recordKeyTypeKey() {
        final var stringKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY.documentId(createRecord());
        assertEquals("a", stringKeySchemaDocumentId);

        final var int8KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT8_SCHEMA, (byte) 10));
        assertEquals("10", int8KeySchemaDocumentId);

        final var int8KWithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(null, (byte) 10));
        assertEquals("10", int8KWithoutKeySchemaDocumentId);

        final var int16KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT8_SCHEMA, (short) 11));
        assertEquals("11", int16KeySchemaDocumentId);

        final var int16WithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(null, (short) 11));
        assertEquals("11", int16WithoutKeySchemaDocumentId);

        final var int32KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT8_SCHEMA, 12));
        assertEquals("12", int32KeySchemaDocumentId);

        final var int32WithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY.documentId(createRecord(null, 12));
        assertEquals("12", int32WithoutKeySchemaDocumentId);

        final var int64KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT64_SCHEMA, (long) 13));
        assertEquals("13", int64KeySchemaDocumentId);

        final var int64WithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(null, (long) 13));
        assertEquals("13", int64WithoutKeySchemaDocumentId);
    }

    @Test
    void recordKeyTypeThrowsDataException() {
        // unsupported schema type
        assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(Schema.FLOAT64_SCHEMA, 1f)));
        // unknown type without Key schema
        assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(Schema.FLOAT64_SCHEMA, 1f)));
    }

    @Test
    void recordKeyTypeThrowsDataExceptionForNoKeyValue() {
        assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(Schema.FLOAT64_SCHEMA, null)));
    }

    SinkRecord createRecord() {
        return createRecord(Schema.STRING_SCHEMA, "a");
    }

    SinkRecord createRecord(final Schema keySchema, final Object keyValue) {
        return new SinkRecord("a", 1, keySchema, keyValue,
                SchemaBuilder.struct().name("struct").field("string", Schema.STRING_SCHEMA).build(), null, 13);
    }

}
