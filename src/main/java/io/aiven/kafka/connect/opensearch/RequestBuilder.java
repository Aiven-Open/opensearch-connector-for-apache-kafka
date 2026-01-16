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

import static io.aiven.kafka.connect.opensearch.OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT;

import java.io.IOException;
import java.util.Objects;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.VersionType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionalInterface
public interface RequestBuilder {

    Logger LOGGER = LoggerFactory.getLogger(RequestBuilder.class);

    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @FunctionalInterface
    interface SetOpenSearchSinkConnectorConfig {
        SetIndexName withConfig(final OpenSearchSinkConnectorConfig config);
    }

    @FunctionalInterface
    interface SetIndexName {
        SetSinkRecord withIndex(final String index);
    }

    @FunctionalInterface
    interface SetSinkRecord {
        SetPayload withSinkRecord(final SinkRecord record);
    }

    @FunctionalInterface
    @Deprecated(since = "2.1.0")
    interface SetPayload {
        @Deprecated(since = "2.1.0")
        RequestBuilder withPayload(final String payload);
    }

    DocWriteRequest<?> build();

    static SetOpenSearchSinkConnectorConfig builder() {
        return config -> index -> record -> payload -> () -> {
            final var documentIDStrategy = config.documentIdStrategy(record.topic());
            final var documentId = documentIDStrategy.documentId(record);
            if (Objects.isNull(record.value())) {
                final var deleteRequest = new DeleteRequest().id(documentId).index(index);
                return config.dataStreamEnabled()
                        ? deleteRequest
                        : addVersionIfAny(documentIDStrategy, record, deleteRequest);
            }
            if (config.indexWriteMethod() == IndexWriteMethod.UPSERT) {
                return new UpdateRequest().id(documentId)
                        .index(index)
                        .doc(payload, XContentType.JSON)
                        .upsert(payload, XContentType.JSON)
                        .docAsUpsert(true)
                        .retryOnConflict(Math.min(config.maxInFlightRequests(), 3));
            } else {
                final var indexRequest = new IndexRequest().id(documentId).index(index);
                if (config.dataStreamEnabled()) {
                    return indexRequest.opType(DocWriteRequest.OpType.CREATE)
                            .source(addTimestampToPayload(config, record, payload), XContentType.JSON);
                } else {
                    return addVersionIfAny(documentIDStrategy, record,
                            indexRequest.opType(DocWriteRequest.OpType.INDEX).source(payload, XContentType.JSON));
                }
            }
        };
    }

    private static DocWriteRequest<?> addVersionIfAny(final DocumentIDStrategy documentIDStrategy,
            final SinkRecord record, final DocWriteRequest<?> request) {
        if (documentIDStrategy == DocumentIDStrategy.RECORD_KEY) {
            request.versionType(VersionType.EXTERNAL);
            request.version(record.kafkaOffset());
        }
        return request;
    }

    private static String addTimestampToPayload(final OpenSearchSinkConnectorConfig config, final SinkRecord record,
            final String payload) {
        if (DATA_STREAM_TIMESTAMP_FIELD_DEFAULT.equals(config.dataStreamTimestampField())) {
            try {
                final var json = OBJECT_MAPPER.readTree(payload);
                if (!json.isObject()) {
                    throw new DataException(
                            "JSON payload is a type of " + json.getNodeType() + ". Required is JSON Object.");
                }
                final var rootObject = (ObjectNode) json;
                if (!rootObject.has(DATA_STREAM_TIMESTAMP_FIELD_DEFAULT)) {
                    if (Objects.isNull(record.timestamp())) {
                        throw new DataException("Record timestamp hasn't been set");
                    }
                    rootObject.put(DATA_STREAM_TIMESTAMP_FIELD_DEFAULT, record.timestamp());
                }
                return OBJECT_MAPPER.writeValueAsString(json);
            } catch (final IOException e) {
                throw new DataException("Could not parse payload", e);
            }
        } else {
            return payload;
        }
    }

}
