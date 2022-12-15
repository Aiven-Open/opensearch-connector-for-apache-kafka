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

import java.util.Objects;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static io.aiven.kafka.connect.opensearch.OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD_DEFAULT;

@FunctionalInterface
interface RequestBuilder {

    @FunctionalInterface
    interface SetDocumentIDStrategy {
        SetPayloadBuilder withDocumentIDStrategy(final DocumentIDStrategy documentIDStrategy);
    }

    @FunctionalInterface
    interface SetPayloadBuilder {
        RequestBuilder withPayloadBuilder(final PayloadBuilder payloadBuilder);
    }

    @FunctionalInterface
    interface SetObjectMapper {
        SetDocumentIDStrategy withObjectMapper(final ObjectMapper objectMapper);
    }

    @FunctionalInterface
    interface SetDataStreamTimestampField {
        SetObjectMapper withDataStreamTimestampField(final String dataStreamTimestampField);
    }

    DocWriteRequest<?> build(final String index, final SinkRecord record);

    static RequestBuilder create(final OpensearchSinkConnectorConfig config) {
        return (index, record) -> {
            final var payloadBuilder = new PayloadBuilder(config);
            final var objectMapper = new ObjectMapper();
            if (Objects.isNull(record.value())) {
                switch (config.behaviorOnNullValues()) {
                    case IGNORE:
                        return null;
                    case DELETE:
                        if (Objects.isNull(record.key())) {
                            // Since the record key is used as the ID of the index to delete and we don't have a key
                            // for this record, we can't delete anything anyways, so we ignore the record.
                            // We can also disregard the value of the ignoreKey parameter, since even if it's true
                            // the resulting index we'd try to delete would be based solely off topic/partition/
                            // offset information for the SinkRecord. Since that information is guaranteed to be
                            // unique per message, we can be confident that there wouldn't be any corresponding
                            // index present in ES to delete anyways.
                            return null;
                        }
                        break;
                    case FAIL:
                    default:
                        throw new DataException(String.format(
                                "Sink record with key of %s and null value encountered for topic/partition/offset "
                                        + "%s/%s/%s (to ignore future records like this "
                                        + "change the configuration property "
                                        + "'%s' from '%s' to '%s')",
                                record.key(),
                                record.topic(),
                                record.kafkaPartition(),
                                record.kafkaOffset(),
                                OpensearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
                                BehaviorOnNullValues.FAIL,
                                BehaviorOnNullValues.IGNORE
                        ));
                }
            }
            if (Objects.isNull(record.value())) {
                return config.docIdStrategy(record.topic()).updateRequest(new DeleteRequest(index), record);
            }
            if (config.dataStreamEnabled()) {
                return buildDataStreamRequest()
                        .withDataStreamTimestampField(config.dataStreamTimestampField())
                        .withObjectMapper(objectMapper)
                        .withDocumentIDStrategy(config.docIdStrategy(record.topic()))
                        .withPayloadBuilder(payloadBuilder)
                        .build(index, record);
            } else {
                return buildInsertRequest()
                        .withDocumentIDStrategy(config.docIdStrategy(record.topic()))
                        .withPayloadBuilder(payloadBuilder)
                        .build(index, record);
            }
        };
    }

    private static SetDocumentIDStrategy buildInsertRequest() {
        return documentIDStrategy -> payloadBuilder -> (index, record) ->
                documentIDStrategy
                        .updateRequest(
                                new IndexRequest()
                                        .index(index)
                                        .opType(DocWriteRequest.OpType.INDEX)
                                        .source(payloadBuilder.buildPayload(record), XContentType.JSON),
                                record);
    }

    private static SetDataStreamTimestampField buildDataStreamRequest() {
        return dataStreamTimestampField -> objectMapper -> documentIDStrategy -> payloadBuilder -> (index, record) -> {
            var payload = payloadBuilder.buildPayload(record);
            if (DATA_STREAM_TIMESTAMP_FIELD_DEFAULT.equals(dataStreamTimestampField)) {
                try {
                    final var jsonObject = readJsonObject(payload, objectMapper);
                    if (!jsonObject.has(DATA_STREAM_TIMESTAMP_FIELD_DEFAULT)) {
                        jsonObject.put(DATA_STREAM_TIMESTAMP_FIELD_DEFAULT, record.timestamp());
                        payload = objectMapper.writeValueAsString(jsonObject);
                    }
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            return documentIDStrategy.updateRequest(
                    new IndexRequest()
                            .index(index)
                            .opType(DocWriteRequest.OpType.CREATE)
                            .source(payload, XContentType.JSON),
                    record
            );
        };
    }

    private static ObjectNode readJsonObject(final String payload, final ObjectMapper objectMapper)
            throws JsonProcessingException {
        final var json = objectMapper.readTree(payload);
        if (!json.isObject()) {
            throw new DataException(
                    "JSON payload is a type of " + json.getNodeType() + ". Required is JSON Object.");
        }
        return (ObjectNode) json;
    }

}
