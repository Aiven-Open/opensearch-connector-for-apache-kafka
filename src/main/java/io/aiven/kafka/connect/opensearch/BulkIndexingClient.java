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

package io.aiven.kafka.connect.opensearch;

import java.io.IOException;
import java.util.List;

import io.aiven.kafka.connect.opensearch.bulk.BulkClient;
import io.aiven.kafka.connect.opensearch.bulk.BulkRequest;
import io.aiven.kafka.connect.opensearch.bulk.BulkResponse;

public class BulkIndexingClient implements BulkClient<IndexableRecord, BulkRequest> {

    private final OpensearchClient client;

    public BulkIndexingClient(final OpensearchClient client) {
        this.client = client;
    }

    @Override
    public BulkRequest bulkRequest(final List<IndexableRecord> batch) {
        return null;
    }

    @Override
    public BulkResponse execute(final BulkRequest bulk) throws IOException {
        return null;
    }

}
