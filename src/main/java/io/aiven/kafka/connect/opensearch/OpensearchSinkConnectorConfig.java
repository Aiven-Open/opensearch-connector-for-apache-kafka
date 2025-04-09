/*
 * Copyright 2020 Aiven Oy
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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchSinkConnectorConfig extends AbstractConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpensearchSinkConnectorConfig.class);

    public static final String CONNECTOR_GROUP_NAME = "Connector";

    public static final String DATA_STREAM_GROUP_NAME = "Data Stream";

    public static final String DATA_CONVERSION_GROUP_NAME = "Data Conversion";

    public static final String CONNECTION_URL_CONFIG = "connection.url";
    private static final String CONNECTION_URL_DOC = "List of OpenSearch HTTP connection URLs e.g. ``http://eshost1:9200,"
            + "http://eshost2:9200``.";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "The number of records to process as a batch when writing to OpenSearch.";
    public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
    private static final String MAX_IN_FLIGHT_REQUESTS_DOC = "The maximum number of indexing requests that can be in-flight to OpenSearch before "
            + "blocking further requests.";
    public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
    private static final String MAX_BUFFERED_RECORDS_DOC = "The maximum number of records each task will buffer before blocking acceptance of more "
            + "records. This config can be used to limit the memory usage for each task.";
    public static final String LINGER_MS_CONFIG = "linger.ms";
    private static final String LINGER_MS_DOC = "Linger time in milliseconds for batching.\n"
            + "Records that arrive in between request transmissions are batched into a single bulk "
            + "indexing request, based on the ``" + BATCH_SIZE_CONFIG + "`` configuration. Normally "
            + "this only occurs under load when records arrive faster than they can be sent out. "
            + "However it may be desirable to reduce the number of requests even under light load and "
            + "benefit from bulk indexing. This setting helps accomplish that - when a pending batch is"
            + " not full, rather than immediately sending it out the task will wait up to the given "
            + "delay to allow other records to be added so that they can be batched into a single " + "request.";
    public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
    private static final String FLUSH_TIMEOUT_MS_DOC = "The timeout in milliseconds to use for periodic flushing, and when waiting for buffer "
            + "space to be made available by completed requests as records are added. If this timeout "
            + "is exceeded the task will fail.";
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    private static final String MAX_RETRIES_DOC = "The maximum number of retries that are allowed for failed indexing requests. If the retry "
            + "attempts are exhausted the task will fail.";
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "How long to wait in milliseconds before attempting the first retry of a failed indexing "
            + "request. Upon a failure, this connector may wait up to twice as long as the previous "
            + "wait, up to the maximum number of retries. "
            + "This avoids retrying in a tight loop under failure scenarios.";

    public static final String KEY_IGNORE_CONFIG = "key.ignore";
    public static final String KEY_IGNORE_ID_STRATEGY_CONFIG = "key.ignore.id.strategy";
    public static final String TOPIC_KEY_IGNORE_CONFIG = "topic.key.ignore";
    public static final String SCHEMA_IGNORE_CONFIG = "schema.ignore";
    public static final String TOPIC_SCHEMA_IGNORE_CONFIG = "topic.schema.ignore";
    public static final String DROP_INVALID_MESSAGE_CONFIG = "drop.invalid.message";

    private static final String KEY_IGNORE_DOC = "Whether to ignore the record key for the purpose of forming the OpenSearch document ID."
            + " When this is set to ``true``, document IDs will be generated according to the " + "``"
            + KEY_IGNORE_ID_STRATEGY_CONFIG + "`` strategy.\n"
            + "Note that this is a global config that applies to all topics, use " + "``" + TOPIC_KEY_IGNORE_CONFIG
            + "`` to apply ``" + KEY_IGNORE_ID_STRATEGY_CONFIG + "`` " + "strategy for specific topics only.";
    private static final String TOPIC_KEY_IGNORE_DOC = "List of topics for which ``" + KEY_IGNORE_CONFIG
            + "`` should be ``true``.";
    private static final String KEY_IGNORE_ID_STRATEGY_DOC = "Specifies the strategy to generate the Document ID. Only applicable when ``"
            + KEY_IGNORE_CONFIG + "`` is" + " ``true`` or specific topics are configured using ``"
            + TOPIC_KEY_IGNORE_CONFIG + "``. " + "Available strategies " + DocumentIDStrategy.describe() + ". "
            + "If not specified, the default generation strategy is ``topic.partition.offset``.\n";
    private static final String SCHEMA_IGNORE_CONFIG_DOC = "Whether to ignore schemas during indexing. When this is set to ``true``, the record "
            + "schema will be ignored for the purpose of registering an OpenSearch mapping. "
            + "OpenSearch will infer the mapping from the data (dynamic mapping needs to be enabled "
            + "by the user).\n Note that this is a global config that applies to all topics, use ``"
            + TOPIC_SCHEMA_IGNORE_CONFIG + "`` to override as ``true`` for specific topics.";
    private static final String TOPIC_SCHEMA_IGNORE_DOC = "List of topics for which ``" + SCHEMA_IGNORE_CONFIG
            + "`` should be ``true``.";
    private static final String DROP_INVALID_MESSAGE_DOC = "Whether to drop kafka message when it cannot be converted to output message.";

    public static final String COMPACT_MAP_ENTRIES_CONFIG = "compact.map.entries";
    private static final String COMPACT_MAP_ENTRIES_DOC = "Defines how map entries with string keys within record values should be written to JSON. "
            + "When this is set to ``true``, these entries are written compactly as "
            + "``\"entryKey\": \"entryValue\"``. "
            + "Otherwise, map entries with string keys are written as a nested document "
            + "``{\"key\": \"entryKey\", \"value\": \"entryValue\"}``. "
            + "All map entries with non-string keys are always written as nested documents. "
            + "Prior to 3.3.0, this connector always wrote map entries as nested documents, "
            + "so set this to ``false`` to use that older behavior.";

    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
    public static final String READ_TIMEOUT_MS_CONFIG = "read.timeout.ms";
    private static final String CONNECTION_TIMEOUT_MS_CONFIG_DOC = "How long to wait "
            + "in milliseconds when establishing a connection to the OpenSearch server. "
            + "The task fails if the client fails to connect to the server in this "
            + "interval, and will need to be restarted.";
    private static final String READ_TIMEOUT_MS_CONFIG_DOC = "How long to wait in "
            + "milliseconds for the OpenSearch server to send a response. The task fails "
            + "if any read operation times out, and will need to be restarted to resume " + "further operations.";

    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a "
            + "non-null key and a null value (i.e. Kafka tombstone records). Valid options are "
            + "``ignore``, ``delete``, and ``fail``.";

    public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "behavior.on.malformed.documents";
    private static final String BEHAVIOR_ON_MALFORMED_DOCS_DOC = "How to handle records that "
            + "OpenSearch rejects due to some malformation of the document itself, such as an index"
            + " mapping conflict or a field name containing illegal characters. \n" + "Valid options are:\n"
            + "- ``ignore`` - do not index the record\n"
            + "- ``warn`` - log a warning message and do not index the record\n"
            + "- ``report`` - report to errant record reporter and do not index the record\n"
            + "- ``fail`` - fail the task.\n\n";

    public static final String BEHAVIOR_ON_VERSION_CONFLICT_CONFIG = "behavior.on.version.conflict";
    private static final String BEHAVIOR_ON_VERSION_CONFLICT_DOC = "How to handle records that "
            + "OpenSearch rejects due to document's version conflicts.\n"
            + "It may happen when offsets were not committed or/and records have to be reprocessed.\n"
            + "Valid options are:\n" + "- ``ignore`` - ignore and keep the existing record\n"
            + "- ``warn`` - log a warning message and keep the existing record\n"
            + "- ``report`` - report to errant record reporter and keep the existing record\n"
            + "- ``fail`` - fail the task.\n\n";

    public static final String ROUTING_ENABLED_CONFIG = "routing.enabled";
    private static final String ROUTING_ENABLED_DOC = "Whether to enable routing for documents. "
            + "If set to true, the connector will use routing when sending documents to OpenSearch. "
            + "If set to false, no routing will be used. Default is false.";

    public static final String ROUTING_FIELD_PATH_CONFIG = "routing.field.path";
    private static final String ROUTING_FIELD_PATH_DOC = "The path of the field to pull from the payload "
            + "to use as the routing value for the document. Supports nested fields using dot notation (e.g., 'customer.id'). "
            + "If set, then that field from either the key or value will be used as the routing value. "
            + "If not set, then the entire key or value will be used as the routing value. "
            + "The value will be added to the PUT request to OpenSearch as the \"routing=...\" argument. "
            + "Only used if routing.enabled is true.";

    public static final String ROUTING_KEY_CONFIG = "routing.key";
    private static final String ROUTING_KEY_DOC = "Whether to use the Kafka key for routing instead of the value. "
            + "If set to true, the key will be used for routing. If set to false, the value will be used for routing. "
            + "Default is false"
            + "Only used if routing.enabled is true.";

    public static final String INDEX_WRITE_METHOD = "index.write.method";

    public static final String INDEX_WRITE_METHOD_DOC = String.format(
            "The method used to write data into OpenSearch index." + "The default value is ``%s`` which means that "
                    + "the record with the same document id will be replaced. "
                    + "The ``%s`` will create a new document if one does not exist or "
                    + "will update the existing document.",
            IndexWriteMethod.INSERT.name().toLowerCase(Locale.ROOT),
            IndexWriteMethod.UPSERT.name().toLowerCase(Locale.ROOT));

    public static final String DATA_STREAM_ENABLED = "data.stream.enabled";
    public static final String DATA_STREAM_INDEX_TEMPLATE_NAME = "data.streams.existing.index.template.name";

    public static final String DATA_STREAM_ENABLED_DOC = "Enable use of data streams. "
            + "If set to true the connector will write to data streams instead of regular indices. "
            + "Default is false.";

    public static final String DATA_STREAM_EXISTING_INDEX_TEMPLATE_NAME_DOC = "If "
            + "data.streams.existing.index.template.name is provided, and if that index "
            + "template does not exist, a template will be created with that name, else no template is created.";

    public static final String DATA_STREAM_PREFIX = "data.stream.prefix";

    public static final String DATA_STREAM_NAME_DOC = "Generic data stream name to write into. "
            + "If set, it will be used to construct the final data stream name in the form "
            + "of {data.stream.prefix}-{topic}.";

    public static final String DATA_STREAM_TIMESTAMP_FIELD = "data.stream.timestamp.field";

    public static final String DATA_STREAM_TIMESTAMP_FIELD_DEFAULT = "@timestamp";

    public static final String DATA_STREAM_TIMESTAMP_FIELD_DOC = "The Kafka record field to use as "
            + "the timestamp for the @timestamp field in documents sent to a data stream. The default is @timestamp.";

    protected static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectorConfigs(configDef);
        addConversionConfigs(configDef);
        addDataStreamConfig(configDef);
        addSpiConfigs(configDef);
        return configDef;
    }

    /**
     * Load configuration definitions from the extension points (if available) using {@link ServiceLoader} mechanism to
     * discover them.
     *
     * @param configDef
     *            configuration definitions to contribute to
     */
    private static void addSpiConfigs(final ConfigDef configDef) {
        final ServiceLoader<ConfigDefContributor> loaders = ServiceLoader.load(ConfigDefContributor.class,
                OpensearchSinkConnectorConfig.class.getClassLoader());

        final Iterator<ConfigDefContributor> iterator = loaders.iterator();
        while (iterator.hasNext()) {
            iterator.next().addConfig(configDef);
        }
    }

    private static void addConnectorConfigs(final ConfigDef configDef) {
        int order = 0;
        configDef.define(CONNECTION_URL_CONFIG, Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
            @Override
            public void ensureValid(final String name, final Object value) {
                // If value is null default validator for required value is triggered.
                if (value != null) {
                    @SuppressWarnings("unchecked")
                    final var urls = (List<String>) value;
                    for (final var url : urls) {
                        try {
                            new URL(url);
                        } catch (final MalformedURLException e) {
                            throw new ConfigException(CONNECTION_URL_CONFIG, url);
                        }
                    }
                }
            }

            @Override
            public String toString() {
                return String.join(", ", "http://eshost1:9200", "http://eshost2:9200");
            }
        }, Importance.HIGH, CONNECTION_URL_DOC, CONNECTOR_GROUP_NAME, ++order, Width.LONG, "Connection URLs")
                .define(BATCH_SIZE_CONFIG, Type.INT, 2000, Importance.MEDIUM, BATCH_SIZE_DOC, CONNECTOR_GROUP_NAME,
                        ++order, Width.SHORT, "Batch Size")
                .define(MAX_IN_FLIGHT_REQUESTS_CONFIG, Type.INT, 5, Importance.MEDIUM, MAX_IN_FLIGHT_REQUESTS_DOC,
                        CONNECTOR_GROUP_NAME, ++order, Width.SHORT, "Max In-flight Requests")
                .define(MAX_BUFFERED_RECORDS_CONFIG, Type.INT, 20000, Importance.LOW, MAX_BUFFERED_RECORDS_DOC,
                        CONNECTOR_GROUP_NAME, ++order, Width.SHORT, "Max Buffered Records")
                .define(LINGER_MS_CONFIG, Type.LONG, 1L, Importance.LOW, LINGER_MS_DOC, CONNECTOR_GROUP_NAME, ++order,
                        Width.SHORT, "Linger (ms)")
                .define(FLUSH_TIMEOUT_MS_CONFIG, Type.LONG, 10000L, Importance.LOW, FLUSH_TIMEOUT_MS_DOC,
                        CONNECTOR_GROUP_NAME, ++order, Width.SHORT, "Flush Timeout (ms)")
                .define(MAX_RETRIES_CONFIG, Type.INT, 5, Importance.LOW, MAX_RETRIES_DOC, CONNECTOR_GROUP_NAME, ++order,
                        Width.SHORT, "Max Retries")
                .define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, 100L, Importance.LOW, RETRY_BACKOFF_MS_DOC,
                        CONNECTOR_GROUP_NAME, ++order, Width.SHORT, "Retry Backoff (ms)")
                .define(CONNECTION_TIMEOUT_MS_CONFIG, Type.INT, 1000, Importance.LOW, CONNECTION_TIMEOUT_MS_CONFIG_DOC,
                        CONNECTOR_GROUP_NAME, ++order, Width.SHORT, "Connection Timeout")
                .define(READ_TIMEOUT_MS_CONFIG, Type.INT, 3000, Importance.LOW, READ_TIMEOUT_MS_CONFIG_DOC,
                        CONNECTOR_GROUP_NAME, ++order, Width.SHORT, "Read Timeout");
    }

    private static void addConversionConfigs(final ConfigDef configDef) {
        int order = 0;
        configDef
                .define(INDEX_WRITE_METHOD, Type.STRING, IndexWriteMethod.INSERT.toString().toLowerCase(Locale.ROOT),
                        IndexWriteMethod.VALIDATOR, Importance.LOW, INDEX_WRITE_METHOD_DOC, DATA_CONVERSION_GROUP_NAME,
                        ++order, Width.SHORT, "Index write method")
                .define(KEY_IGNORE_CONFIG, Type.BOOLEAN, false, Importance.HIGH, KEY_IGNORE_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Ignore Key mode")
                .define(KEY_IGNORE_ID_STRATEGY_CONFIG, Type.STRING,
                        DocumentIDStrategy.TOPIC_PARTITION_OFFSET.toString(), DocumentIDStrategy.VALIDATOR,
                        Importance.LOW, KEY_IGNORE_ID_STRATEGY_DOC, DATA_CONVERSION_GROUP_NAME, ++order, Width.LONG,
                        "Document ID generation strategy")
                .define(SCHEMA_IGNORE_CONFIG, Type.BOOLEAN, false, Importance.LOW, SCHEMA_IGNORE_CONFIG_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Ignore Schema mode")
                .define(COMPACT_MAP_ENTRIES_CONFIG, Type.BOOLEAN, true, Importance.LOW, COMPACT_MAP_ENTRIES_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Compact Map Entries")
                .define(TOPIC_KEY_IGNORE_CONFIG, Type.LIST, "", Importance.LOW, TOPIC_KEY_IGNORE_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.LONG, "Topics for 'Ignore Key' mode")
                .define(TOPIC_SCHEMA_IGNORE_CONFIG, Type.LIST, "", Importance.LOW, TOPIC_SCHEMA_IGNORE_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.LONG, "Topics for 'Ignore Schema' mode")
                .define(DROP_INVALID_MESSAGE_CONFIG, Type.BOOLEAN, false, Importance.LOW, DROP_INVALID_MESSAGE_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.LONG, "Drop invalid messages")
                .define(BEHAVIOR_ON_NULL_VALUES_CONFIG, Type.STRING,
                        RecordConverter.BehaviorOnNullValues.DEFAULT.toString(),
                        RecordConverter.BehaviorOnNullValues.VALIDATOR, Importance.LOW, BEHAVIOR_ON_NULL_VALUES_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Behavior for null-valued records")
                .define(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, Type.STRING,
                        BulkProcessor.BehaviorOnMalformedDoc.DEFAULT.toString(),
                        BulkProcessor.BehaviorOnMalformedDoc.VALIDATOR, Importance.LOW, BEHAVIOR_ON_MALFORMED_DOCS_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Behavior on malformed documents")
                .define(BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, Type.STRING,
                        BulkProcessor.BehaviorOnVersionConflict.DEFAULT.toString(),
                        BulkProcessor.BehaviorOnVersionConflict.VALIDATOR, Importance.LOW,
                        BEHAVIOR_ON_VERSION_CONFLICT_DOC, DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT,
                        "Behavior on document's version conflict (optimistic locking)")
                .define(ROUTING_ENABLED_CONFIG, Type.BOOLEAN, false, Importance.LOW, ROUTING_ENABLED_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Enable routing")
                .define(ROUTING_FIELD_PATH_CONFIG, Type.STRING, null, Importance.LOW, ROUTING_FIELD_PATH_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Routing field path")
                .define(ROUTING_KEY_CONFIG, Type.BOOLEAN, false, Importance.LOW, ROUTING_KEY_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Use Kafka key for routing");
    }

    private static void addDataStreamConfig(final ConfigDef configDef) {
        int order = 0;
        configDef
                .define(DATA_STREAM_ENABLED, Type.BOOLEAN, false, Importance.MEDIUM, DATA_STREAM_ENABLED_DOC,
                        DATA_STREAM_GROUP_NAME, ++order, Width.LONG, "Data stream name")
                .define(DATA_STREAM_PREFIX, Type.STRING, null, new ConfigDef.NonEmptyString(), Importance.MEDIUM,
                        DATA_STREAM_NAME_DOC, DATA_STREAM_GROUP_NAME, ++order, Width.LONG, "Data stream name")
                .define(DATA_STREAM_TIMESTAMP_FIELD, Type.STRING, DATA_STREAM_TIMESTAMP_FIELD_DEFAULT,
                        new ConfigDef.NonEmptyString(), Importance.MEDIUM, DATA_STREAM_TIMESTAMP_FIELD_DOC,
                        DATA_STREAM_GROUP_NAME, ++order, Width.LONG, "Data stream timestamp field")
                .define(DATA_STREAM_INDEX_TEMPLATE_NAME, Type.STRING, null, Importance.MEDIUM,
                        DATA_STREAM_EXISTING_INDEX_TEMPLATE_NAME_DOC, DATA_STREAM_GROUP_NAME, ++order, Width.LONG,
                        "Data stream name");
    }

    public static final ConfigDef CONFIG = baseConfigDef();

    public OpensearchSinkConnectorConfig(final Map<String, String> props) {
        super(CONFIG, props);
        validate();
    }

    public boolean requiresErrantRecordReporter() {
        return behaviorOnMalformedDoc() == BulkProcessor.BehaviorOnMalformedDoc.REPORT
                || behaviorOnVersionConflict() == BulkProcessor.BehaviorOnVersionConflict.REPORT;
    }

    private void validate() {
        if (dataStreamEnabled() && indexWriteMethod() == IndexWriteMethod.UPSERT) {
            throw new ConfigException("Data streams do not support upsert index write method");
        }
        if (!dataStreamEnabled() && dataStreamPrefix().isPresent()) {
            LOGGER.warn("The property data.stream.prefix was set but data streams are not enabled");
        }
        if (indexWriteMethod() == IndexWriteMethod.UPSERT && ignoreKey()
                && documentIdStrategy() != DocumentIDStrategy.RECORD_KEY) {
            throw new ConfigException(KEY_IGNORE_ID_STRATEGY_CONFIG, documentIdStrategy().toString(),
                    String.format("%s is not supported for index upsert. Supported is: %s",
                            documentIdStrategy().toString(), DocumentIDStrategy.RECORD_KEY));
        }
    }

    public HttpHost[] httpHosts() {
        final var connectionUrls = connectionUrls();
        final var httpHosts = new HttpHost[connectionUrls.size()];
        int idx = 0;
        for (final var url : connectionUrls) {
            httpHosts[idx] = HttpHost.create(url);
            idx++;
        }
        return httpHosts;
    }

    private List<String> connectionUrls() {
        return getList(CONNECTION_URL_CONFIG).stream()
                .map(u -> u.endsWith("/") ? u.substring(0, u.length() - 1) : u)
                .collect(Collectors.toList());
    }

    public int connectionTimeoutMs() {
        return getInt(CONNECTION_TIMEOUT_MS_CONFIG);
    }

    public int readTimeoutMs() {
        return getInt(READ_TIMEOUT_MS_CONFIG);
    }

    public boolean ignoreKey() {
        return getBoolean(OpensearchSinkConnectorConfig.KEY_IGNORE_CONFIG);
    }

    public boolean ignoreSchema() {
        return getBoolean(OpensearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG);
    }

    public boolean useCompactMapEntries() {
        return getBoolean(OpensearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG);
    }

    protected IndexWriteMethod indexWriteMethod() {
        return IndexWriteMethod.valueOf(getString(INDEX_WRITE_METHOD).toUpperCase(Locale.ROOT));
    }

    public boolean dataStreamEnabled() {
        return getBoolean(DATA_STREAM_ENABLED);
    }

    public Optional<String> dataStreamExistingIndexTemplateName() {
        return Optional.ofNullable(getString(OpensearchSinkConnectorConfig.DATA_STREAM_INDEX_TEMPLATE_NAME));
    }

    public Optional<String> dataStreamPrefix() {
        return Optional.ofNullable(getString(OpensearchSinkConnectorConfig.DATA_STREAM_PREFIX));
    }

    public String dataStreamTimestampField() {
        return getString(OpensearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD);
    }

    public Set<String> topicIgnoreKey() {
        return Set.copyOf(getList(OpensearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG));
    }

    public Set<String> topicIgnoreSchema() {
        return Set.copyOf(getList(OpensearchSinkConnectorConfig.TOPIC_SCHEMA_IGNORE_CONFIG));
    }

    public long flushTimeoutMs() {
        return getLong(OpensearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
    }

    public int maxBufferedRecords() {
        return getInt(OpensearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
    }

    public int batchSize() {
        return getInt(OpensearchSinkConnectorConfig.BATCH_SIZE_CONFIG);
    }

    public long lingerMs() {
        return getLong(OpensearchSinkConnectorConfig.LINGER_MS_CONFIG);
    }

    public int maxInFlightRequests() {
        return getInt(OpensearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
    }

    public long retryBackoffMs() {
        return getLong(OpensearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
    }

    public int maxRetry() {
        return getInt(OpensearchSinkConnectorConfig.MAX_RETRIES_CONFIG);
    }

    public boolean dropInvalidMessage() {
        return getBoolean(OpensearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG);
    }

    private DocumentIDStrategy documentIdStrategy() {
        return DocumentIDStrategy.fromString(getString(KEY_IGNORE_ID_STRATEGY_CONFIG));
    }

    public DocumentIDStrategy documentIdStrategy(final String topic) {
        return (ignoreKey() || topicIgnoreKey().contains(topic)) ? documentIdStrategy() : DocumentIDStrategy.RECORD_KEY;
    }

    public Function<String, String> topicToIndexNameConverter() {
        return dataStreamEnabled()
                ? this::convertTopicToDataStreamName
                : OpensearchSinkConnectorConfig::convertTopicToIndexName;
    }

    private static String convertTopicToIndexName(final String topic) {
        var indexName = topic.toLowerCase();
        if (indexName.length() > 255) {
            indexName = indexName.substring(0, 255);
            LOGGER.warn("Topic {} length is more than 255 bytes. The final index name is {}", topic, indexName);
        }
        if (indexName.contains(":")) {
            indexName = indexName.replaceAll(":", "_");
            LOGGER.warn("Topic {} contains :. The final index name is {}", topic, indexName);
        }
        if (indexName.startsWith("-") || indexName.startsWith("_") || indexName.startsWith("+")) {
            indexName = indexName.substring(1);
            LOGGER.warn("Topic {} starts with -, _ or +. The final index name is {}", topic, indexName);
        }
        if (indexName.equals(".") || indexName.equals("..")) {
            indexName = indexName.replace(".", "dot");
            LOGGER.warn("Topic {} name is . or .. . The final index name is {}", topic, indexName);
        }
        return indexName;
    }

    private String convertTopicToDataStreamName(final String topic) {
        return dataStreamPrefix().map(prefix -> String.format("%s-%s", prefix, convertTopicToIndexName(topic)))
                .orElseGet(() -> convertTopicToIndexName(topic));
    }

    public boolean ignoreSchemaFor(final String topic) {
        return ignoreSchema() || topicIgnoreSchema().contains(topic);
    }

    public RecordConverter.BehaviorOnNullValues behaviorOnNullValues() {
        return RecordConverter.BehaviorOnNullValues
                .forValue(getString(OpensearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG));
    }

    public BulkProcessor.BehaviorOnMalformedDoc behaviorOnMalformedDoc() {
        return BulkProcessor.BehaviorOnMalformedDoc
                .forValue(getString(OpensearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG));
    }

    public BulkProcessor.BehaviorOnVersionConflict behaviorOnVersionConflict() {
        return BulkProcessor.BehaviorOnVersionConflict
                .forValue(getString(OpensearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG));
    }

    public boolean isRoutingEnabled() {
        return getBoolean(OpensearchSinkConnectorConfig.ROUTING_ENABLED_CONFIG);
    }

    public Optional<String> routingFieldPath() {
        return Optional.ofNullable(getString(OpensearchSinkConnectorConfig.ROUTING_FIELD_PATH_CONFIG));
    }

    public boolean useRoutingKey() {
        return getBoolean(OpensearchSinkConnectorConfig.ROUTING_KEY_CONFIG);
    }

    public static void main(final String[] args) {
        System.out.println("=========================================");
        System.out.println("OpenSearch Sink Connector Configuration Options");
        System.out.println("=========================================");
        System.out.println();
        System.out.println(CONFIG.toEnrichedRst());
    }
}
