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

import static org.apache.kafka.common.config.SslConfigs.SSL_CIPHER_SUITES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
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
import org.apache.kafka.common.config.SslConfigs;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;

import org.apache.hc.core5.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchSinkConnectorConfig extends AbstractConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSinkConnectorConfig.class);

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

    private final static String SSL_SETTINGS_GROUP_NAME = "TLS Configuration for HTTPS";

    public final static String SSL_CONFIG_PREFIX = "connection.";

    public final static String SSL_CONFIG_TRUST_ALL_CERTIFICATES = "trust.all.certificates";
    public final static String SSL_CONFIG_TRUST_ALL_CERTIFICATES_DOC = "Allow to trust all certificates. Default is false";

    protected static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectorConfigs(configDef);
        addSslConfig(configDef);
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
                OpenSearchSinkConnectorConfig.class.getClassLoader());

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

    private static void addSslConfig(final ConfigDef configDef) {
        ConfigDef sslConfigDef = new ConfigDef();
        sslConfigDef
                .define(SslConfigs.SSL_PROTOCOL_CONFIG, ConfigDef.Type.STRING, SslConfigs.DEFAULT_SSL_PROTOCOL,
                        ConfigDef.Importance.MEDIUM, SslConfigs.SSL_PROTOCOL_DOC)
                .define(SSL_CIPHER_SUITES_CONFIG, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW,
                        SslConfigs.SSL_CIPHER_SUITES_DOC)
                .define(SSL_ENABLED_PROTOCOLS_CONFIG, ConfigDef.Type.LIST, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS,
                        ConfigDef.Importance.MEDIUM, SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
                .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, ConfigDef.Type.STRING,
                        SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, ConfigDef.Importance.MEDIUM,
                        SslConfigs.SSL_KEYSTORE_TYPE_DOC)
                .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
                .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null,
                        ConfigDef.Importance.HIGH, SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
                .define(SSL_KEY_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH,
                        SslConfigs.SSL_KEY_PASSWORD_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ConfigDef.Type.STRING,
                        SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, ConfigDef.Importance.MEDIUM,
                        SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
                .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null,
                        ConfigDef.Importance.HIGH, SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ConfigDef.Type.STRING,
                        SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ConfigDef.Importance.LOW,
                        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
                .define(SSL_CONFIG_TRUST_ALL_CERTIFICATES, Type.BOOLEAN, false, Importance.LOW,
                        SSL_CONFIG_TRUST_ALL_CERTIFICATES_DOC);

        configDef.embed(SSL_CONFIG_PREFIX, SSL_SETTINGS_GROUP_NAME, configDef.configKeys().size() + 1, sslConfigDef);
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
                .define(BEHAVIOR_ON_NULL_VALUES_CONFIG, Type.STRING, BehaviorOnNullValues.DEFAULT.toString(),
                        BehaviorOnNullValues.VALIDATOR, Importance.LOW, BEHAVIOR_ON_NULL_VALUES_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Behavior for null-valued records")
                .define(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, Type.STRING, BehaviorOnMalformedDoc.DEFAULT.toString(),
                        BehaviorOnMalformedDoc.VALIDATOR, Importance.LOW, BEHAVIOR_ON_MALFORMED_DOCS_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT, "Behavior on malformed documents")
                .define(BEHAVIOR_ON_VERSION_CONFLICT_CONFIG, Type.STRING, BehaviorOnVersionConflict.DEFAULT.toString(),
                        BehaviorOnVersionConflict.VALIDATOR, Importance.LOW, BEHAVIOR_ON_VERSION_CONFLICT_DOC,
                        DATA_CONVERSION_GROUP_NAME, ++order, Width.SHORT,
                        "Behavior on document's version conflict (optimistic locking)");
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

    public OpenSearchSinkConnectorConfig(final Map<String, String> props) {
        super(CONFIG, props);
        validate();
    }

    public boolean requiresErrantRecordReporter() {
        return behaviorOnMalformedDoc() == BehaviorOnMalformedDoc.REPORT
                || behaviorOnVersionConflict() == BehaviorOnVersionConflict.REPORT;
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
        try {
            final var connectionUrls = connectionUrls();
            final var httpHosts = new HttpHost[connectionUrls.size()];
            int idx = 0;
            for (final var url : connectionUrls) {
                httpHosts[idx] = HttpHost.create(url);
                idx++;
            }
            return httpHosts;
        } catch (URISyntaxException e) {
            throw new ConfigException(CONNECTION_URL_CONFIG, String.format("Wrong URI format. %s", e.getMessage()));
        }
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
        return getBoolean(OpenSearchSinkConnectorConfig.KEY_IGNORE_CONFIG);
    }

    public boolean ignoreSchema() {
        return getBoolean(OpenSearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG);
    }

    public boolean useCompactMapEntries() {
        return getBoolean(OpenSearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG);
    }

    public IndexWriteMethod indexWriteMethod() {
        return IndexWriteMethod.valueOf(getString(INDEX_WRITE_METHOD).toUpperCase(Locale.ROOT));
    }

    public boolean dataStreamEnabled() {
        return getBoolean(DATA_STREAM_ENABLED);
    }

    public Optional<String> dataStreamExistingIndexTemplateName() {
        return Optional.ofNullable(getString(OpenSearchSinkConnectorConfig.DATA_STREAM_INDEX_TEMPLATE_NAME));
    }

    public Optional<String> dataStreamPrefix() {
        return Optional.ofNullable(getString(OpenSearchSinkConnectorConfig.DATA_STREAM_PREFIX));
    }

    public String dataStreamTimestampField() {
        return getString(OpenSearchSinkConnectorConfig.DATA_STREAM_TIMESTAMP_FIELD);
    }

    public Set<String> topicIgnoreKey() {
        return Set.copyOf(getList(OpenSearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG));
    }

    public Set<String> topicIgnoreSchema() {
        return Set.copyOf(getList(OpenSearchSinkConnectorConfig.TOPIC_SCHEMA_IGNORE_CONFIG));
    }

    public long flushTimeoutMs() {
        return getLong(OpenSearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
    }

    public int maxBufferedRecords() {
        return getInt(OpenSearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
    }

    public int batchSize() {
        return getInt(OpenSearchSinkConnectorConfig.BATCH_SIZE_CONFIG);
    }

    public long lingerMs() {
        return getLong(OpenSearchSinkConnectorConfig.LINGER_MS_CONFIG);
    }

    public int maxInFlightRequests() {
        return getInt(OpenSearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
    }

    public long retryBackoffMs() {
        return getLong(OpenSearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
    }

    public int maxRetry() {
        return getInt(OpenSearchSinkConnectorConfig.MAX_RETRIES_CONFIG);
    }

    public boolean dropInvalidMessage() {
        return getBoolean(OpenSearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG);
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
                : OpenSearchSinkConnectorConfig::convertTopicToIndexName;
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

    public BehaviorOnNullValues behaviorOnNullValues() {
        return BehaviorOnNullValues.forValue(getString(OpenSearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG));
    }

    public BehaviorOnMalformedDoc behaviorOnMalformedDoc() {
        return BehaviorOnMalformedDoc
                .forValue(getString(OpenSearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG));
    }

    public BehaviorOnVersionConflict behaviorOnVersionConflict() {
        return BehaviorOnVersionConflict
                .forValue(getString(OpenSearchSinkConnectorConfig.BEHAVIOR_ON_VERSION_CONFLICT_CONFIG));
    }

    public Optional<Path> trustStorePath() {
        return Optional.ofNullable(getString(SSL_CONFIG_PREFIX + SSL_TRUSTSTORE_LOCATION_CONFIG)).map(Path::of);
    }

    public String trustStoreType() {
        return getString(SSL_CONFIG_PREFIX + SSL_TRUSTSTORE_TYPE_CONFIG);
    }

    public char[] trustStorePassword() {
        final var pwd = getPassword(SSL_CONFIG_PREFIX + SSL_TRUSTSTORE_PASSWORD_CONFIG);
        if (pwd == null) {
            return null;
        }
        return pwd.value().toCharArray();
    }

    public Optional<Path> keyStorePath() {
        return Optional.ofNullable(getString(SSL_CONFIG_PREFIX + SSL_KEYSTORE_LOCATION_CONFIG)).map(Path::of);
    }

    public char[] keyStorePassword() {
        final var pwd = getPassword(SSL_CONFIG_PREFIX + SSL_KEYSTORE_PASSWORD_CONFIG);
        if (pwd == null) {
            return null;
        }
        return pwd.value().toCharArray();
    }

    public String keyStoreType() {
        return getString(SSL_CONFIG_PREFIX + SSL_KEYSTORE_TYPE_CONFIG);
    }

    public char[] keyPassword() {
        final var pwd = getPassword(SSL_CONFIG_PREFIX + SSL_KEY_PASSWORD_CONFIG);
        if (pwd == null) {
            return null;
        }
        return pwd.value().toCharArray();
    }

    public String sslProtocol() {
        return getString(SSL_CONFIG_PREFIX + SSL_PROTOCOL_CONFIG);
    }

    public String[] sslEnableProtocols() {
        return getList(SSL_CONFIG_PREFIX + SSL_ENABLED_PROTOCOLS_CONFIG).toArray(new String[0]);
    }

    public String[] cipherSuitesConfig() {
        final var suites = getList(SSL_CONFIG_PREFIX + SSL_CIPHER_SUITES_CONFIG);
        if (suites != null && !suites.isEmpty()) {
            return suites.toArray(new String[0]);
        }
        return null;
    }

    public boolean disableHostnameVerification() {
        String sslEndpointIdentificationAlgorithm = getString(
                SSL_CONFIG_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        return sslEndpointIdentificationAlgorithm != null && sslEndpointIdentificationAlgorithm.isEmpty();
    }

    public boolean trustAllCertificates() {
        return getBoolean(SSL_CONFIG_PREFIX + SSL_CONFIG_TRUST_ALL_CERTIFICATES);
    }

    public static void main(final String[] args) {
        System.out.println("=========================================");
        System.out.println("OpenSearch Sink Connector Configuration Options");
        System.out.println("=========================================");
        System.out.println();
        System.out.println(CONFIG.toEnrichedRst());
    }
}
