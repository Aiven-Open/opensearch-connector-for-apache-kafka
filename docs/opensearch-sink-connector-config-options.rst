=========================================
OpenSearch Sink Connector Configuration Options
=========================================

Connector
^^^^^^^^^

``connection.url``
  List of OpenSearch HTTP connection URLs e.g. ``http://eshost1:9200,http://eshost2:9200``.

  * Type: list
  * Valid Values: http://eshost1:9200, http://eshost2:9200
  * Importance: high

``batch.size``
  The number of records to process as a batch when writing to OpenSearch.

  * Type: int
  * Default: 2000
  * Importance: medium

``max.in.flight.requests``
  The maximum number of indexing requests that can be in-flight to OpenSearch before blocking further requests.

  * Type: int
  * Default: 5
  * Importance: medium

``max.buffered.records``
  The maximum number of records each task will buffer before blocking acceptance of more records. This config can be used to limit the memory usage for each task.

  * Type: int
  * Default: 20000
  * Importance: low

``linger.ms``
  Linger time in milliseconds for batching.

  Records that arrive in between request transmissions are batched into a single bulk indexing request, based on the ``batch.size`` configuration. Normally this only occurs under load when records arrive faster than they can be sent out. However it may be desirable to reduce the number of requests even under light load and benefit from bulk indexing. This setting helps accomplish that - when a pending batch is not full, rather than immediately sending it out the task will wait up to the given delay to allow other records to be added so that they can be batched into a single request.

  * Type: long
  * Default: 1
  * Importance: low

``flush.timeout.ms``
  The timeout in milliseconds to use for periodic flushing, and when waiting for buffer space to be made available by completed requests as records are added. If this timeout is exceeded the task will fail.

  * Type: long
  * Default: 10000 (10 seconds)
  * Importance: low

``max.retries``
  The maximum number of retries that are allowed for failed indexing requests. If the retry attempts are exhausted the task will fail.

  * Type: int
  * Default: 5
  * Importance: low

``retry.backoff.ms``
  How long to wait in milliseconds before attempting the first retry of a failed indexing request. Upon a failure, this connector may wait up to twice as long as the previous wait, up to the maximum number of retries. This avoids retrying in a tight loop under failure scenarios.

  * Type: long
  * Default: 100
  * Importance: low

``connection.timeout.ms``
  How long to wait in milliseconds when establishing a connection to the OpenSearch server. The task fails if the client fails to connect to the server in this interval, and will need to be restarted.

  * Type: int
  * Default: 1000 (1 second)
  * Importance: low

``read.timeout.ms``
  How long to wait in milliseconds for the OpenSearch server to send a response. The task fails if any read operation times out, and will need to be restarted to resume further operations.

  * Type: int
  * Default: 3000 (3 seconds)
  * Importance: low

Data Conversion
^^^^^^^^^^^^^^^

``index.write.method``
  The method used to write data into OpenSearch index.The default value is ``insert`` which means that the record with the same document id will be replaced. The ``upsert`` will create a new document if one does not exist or will update the existing document.

  * Type: string
  * Default: insert
  * Valid Values: ``insert``, ``upsert``
  * Importance: low

``key.ignore``
  Whether to ignore the record key for the purpose of forming the OpenSearch document ID. When this is set to ``true``, document IDs will be generated according to the ``key.ignore.id.strategy`` strategy.

  Note that this is a global config that applies to all topics, use ``topic.key.ignore`` to apply ``key.ignore.id.strategy`` strategy for specific topics only.

  * Type: boolean
  * Default: false
  * Importance: high

``key.ignore.id.strategy``
  Specifies the strategy to generate the Document ID. Only applicable when ``key.ignore`` is ``true`` or specific topics are configured using ``topic.key.ignore``. Available strategies {none : No Doc ID is added, record.key : Generated from the record's key, topic.partition.offset : Generated as record's ``topic+partition+offset``}. If not specified, the default generation strategy is ``topic.partition.offset``.

  * Type: string
  * Default: topic.partition.offset
  * Valid Values: [none, record.key, topic.partition.offset]
  * Importance: low

``schema.ignore``
  Whether to ignore schemas during indexing. When this is set to ``true``, the record schema will be ignored for the purpose of registering an OpenSearch mapping. OpenSearch will infer the mapping from the data (dynamic mapping needs to be enabled by the user).

   Note that this is a global config that applies to all topics, use ``topic.schema.ignore`` to override as ``true`` for specific topics.

  * Type: boolean
  * Default: false
  * Importance: low

``compact.map.entries``
  Defines how map entries with string keys within record values should be written to JSON. When this is set to ``true``, these entries are written compactly as ``"entryKey": "entryValue"``. Otherwise, map entries with string keys are written as a nested document ``{"key": "entryKey", "value": "entryValue"}``. All map entries with non-string keys are always written as nested documents. Prior to 3.3.0, this connector always wrote map entries as nested documents, so set this to ``false`` to use that older behavior.

  * Type: boolean
  * Default: true
  * Importance: low

``topic.key.ignore``
  List of topics for which ``key.ignore`` should be ``true``.

  * Type: list
  * Default: ""
  * Importance: low

``topic.schema.ignore``
  List of topics for which ``schema.ignore`` should be ``true``.

  * Type: list
  * Default: ""
  * Importance: low

``drop.invalid.message``
  Whether to drop kafka message when it cannot be converted to output message.

  * Type: boolean
  * Default: false
  * Importance: low

``behavior.on.null.values``
  How to handle records with a non-null key and a null value (i.e. Kafka tombstone records). Valid options are ``ignore``, ``delete``, and ``fail``.

  * Type: string
  * Default: ignore
  * Valid Values: [ignore, delete, fail]
  * Importance: low

``behavior.on.malformed.documents``
  How to handle records that OpenSearch rejects due to some malformation of the document itself, such as an index mapping conflict or a field name containing illegal characters. 

  Valid options are:

  - ``ignore`` - do not index the record

  - ``warn`` - log a warning message and do not index the record

  - ``report`` - report to errant record reporter and do not index the record

  - ``fail`` - fail the task.

  * Type: string
  * Default: fail
  * Valid Values: [ignore, warn, fail, report]
  * Importance: low

``behavior.on.version.conflict``
  How to handle records that OpenSearch rejects due to document's version conflicts.

  It may happen when offsets were not committed or/and records have to be reprocessed.

  Valid options are:

  - ``ignore`` - ignore and keep the existing record

  - ``warn`` - log a warning message and keep the existing record

  - ``report`` - report to errant record reporter and keep the existing record

  - ``fail`` - fail the task.

  * Type: string
  * Default: fail
  * Valid Values: [ignore, warn, fail, report]
  * Importance: low

Data Stream
^^^^^^^^^^^

``data.stream.enabled``
  Enable use of data streams. If set to true the connector will write to data streams instead of regular indices. Default is false.

  * Type: boolean
  * Default: false
  * Importance: medium

``data.stream.prefix``
  Generic data stream name to write into. If set, it will be used to construct the final data stream name in the form of {data.stream.prefix}-{topic}.

  * Type: string
  * Default: null
  * Valid Values: non-empty string
  * Importance: medium

``data.stream.timestamp.field``
  The Kafka record field to use as the timestamp for the @timestamp field in documents sent to a data stream. The default is @timestamp.

  * Type: string
  * Default: @timestamp
  * Valid Values: non-empty string
  * Importance: medium

``data.streams.existing.index.template.name``
  If data.streams.create.index.template is provided, data stream and index template will be created f it doesn't exist.

  * Type: string
  * Default: null
  * Importance: medium

Authentication
^^^^^^^^^^^^^^

``connection.username``
  The username used to authenticate with OpenSearch. The default is the null, and authentication will only be performed if  both the username and password are non-null.

  * Type: string
  * Default: null
  * Importance: medium

``connection.password``
  The password used to authenticate with OpenSearch. The default is the null, and authentication will only be performed if  both the username and password are non-null.

  * Type: password
  * Default: null
  * Importance: medium


