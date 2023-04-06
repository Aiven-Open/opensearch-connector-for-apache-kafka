# Aiven's OpenSearch® Connector for Apache Kafka®

[![Build Status](https://github.com/aiven/opensearch-connector-for-apache-kafka/actions/workflows/master_push_workflow.yml/badge.svg)](https://github.com/aiven/opensearch-connector-for-apache-kafka/actions)

This repository includes Aiven's OpenSearch [Apache Kafka® Connector](http://kafka.apache.org/documentation.html#connect) for Apache
Kafka®.

The project originates from
Aiven's [elasticsearch-connector-for-apache-kafka](https://github.com/aiven/elasticsearch-connector-for-apache-kafka).
The code was forked and all classes were renamed.

# Documentation

## How to install

1. Connector plugins are packaged in zip/tar format to be released
2. Users download plugins from GitHub releases or build binaries from source
3. Users place connector plugins on Connect worker instances and add them via configuration
4. Start creating connectors using installed plugins

### Download binaries

Binaries are included on every release as zip/tar files: https://github.com/aiven/opensearch-connector-for-apache-kafka/releases/latest

### Build from Source

Execute gradle task to build binaries:

```shell
./gradlew installDist
# or ./gradlew assembleDist to package binaries
```

This produces an output on `build/install` directory with the plugin binaries to add into Connect cluster.

### Add plugin to Connect worker

Place unpacked binaries into a directory on each Connect worker node, e.g. `/kafka-connect-plugins`.

In this case, place `opensearch-connector-for-kafka` into `/kafka-connect-plugins`:

```
/kafka-connect-plugins
└── opensearch-connector-for-apache-kafka
```

Then, on each connect worker configuration make sure to add `/kafka-connect-plugins` to the `plugin.path` configuration:

```properties
plugin.path=/kafka-connect-plugins
```

### Validate Connector plugin installation

Once placed on each worker node, start the workers and check the plugins installed
and check the plugin (with the correct version) is included:

```shell
# Go to connector rest api
curl http://localhost:8083/connector-plugins | jq .
```
```json
[
  ...
  {
    "class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
    "type": "sink",
    "version": "3.0.0"
  },
  ...
]
```

## Connector Configuration

[OpenSearch® Sink Connector Configuration Options](docs/opensearch-sink-connector-config-options.rst)

# Contribute

[Source Code](https://github.com/aiven/aiven-kafka-connect-opensearch)

[Issue Tracker](https://github.com/aiven/aiven-kafka-connect-opensearch/issues)

# License

The project is licensed under the [Apache 2 license](https://www.apache.org/licenses/LICENSE-2.0).
See [LICENSE](LICENSE).

# Trademark

Apache Kafka, Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.

OpenSearch is a trademark and property of its respective owners. All product and service names used in this website are for identification purposes only and do not imply endorsement.
