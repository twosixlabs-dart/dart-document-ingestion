# DART Document Processing Pipeline

## Overview

This application is the primary document ingestion pipeline for the DART platform. Documents are ingested via a Kafka topic and then sent through several processing stages. This processing includes checks for duplicated documents in the corpus as well as normalizing and reparing common text extraction errors.

After the initial ingestion, the pipeline sends the document through the annotation pipeline. Annotators enrich the document with additional natural language metadata extracted from the text. Examples of this include Named Entity Recognition (NER), topic modeling, and sentiment analysis. There are a number of pre-built annotators originally developed by Qntfy, and it is also possible to implement your own annotator.

## Building
This project is built using SBT. For more information on installation and configuration of SBT please [see their documentation](https://www.scala-sbt.org/1.x/docs/)

The DART application itself is composed of other DART libraries that are included as Java (JAR) dependencies. As a prerequisite these libraries (under the `com.twosixlabs.dart`) namespace must be available either in the build machine's local cache or available through a networked artifact repository such as [Sonatype Nexus](https://www.sonatype.com/products/repository-oss-download) or (JFrog Artifactory)[https://jfrog.com/artifactory/].

To build and test the code:
```bash
sbt clean test
````

To create a runnable JAR:
```bash
sbt clean package
```

To create a Docker image of the runnable application:
```bash
make docker-build
```

## Runtime Configuration
```yaml
document-processing-pipeline:
    image: docker.causeex.com/dart/dart-3:latest
    environment:
      PROGRAM_ARGS: "--primary-ds arangodb --sql-ds postgresql"
      POLL_SIZE: 200
      ARANGODB_HOST: dart.arangodb.com
      ARANGODB_PORT: 8529
      KAFKA_BOOTSTRAP_SERVERS: dart.kafka.com:19092
      STREAMING_INTERNAL_IP: 127.0.0.0.0
      STREAM_ANNOTATION_UPDATES: "true"
      ANNOTATOR_HOST: dart.analytics.com
      DEDUP_HOST: 127.0.0.0.0
      DEDUP_PORT: 50000
      SEARCH_HOST: 127.0.0.0.0
      SEARCH_PORT: 9200
      SQL_DB_ENGINE: postgresql
      SQL_DB_HOST: 127.0.0.0.0
      SQL_DB_PORT: 5432
      SQL_DB_USER: dart
      SQL_DB_PASSWORD: mysecretpassword
      SQL_DB_NAME: dart_db
      ANNOTATORS_ENABLED: "true"
    restart: unless-stopped
    depends_on:
      - kafka-broker-1
      - topic-provisioner
    network_mode: host
```

## Environment Variables
| Name                     | Description                                                                    | Example Values            |
|--------------------------|--------------------------------------------------------------------------------|---------------------------|
| KAFKA_BOOTSTRAP_SERVERS  | Connection string to the Kafka broker                                          | `dart.kafka.com:19092`    |
| POLL_SIZE                | Integer value for how many records are included with each Kafka pull operation | `10`                      |
| ANNOTATORS_ENABLED       | Toggles if the pipeline will run annotators                                    | `"true"` or `"false"`     |
| ANNOTATOR_HOST           | Hostname that annotator services are hosted on                                 | `annotators.wm.com`       |
| STREAM_ANNOTATOR_UPDATES | Toggles whether annotations are published to the update stream                 | `"true"` or `"false"`     |
| SQL_DB_HOST              | host of the operations database                                                | `dart.postgres.com`       |
| SQL_DB_PORT              | port for the operation database                                                | `5432`                    |
| SQL_DB_USER              | user of the operations database                                                | `dart`                    |
| SQL_DB_PASSWORD          | password of the operations database                                            | `my_pwd`                  |
| SQL_DB_NAME              | username of the operations database                                            | `dart`                    |
| ARANGODB_DATABASE        | namespace of the canonical document database                                   | `dart`                    |
| ARANGODB_HOST            | host of the canonical database                                                 | `dart.arangodb.com`       |
| ARANGODB_PORT            | port of the canonical database                                                 | `8529`                    |
| ARANGODB_CONNECTION_POOL | size of the connection pool for ArangoDB                                       | `5`                       |
| SEARCH_HOST              | hostname of the search index                                                   | `dart.elasticsearch.comd` |
| SEARCH_PORT              | port of the search index                                                       | `9200`                    |
| DEDUP_HOST               | hostname of the deduplication service                                          | `dart.dedup.com`          |
| DEDUP_PORT               | hostname of the deduplication service                                          | `50000`                   |