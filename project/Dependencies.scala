import sbt._

object Dependencies {

    val slf4jVersion = "1.7.20"
    val logbackVersion = "1.2.9"

    val java8CompatVersion = "0.9.1"

    val betterFilesVersion = "3.8.0"

    val cdr4sVersion = "3.0.9"
    val dartCommonsVersion = "3.0.30"
    val dartTextUtilsVersion = "3.0.5"
    val linguaVersion = "0.6.1"
    val arangoDatastoreVersion = "3.0.8"
    val operationsVersion = "3.0.14"
    val ontologyRegistryVersion = "3.0.6"

    val akkaVersion = "2.6.9"

    val scalaTestVersion = "3.1.4"
    val scalaMockVersion = "4.2.0"

    val scoptVersion = "4.0.0-RC2"

    val okhttpVersion = "4.1.0"
    val elastic4sVersion = "7.13.0"
    val embeddedEsVersion = "7.6.2"

    val kafkaVersion = "2.2.1"
    val embeddedKafkaVersion = "3.1.0"

    val rdf4jVersion = "2.5.1"

    val mockitoVersion = "1.16.0"

    val dedupVersion = "1.0.3"

    val kafka = Seq( "org.apache.kafka" %% "kafka" % kafkaVersion,
                     "org.apache.kafka" % "kafka-clients" % kafkaVersion )

    val akka = Seq( "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
                    "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test )

    val cdr4s = Seq( "com.twosixlabs.cdr4s" %% "cdr4s-core" % cdr4sVersion,
                     "com.twosixlabs.cdr4s" %% "cdr4s-ladle-json" % cdr4sVersion,
                     "com.twosixlabs.cdr4s" %% "cdr4s-dart-json" % cdr4sVersion )

    val arangoDatastoreRepo = Seq( "com.twosixlabs.dart" %% "dart-arangodb-datastore" % arangoDatastoreVersion )

    val dartCommons = Seq( "com.twosixlabs.dart" %% "dart-json" % dartCommonsVersion,
                           "com.twosixlabs.dart" %% "dart-test-base" % dartCommonsVersion % Test )

    val dartTextUtils = Seq( "com.twosixlabs.dart" %% "embedded-text-utils" % dartTextUtilsVersion )

    val operations = Seq( "com.twosixlabs.dart.operations" %% "status-client" % operationsVersion )

    val ontologyRegistry = Seq( "com.twosixlabs.dart.ontologies" %% "ontology-registry-api" % ontologyRegistryVersion,
                                "com.twosixlabs.dart.ontologies" %% "ontology-registry-services" % ontologyRegistryVersion )

    val lingua = Seq( "com.github.pemistahl" % "lingua" % linguaVersion )

    val dedup = Seq( "com.twosixlabs.dart" %% "dedup" % dedupVersion )

    val elastic4s = Seq( "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
                         "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,
                         "com.sksamuel.elastic4s" %% "elastic4s-json-jackson" % elastic4sVersion )

    val scopt = Seq( "com.github.scopt" %% "scopt" % scoptVersion )

    val logging = Seq( "org.slf4j" % "slf4j-api" % slf4jVersion,
                       "ch.qos.logback" % "logback-classic" % logbackVersion )

    val betterFiles = Seq( "com.github.pathikrit" %% "better-files" % betterFilesVersion )

    val okhttp = Seq( "com.squareup.okhttp3" % "okhttp" % okhttpVersion,
                      "com.squareup.okhttp3" % "mockwebserver" % okhttpVersion )

    val unitTesting = Seq( "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
                           "org.mockito" %% "mockito-scala-scalatest" % mockitoVersion % Test )

    val embeddedKafka = Seq( "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion % Test,
                             "io.github.embeddedkafka" %% "embedded-kafka-streams" % embeddedKafkaVersion % Test,
                             "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.2" % Test ) //https://github.com/sbt/sbt/issues/3618

    val java8Compat = Seq( "org.scala-lang.modules" %% "scala-java8-compat" % java8CompatVersion )

    val rdf4j = Seq( "org.eclipse.rdf4j" % "rdf4j-rio-api" % rdf4jVersion,
                     "org.eclipse.rdf4j" % "rdf4j-rio-ntriples" % rdf4jVersion )

}
