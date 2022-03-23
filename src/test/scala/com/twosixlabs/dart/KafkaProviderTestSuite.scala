package com.twosixlabs.dart

import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.TEST_CONFIG
import com.typesafe.config.{Config, ConfigValueFactory}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serdes, Serializer}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class KafkaProviderTestSuite extends StandardTestBase3x with EmbeddedKafka {

    implicit val executionContext : ExecutionContext = scala.concurrent.ExecutionContext.global

    private implicit val EMBEDDED_KAFKA_CONFIG = EmbeddedKafkaConfig( kafkaPort = 6308, zooKeeperPort = 6309 )

    private implicit val serializer : Serializer[ String ] = Serdes.String.serializer()
    private implicit val deserializer : Deserializer[ String ] = Serdes.String.deserializer()

    // need to override the kafka URL based on the port used by the test...
    private val KAFKA_TEST_CONFIG : Config = TEST_CONFIG.getConfig( "kafka" ).withValue( "bootstrap.servers", ConfigValueFactory.fromAnyRef( "localhost:6308" ) )

    private val provider : KafkaProvider = new KafkaProvider( KAFKA_TEST_CONFIG )

    "Kafka Provider" should "create a Consumer with specified properties" in { // TODO - testing consumers is a PITA for some reason
        //@formatter:off
        withRunningKafka {
            val consumer : KafkaConsumer[ String, String ] = provider.newConsumer[ String, String ]( "test.consumer.topic", TEST_CONFIG.getConfig( "ingest-proxy" ) )
            consumer.subscription() shouldBe Set( "test.consumer.topic" ).asJava
        }
        //@formatter:on
    }

    "Kafka Producer" should "create a Producer with the specified properties" in {

        //@formatter:off
        withRunningKafka {
            val producer : KafkaProducer[ String, String ] = provider.newProducer[ String, String ]( TEST_CONFIG.getConfig( "notifications" ) )

            producer.send( new ProducerRecord[ String, String ]( "test.producer.topic", "key", "value" ) )

            val result = consumeFirstKeyedMessageFrom( "test.producer.topic" )
            result._1 shouldBe "key"
            result._2 shouldBe "value"
        }
        //@formatter:on
    }
}
