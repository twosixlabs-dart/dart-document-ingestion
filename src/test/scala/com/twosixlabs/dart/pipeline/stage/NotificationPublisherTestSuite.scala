//package com.twosixlabs.dart.pipeline.stage
//
//import com.twosixlabs.dart.pipeline.command.{NotifyError, NotifyUpdate}
//import com.twosixlabs.dart.serialization.json.JsonDataFormats.DART_JSON
//import com.twosixlabs.dart.test.base.AkkaTestBase
//import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE
//import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
//import org.apache.kafka.clients.producer.KafkaProducer
//import org.apache.kafka.common.serialization.{Deserializer, Serdes, Serializer}
//
//import java.util.Properties
//
//class NotificationPublisherTestSuite extends AkkaTestBase with EmbeddedKafka {
//
//    private implicit val EMBEDDED_KAFKA_CONFIG = EmbeddedKafkaConfig( kafkaPort = 6310, zooKeeperPort = 6311 )
//
//    private implicit val serializer : Serializer[ String ] = Serdes.String.serializer()
//    private implicit val deserializer : Deserializer[ String ] = Serdes.String.deserializer()
//
//    "Notification Publisher" should "send a notification to the specified topic" in {
//        val doc = CDR_TEMPLATE.copy( documentId = "1a" )
//        val props : Properties = {
//            val p = new Properties()
//            p.setProperty( "bootstrap.servers", "localhost:6310" )
//            p.setProperty( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" )
//            p.setProperty( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" )
//            p
//        }
//        val producer : KafkaProducer[ String, String ] = new KafkaProducer[ String, String ]( props )
//
//        val kafkaPublisher = testKit.spawn( NotificationPublisher( producer ) )
//        val expectedJson = DART_JSON.marshalThinCdr( doc.makeThin ).get
//        //@formatter:off
//        withRunningKafka {
//
//            kafkaPublisher ! NotifyUpdate( doc, "test.publisher.topic" )
//
//            val result = consumeFirstKeyedMessageFrom( "test.publisher.topic" )
//            result._1 shouldBe "1a"
//            result._2 shouldBe expectedJson
//        }
//        //@formatter:on
//    }
//
//    "Notification Publisher" should "send an error to the specified topic" in {
//        val props : Properties = {
//            val p = new Properties()
//            p.setProperty( "bootstrap.servers", "localhost:6310" )
//            p.setProperty( "key.serializer", "org.apache.kafka.common.serialization.StringSerializer" )
//            p.setProperty( "value.serializer", "org.apache.kafka.common.serialization.StringSerializer" )
//            p
//        }
//
//
//        val producer : KafkaProducer[ String, String ] = new KafkaProducer[ String, String ]( props )
//
//        val kafkaPublisher = testKit.spawn( NotificationPublisher( producer ) )
//
//        //@formatter:off
//        withRunningKafka {
//
//            kafkaPublisher ! NotifyError( "1a", "test.publisher.dlq.topic", "core-processing", "test error", new Exception() )
//
//            val result = consumeFirstKeyedMessageFrom( "test.publisher.dlq.topic" )
//            result._1 shouldBe "1a"
//            result._2 should startWith( s"DlqWrapper(" )
//        }
//        //@formatter:on
//    }
//}
