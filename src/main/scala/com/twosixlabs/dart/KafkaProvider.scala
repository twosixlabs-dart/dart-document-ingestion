package com.twosixlabs.dart

import com.twosixlabs.dart.ConfigUtils._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class KafkaProvider( config : Config ) {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private val KAFKA_DEFAULTS : Map[ String, String ] = configToMap( config )

    def newConsumer[ Key, Value ]( topic : String, config : Config ) : KafkaConsumer[ Key, Value ] = {

        val consumerProps : Map[ String, String ] = createKafkaConfig( configToMap( config ) )
        if ( LOG.isDebugEnabled ) logConsumerInfo( consumerProps )

        val consumer = new KafkaConsumer[ Key, Value ]( consumerProps.asJava.asInstanceOf[ java.util.Map[ String, AnyRef ] ] )
        consumer.subscribe( List( topic ).asJava )
        consumer
    }

    def newProducer[ Key, Value ]( config : Config ) : KafkaProducer[ Key, Value ] = {
        val consumerProps : Map[ String, String ] = createKafkaConfig( configToMap( config ) )
        if ( LOG.isDebugEnabled ) logConsumerInfo( consumerProps )

        new KafkaProducer[ Key, Value ]( consumerProps.asJava.asInstanceOf[ java.util.Map[ String, AnyRef ] ] )
    }

    private def createKafkaConfig( props : Map[ String, String ] ) : Map[ String, String ] = overlay( KAFKA_DEFAULTS, props )

    private def logConsumerInfo( props : Map[ String, String ] ) : Unit = {
        LOG.debug( "using the following consumer properties..." )
        props.foreach( kv => LOG.debug( s"${kv._1} = ${kv._2}" ) )
    }

}
