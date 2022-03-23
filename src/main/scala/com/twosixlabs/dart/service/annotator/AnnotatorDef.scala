package com.twosixlabs.dart.service.annotator

import com.typesafe.config.Config

import scala.collection.JavaConverters._

object AnnotatorDef {

    def parseConfig( config : Config ) : Set[ AnnotatorDef ] = {
        val globalAnnotatorHost : Option[ String ] = if ( config.hasPath( "host" ) ) Some( config.getString( "host" ) ) else None

        val retryCount : Int = config.getInt( "retry.count" )
        val retryPauseMillis : Long = config.getLong( "retry.pause.millis" )

        config.getConfigList( "defs" ).asScala.map( definition => {
            val name = definition.getString( "name" )
            val host = {
                if ( definition.hasPath( "host" ) ) definition.getString( "host" )
                else {
                    if ( globalAnnotatorHost.nonEmpty ) globalAnnotatorHost.get
                    else throw new IllegalStateException( s"no host was specified for annotator: ${name}" )
                }
            }
            val port = definition.getInt( "port" )
            val path = definition.getString( "path" )
            val doPostprocessing : Boolean = definition.getBoolean( "post.process" )
            AnnotatorDef( name, host, port, doPostprocessing, path, retryCount, retryPauseMillis )
        } ).toSet
    }

}

case class AnnotatorDef( label : String,
                         host : String,
                         port : Int,
                         postProcess : Boolean,
                         path : String,
                         retryCount : Int,
                         retryPauseMillis : Long )
