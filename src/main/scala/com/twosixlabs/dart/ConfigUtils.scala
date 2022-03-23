package com.twosixlabs.dart

import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.mutable

object ConfigUtils {

    def configToMap( config : Config ) : Map[ String, String ] = {
        config
          .entrySet()
          .asScala
          .map( pair => (pair.getKey, pair.getValue.unwrapped().toString) )
          .toMap
    }

    def overlay( source : Map[ String, String ], target : Map[ String, String ] ) : Map[ String, String ] = {
        val results : mutable.Map[ String, String ] = mutable.Map() ++= source
        target.foreach( kv => results += ( kv._1 -> kv._2 ) )

        results.toMap
    }
}
