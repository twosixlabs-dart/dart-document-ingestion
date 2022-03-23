
package com.twosixlabs.dart

import com.typesafe.config.Config
import okhttp3.{ConnectionPool, OkHttpClient}

import java.util.concurrent.TimeUnit

object HttpClientFactory {

    val DEFAULT_TIMEOUT : Long = 2000

    def newClient( config : Config ) : OkHttpClient = {
        // this is experimental for now, will move to config if it is helpful
        val connectionPool = new ConnectionPool( 1000, 5, TimeUnit.MINUTES )


        val connectTimout : Long = if ( config.hasPath( "connect.timeout.ms" ) ) config.getLong( "connect.timeout.ms" ) else DEFAULT_TIMEOUT
        val callTimeout : Long = if ( config.hasPath( "call.timeout.ms" ) ) config.getLong( "call.timeout.ms" ) else DEFAULT_TIMEOUT
        val readTimeout : Long = if ( config.hasPath( "read.timeout.ms" ) ) config.getLong( "read.timeout.ms" ) else DEFAULT_TIMEOUT
        val writeTimeout : Long = if ( config.hasPath( "write.timeout.ms" ) ) config.getLong( "write.timeout.ms" ) else DEFAULT_TIMEOUT

        new OkHttpClient.Builder()
          .connectionPool( connectionPool )
          .connectTimeout( connectTimout, TimeUnit.MILLISECONDS )
          .callTimeout( callTimeout, TimeUnit.MILLISECONDS )
          .readTimeout( readTimeout, TimeUnit.MILLISECONDS )
          .writeTimeout( writeTimeout, TimeUnit.MILLISECONDS )
          .build()
    }

}
