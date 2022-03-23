package com.twosixlabs.dart

import com.twosixlabs.dart.sql.SqlClient
import com.typesafe.config.Config

import java.util.Properties
import scala.util.control.Exception.allCatch

object SqlClientProvider {
    def apply( mode : String, config : Config ) : SqlClientProvider = {

        val sqlConf : Config = config.getConfig( mode )

        val engine = sqlConf.getString( "engine" )
        val name : String = sqlConf.getString( "name" )
        val host : Option[ String ] = allCatch.opt( sqlConf.getString( "host" ) )
        val port : Option[ Int ] = allCatch.opt( sqlConf.getInt( "port" ) )
        val user : Option[ String ] = allCatch.opt( sqlConf.getString( "user" ) )
        val pass : Option[ String ] = allCatch.opt( sqlConf.getString( "password" ) )

        new SqlClientProvider( engine, name, host, port, user, pass )
    }
}

class SqlClientProvider( engine : String,
                         name : String,
                         host : Option[ String ],
                         port : Option[ Int ],
                         user : Option[ String ],
                         password : Option[ String ] ) {

    def newClient( ) : SqlClient = {
        engine match {
            case "h2" => SqlClient.newClient( engine, name, null, -1, None, None )
            case "postgresql" => {
                val props : Map[ String, String ] = Map( "sql.db.engine" -> engine,
                                                         "sql.db.name" -> name,
                                                         "sql.db.host" -> host.get,
                                                         "sql.db.port" -> port.get.toString,
                                                         "sql.db.user" -> user.get,
                                                         "sql.db.password" -> password.get,
                                                         "connection.pool.min.size" -> "10",
                                                         "connection.pool.max.size" -> "100" )

                val p = new Properties()
                props.foreach( x => p.setProperty( x._1, x._2 ) )

                SqlClient.newClient( p )
            }
            case other => throw new IllegalArgumentException( s"${other} is not a supported SQL engine type" )
        }
    }
}
