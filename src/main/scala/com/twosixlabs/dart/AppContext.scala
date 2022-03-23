package com.twosixlabs.dart

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.sun.java.accessibility.util.Translator
import com.twosixlabs.dart.arangodb.{Arango, ArangoConf}
import com.twosixlabs.dart.datastore.{ArangoDartDatastore, DartDatastore}
import com.twosixlabs.dart.ontologies.OntologyRegistryService
import com.twosixlabs.dart.ontologies.dao.sql.PgSlickProfile.api.Database
import com.twosixlabs.dart.ontologies.dao.sql.SqlOntologyArtifactTable
import com.twosixlabs.dart.operations.status.client.{PipelineStatusUpdateClient, SqlPipelineStatusUpdateClient}
import com.twosixlabs.dart.service.annotator.AnnotatorDef
import com.twosixlabs.dart.service.dedup.Deduplication
import com.twosixlabs.dart.service.{LanguageDetector, NoOpLanguageDetector, Preprocessor, SearchIndex}
import com.typesafe.config.Config
import okhttp3.OkHttpClient

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

object AppContext {

    def apply( config : Config, flags : CliFlags ) : AppContext = {

        val cdrDatastore = initPrimaryDatastore( config, flags )

        val kafkaProvider : KafkaProvider = new KafkaProvider( config.getConfig( "kafka" ) )

        val httpClient : OkHttpClient = HttpClientFactory.newClient( config.getConfig( "http-client" ) )

        val sqlClientProvider : SqlClientProvider = SqlClientProvider( flags.sqlDatastoreMode, config.getConfig( "sql-conf" ) )

        val slickDb = initSlick( flags.sqlDatastoreMode, config )

        val searchIndexProvider : SearchIndexProvider = {
            val searchConfig = config.getConfig( "search-indexer" )
            val host = searchConfig.getString( "host" )
            val port = searchConfig.getInt( "port" )
            new SearchIndexProvider( host, port )
        }

        val preprocessors = initPreprocessors( config )

        val annotatorDefs = initAnnotationDefs( config )

        val languageDetector = new NoOpLanguageDetector

        val enableAnnotators = config.getBoolean( "annotators.enabled" )

        val dedup = initDedup( config )

        AppContext( cdrDatastore,
                    annotatorDefs,
                    preprocessors,
                    sqlClientProvider,
                    slickDb,
                    kafkaProvider,
                    httpClient,
                    searchIndexProvider,
                    languageDetector,
                    enableAnnotators,
                    dedup,
                    config )
    }

    private def initDedup( conf : Config ) : Deduplication = Deduplication.fromConfig( conf.getConfig( "dedup" ) )

    private def initSlick( mode : String, config : Config ) : Database = {
        val datasource = new ComboPooledDataSource()
        mode match {
            case "postgresql" => {
                datasource.setDriverClass( "org.postgresql.Driver" )
                val pgHost = config.getString( "sql-conf.postgresql.host" )
                val pgPort = config.getInt( "sql-conf.postgresql.port" )
                val pgDb = config.getString( "sql-conf.postgresql.name" )
                datasource.setJdbcUrl( s"jdbc:postgresql://$pgHost:$pgPort/$pgDb" )
                datasource.setUser( config.getString( "sql-conf.postgresql.user" ) )
                datasource.setPassword( config.getString( "sql-conf.postgresql.password" ) )
            }
            case "h2" => {
                datasource.setDriverClass( "org.h2.Driver" )
                datasource.setJdbcUrl( s"jdbc:h2:mem:${config.getString( "sql-conf.h2.name" )};MODE=PostgreSQL;DB_CLOSE_DELAY=-1" )
            }
            case other => throw new IllegalArgumentException( s"${other} is not a supported SQL database" )
        }

        Database.forDataSource( datasource, maxConnections = Some( 15 ) )
    }


    private def initAnnotationDefs( config : Config ) : Set[ AnnotatorDef ] = {
        val all = AnnotatorDef.parseConfig( config.getConfig( "annotators" ) )
        if ( System.getenv( "ENABLED_ANNOTATORS" ) != null ) {
            val enabled = System.getenv( "ENABLED_ANNOTATORS" ).trim().split( "," ).toSet
            if ( enabled.nonEmpty ) all.filter( ann => enabled.contains( ann.label ) )
            else all
        }
        else all
    }

    private def initPrimaryDatastore( config : Config, flags : CliFlags ) : DartDatastore = {
        flags.primaryDatastoreMode.toLowerCase match {
            case "arangodb" => {
                val conf : Config = config.getConfig( "cdr-datastore.arangodb" )
                val arangoConf : ArangoConf = {
                    ArangoConf( host = conf.getString( "host" ),
                                port = conf.getString( "port" ).toInt,
                                database = conf.getString( "database" ),
                                connectionPoolSize = conf.getInt( "connection.pool" ) )
                }
                val arango : Arango = new Arango( arangoConf )
                ArangoDartDatastore( arango )
            }
            case other => throw new IllegalArgumentException( s"${other} is not a supported primary datastore for DART" )
        }
    }

    def initPreprocessors( config : Config ) : Seq[ Preprocessor ] = {
        config
          .getList( "preprocessors" )
          .unwrapped().asScala
          .map {
              case other => throw new IllegalArgumentException( s"${other} is not a known preprocessor" )
          }
    }
}

case class AppContext( cdrDatastore : DartDatastore,
                       annotators : Set[ AnnotatorDef ],
                       preprocessors : Seq[ Preprocessor ],
                       sqlClientProvider : SqlClientProvider,
                       slickDb : Database,
                       kafkaProvider : KafkaProvider,
                       httpClient : OkHttpClient,
                       searchIndexProvider : SearchIndexProvider,
                       languageDetector : LanguageDetector,
                       enableAnnotators : Boolean,
                       deduplication : Deduplication,
                       config : Config ) {

    private val OPS_TABLE : String = "pipeline_status"

    private val _searchIndex : SearchIndex = searchIndexProvider.newClient()

    val ontologyRegistry : OntologyRegistryService = new OntologyRegistryService( new SqlOntologyArtifactTable( slickDb, Duration.Inf, scala.concurrent.ExecutionContext.global ) )

    val opsDatastore : PipelineStatusUpdateClient = new SqlPipelineStatusUpdateClient( sqlClientProvider.newClient(), OPS_TABLE )

    val searchIndex : SearchIndex = _searchIndex

    val translator : Translator = null

    val annotatorFactory : AnnotatorFactory = new AnnotatorFactory( this.httpClient )

}
