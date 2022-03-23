import akka.actor.typed.ActorSystem
import com.twosixlabs.dart.notifications.Notifications
import com.twosixlabs.dart.pipeline.DART
import com.twosixlabs.dart.pipeline.command.{DocumentProcessing, IngestProxy}
import com.twosixlabs.dart.serialization.json.JsonDataFormats.INGEST_PROXY_JSON
import com.twosixlabs.dart.{AppContext, CliFlags}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.{Logger, LoggerFactory}
import scopt.OParser

import java.time.Duration
import scala.collection.JavaConverters._

object Main {

    // prevent chronicle map from sending analytics https://github.com/reynoldsm88/dedup#disclaimer
    System.setProperty( "chronicle.announcer.disable", "true" )

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    def main( args : Array[ String ] ) : Unit = {
        val flags : CliFlags = processCli( args ) match {
            case Some( config ) => config
            case None => throw new IllegalStateException( "unable to process cli args" )
        }

        val akkaConfig : Config = ConfigFactory.load()

        LOG.info( s"starting DART using ${flags.environment} configuration file..." )
        val appConfig : Config = ConfigFactory.parseResources( s"env/${flags.environment}.conf" ).resolve()
        val appContext : AppContext = AppContext( appConfig, flags )

        implicit val dartSystem : ActorSystem[ DocumentProcessing ] = ActorSystem( DART( appContext ), "dart", akkaConfig )

        val ingestProxyConsumer = appContext.kafkaProvider.newConsumer[ String, String ]( Notifications.INGEST_PROXY_TOPIC, appConfig.getConfig( "ingest-proxy.kafka" ) )

        while ( true ) {
            val messages : Iterable[ ConsumerRecord[ String, String ] ] = ingestProxyConsumer.poll( Duration.ofMillis( 500 ) ).asScala

            messages.foreach( input => {
                val docId = input.key()
                INGEST_PROXY_JSON.fromJson( input.value() ) match {
                    case Some( event ) => {
                        dartSystem ! IngestProxy( docId, event )
                    }
                    case None => LOG.error( s"the following event data was invalid ${input.value()}" )
                }
            } )
        }
    }

    private def processCli( args : Array[ String ] ) : Option[ CliFlags ] = {
        val builder = OParser.builder[ CliFlags ]

        //@formatter:off
        val parser = {
            import builder._
            OParser.sequence(
                opt[ String ]( "env" )
                  .optional()
                  .valueName( "storage mode for the primary document store" )
                  .action( ( mode, config ) => config.copy( environment = mode) ),
                opt[ String ]( "primary-ds" )
                  .optional()
                  .valueName( "storage mode for the primary document store" )
                  .action( ( mode, config ) => config.copy( primaryDatastoreMode = mode) ),
                opt[ String ]( "sql-ds" )
                  .optional()
                  .valueName( "storage mode to use for the operational SQL datastore" )
                  .action( ( mode, config ) => config.copy( sqlDatastoreMode = mode ) ),
            )
        }
        //@formatter:on
        OParser.parse( parser, args, CliFlags() ) match {
            case config@Some( _ ) => config
            case None => None
        }
    }

}
