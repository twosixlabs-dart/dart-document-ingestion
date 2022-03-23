package com.twosixlabs.dart.serialization.json

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty, JsonRawValue}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.twosixlabs.dart.json.JsonFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.beans.BeanProperty

@JsonInclude( Include.NON_EMPTY )
@JsonIgnoreProperties( ignoreUnknown = true )
case class IngestProxyEvent( @BeanProperty @JsonProperty( value = "metadata", required = false ) metadata : IngestProxyEventMetadata,
                             @JsonRawValue @BeanProperty @JsonProperty( value = "document", required = true ) document : ObjectNode ) {

    def documentJson : String = document.toString
}

@JsonInclude( Include.ALWAYS )
@JsonIgnoreProperties( ignoreUnknown = true )
case class IngestProxyEventMetadata( @BeanProperty @JsonProperty( "tenants" ) tenants : Set[ String ] = Set(),
                                     @BeanProperty @JsonProperty( "source_uri" ) sourceUri : Option[ String ] = None,
                                     @BeanProperty @JsonProperty( "labels" ) labels : Set[ String ] = Set(),
                                     @BeanProperty @JsonProperty( "genre" ) genre : String = null,
                                     @BeanProperty @JsonProperty( "ingestion_system" ) ingestionSystem : String = null,
                                     @BeanProperty @JsonProperty( "reannotate" ) reannotate : Boolean = false )


class IngestProxyEventJsonFormat extends JsonFormat {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    def toJson( record : IngestProxyEvent ) : Option[ String ] = {
        try Some( objectMapper.writeValueAsString( record ) )
        catch {
            case e : Exception => {
                LOG.error( s"Error marshalling Ingestion Record : ${e.getClass.getSimpleName} : ${e.getMessage}" )
                LOG.debug( s"Error root cause : ${e.getCause}" )
                e.printStackTrace()
                None
            }
        }

    }

    def fromJson( ingestionRecord : String ) : Option[ IngestProxyEvent ] = {
        try {
            Some( objectMapper.readValue( ingestionRecord, classOf[ IngestProxyEvent ] ) )
        } catch {
            case e : Exception => {
                LOG.error( s"Error unmarshalling Ingestion Record : ${e.getClass.getSimpleName} : ${e.getMessage}" )
                LOG.debug( s"Error root cause : ${e.getCause}" )
                e.printStackTrace()
                None
            }
        }
    }

    def metadataToJson( metadata : IngestProxyEventMetadata ) : Option[ String ] = {
        try Some( objectMapper.writeValueAsString( metadata ) )
        catch {
            case e : Exception => {
                LOG.error( s"Error marshalling Ingestion Metadata : ${e.getClass.getSimpleName} : ${e.getMessage}" )
                LOG.debug( s"Error root cause : ${e.getCause}" )
                e.printStackTrace()
                None
            }
        }
    }

    def metadataFromJson( ingestionRecord : String ) : Option[ IngestProxyEventMetadata ] = {
        try {
            Some( objectMapper.readValue( ingestionRecord, classOf[ IngestProxyEventMetadata ] ) )
        } catch {
            case e : Exception => {
                LOG.error( s"Error unmarshalling Document Exchange : ${e.getClass.getSimpleName} : ${e.getMessage}" )
                LOG.debug( s"Error root cause : ${e.getCause}" )
                e.printStackTrace()
                None
            }
        }
    }
}
