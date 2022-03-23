package com.twosixlabs.dart.serialization.json

import better.files.Resource
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty}
import com.twosixlabs.cdr4s.core.{CdrDocument, CdrMetadata}
import com.twosixlabs.dart.json.JsonFormat
import com.twosixlabs.dart.utils.DatesAndTimes.{fromIsoOffsetDateTimeStr, offsetDateTimeFromEpochSec}
import org.everit.json.schema.loader.SchemaLoader
import org.everit.json.schema.{Schema, ValidationException}
import org.json.{JSONObject, JSONTokener}
import org.slf4j.{Logger, LoggerFactory}

import java.time.Instant
import scala.beans.BeanProperty

@JsonInclude( Include.NON_EMPTY )
@JsonIgnoreProperties( ignoreUnknown = true )
case class PulseDocumentJson( @BeanProperty @JsonProperty( value = "norm", required = true ) normalizedData : NormalizedData = null,
                              @BeanProperty @JsonProperty( value = "norm_attribs", required = false ) normalizedDataAttributes : NormalizedDataAttributes = null,
                              @BeanProperty @JsonProperty( value = "meta", required = false ) metadata : Metadata = null )


@JsonInclude( Include.NON_EMPTY )
@JsonIgnoreProperties( ignoreUnknown = true )
case class NormalizedData( @BeanProperty @JsonProperty( value = "body", required = true ) extractedText : String,
                           @BeanProperty @JsonProperty( value = "author", required = false ) author : String = null,
                           @BeanProperty @JsonProperty( value = "url", required = false ) url : String = null,
                           @BeanProperty @JsonProperty( value = "timestamp", required = false ) timestamp : String = null )

@JsonInclude( Include.NON_EMPTY )
@JsonIgnoreProperties( ignoreUnknown = true )
case class NormalizedDataAttributes( @BeanProperty @JsonProperty( value = "type", required = true ) documentSource : String = null )

@JsonInclude( Include.NON_EMPTY )
@JsonIgnoreProperties( ignoreUnknown = true )
case class Metadata( @BeanProperty @JsonProperty( value = "title", required = false ) titleAttributes : Seq[ MetadataAttributes ] = null,
                     @BeanProperty @JsonProperty( value = "author_name", required = false ) authorNameAttributes : Seq[ MetadataAttributes ] = null )

@JsonInclude( Include.NON_EMPTY )
@JsonIgnoreProperties( ignoreUnknown = true )
case class MetadataAttributes( @BeanProperty @JsonProperty( value = "results", required = false ) results : Seq[ String ] = null )


class PulseJsonFormat extends JsonFormat {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private val CLASSIFICATION : String = "UNCLASSIFIED"
    private val PRODUCER : String = "Pulse"
    private val SCHEMA : Schema = {
        val schemaIs = Resource.getAsStream( "pulse/schema.json" )
        val schemaJson = new JSONObject( new JSONTokener( schemaIs ) )
        SchemaLoader.load( schemaJson )
    }

    def validate( pulseDocument : Array[ Byte ] ) : Boolean = {
        try {
            SCHEMA.validate( new JSONObject( new String( pulseDocument ) ) )
            return true
        }
        catch {
            case e : ValidationException => false
        }
    }


    def convertPulseToCdr( documentId : String, json : String ) : Option[ CdrDocument ] = {
        try
            Some( dtoToCdr( documentId, objectMapper.readValue( json, classOf[ PulseDocumentJson ] ) ) )
        catch {
            case e : Exception => {
                LOG.error( s"Error unmarshalling CDR json : ${e.getClass.getSimpleName} : ${e.getMessage}" )
                LOG.error( s"Error root cause : ${e.getCause}" )
                None
            }
        }
    }

    private def dtoToCdr( documentId : String, dto : PulseDocumentJson ) : CdrDocument = {
        val timeStamp = offsetDateTimeFromEpochSec( Instant.now.getEpochSecond )
        val title = if ( null == dto.metadata || null == dto.metadata.titleAttributes || dto.metadata.titleAttributes.isEmpty || dto.metadata.titleAttributes.head.results.isEmpty ) {
            ""
        } else
            dto.metadata.titleAttributes.head.results.head
        val cdrMetadata : CdrMetadata = CdrMetadata( if ( null == dto.normalizedData.timestamp ) null else fromIsoOffsetDateTimeStr( dto.normalizedData.timestamp ).toLocalDate,
                                                     null,
                                                     "",
                                                     dto.normalizedDataAttributes.documentSource,
                                                     "",
                                                     "en",
                                                     CLASSIFICATION,
                                                     title,
                                                     dto.normalizedData.author,
                                                     dto.normalizedData.url,
                                                     None,
                                                     "",
                                                     "",
                                                     PRODUCER )


        CdrDocument( s"pulse_${dto.normalizedDataAttributes.documentSource}",
                     cdrMetadata,
                     dto.normalizedDataAttributes.documentSource,
                     Map.empty,
                     documentId,
                     dto.normalizedData.extractedText,
                     null,
                     Option( dto.normalizedData.url ).getOrElse( "" ),
                     null,
                     timeStamp,
                     List() )

    }


}
