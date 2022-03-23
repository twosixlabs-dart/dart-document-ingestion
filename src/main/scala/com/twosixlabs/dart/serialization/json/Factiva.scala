package com.twosixlabs.dart.serialization.json

import better.files.Resource
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.twosixlabs.cdr4s.annotations.FacetScore
import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument, CdrMetadata, FacetAnnotation}
import com.twosixlabs.dart.json.JsonFormat
import com.twosixlabs.dart.utils.DatesAndTimes.{localDateFromEpochMilliSec, offsetDateTimeFromEpochSec}
import org.everit.json.schema.loader.SchemaLoader
import org.everit.json.schema.{Schema, ValidationException}
import org.json.{JSONObject, JSONTokener}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.CodingErrorAction
import java.time.Instant
import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

//@JsonInclude( Include.NON_EMPTY )
@JsonIgnoreProperties( ignoreUnknown = true )
case class FactivaDocumentJson( @BeanProperty @JsonProperty( "an" ) an : String = null,
                                @BeanProperty @JsonProperty( "ingestion_datetime" ) ingestionDatetime : String = null,
                                @BeanProperty @JsonProperty( "publication_date" ) publicationDate : String = null,
                                @BeanProperty @JsonProperty( "publication_datetime" ) publicationDatetime : String = null,
                                @BeanProperty @JsonProperty( "snippet" ) snippet : String = null,
                                @BeanProperty @JsonProperty( "body" ) body : String = null,
                                @BeanProperty @JsonProperty( "art" ) art : String = null,
                                @BeanProperty @JsonProperty( "action" ) action : String = null,
                                @BeanProperty @JsonProperty( "credit" ) credit : String = null,
                                @BeanProperty @JsonProperty( "byline" ) byline : String = null,
                                @BeanProperty @JsonProperty( "document_type" ) documentType : String = null,
                                @BeanProperty @JsonProperty( "language_code" ) languageCode : String = null,
                                @BeanProperty @JsonProperty( "title" ) title : String = null,
                                @BeanProperty @JsonProperty( "copyright" ) copyright : String = null,
                                @BeanProperty @JsonProperty( "dateline" ) dateline : String = null,
                                @BeanProperty @JsonProperty( "source_code" ) sourceCode : String = null,
                                @BeanProperty @JsonProperty( "modification_datetime" ) modificationDateTime : String = null,
                                @BeanProperty @JsonProperty( "modification_date" ) modificationDate : String = null,
                                @BeanProperty @JsonProperty( "section" ) section : String = null,
                                @BeanProperty @JsonProperty( "company_codes" ) companyCodes : String = null,
                                @BeanProperty @JsonProperty( "publisher_name" ) publisherName : String = null,
                                @BeanProperty @JsonProperty( "region_of_origin" ) regionOfOrigin : String = null,
                                @BeanProperty @JsonProperty( "word_count" ) word_count : String = null,
                                @BeanProperty @JsonProperty( "subject_codes" ) subjectCodes : String = null,
                                @BeanProperty @JsonProperty( "region_codes" ) regionCodes : String = null,
                                @BeanProperty @JsonProperty( "industry_codes" ) industryCodes : String = null,
                                @BeanProperty @JsonProperty( "person_codes" ) personCodes : String = null,
                                @BeanProperty @JsonProperty( "currency_codes" ) currencyCodes : String = null,
                                @BeanProperty @JsonProperty( "market_index_codes" ) marketIndexCodes : String = null,
                                @BeanProperty @JsonProperty( "company_codes_about" ) companyCodesAbout : String = null,
                                @BeanProperty @JsonProperty( "company_codes_association" ) companyCodesAssociation : String = null,
                                @BeanProperty @JsonProperty( "company_codes_lineage" ) companyCodesLineage : String = null,
                                @BeanProperty @JsonProperty( "company_codes_occur" ) companyCodesOccur : String = null,
                                @BeanProperty @JsonProperty( "company_codes_relevance" ) companyCodesRelevance : String = null,
                                @BeanProperty @JsonProperty( "source_name" ) sourceName : String = null )

class FactivaJsonFormat extends JsonFormat {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private val CLASSIFICATION : String = "UNCLASSIFIED"
    private val CAPTURE_SOURCE : String = "AutoCollection"
    private val CONTENT_TYPE : String = "application/json"
    private val PRODUCER : String = "Dow Jones"

    private val SCHEMA : Schema = {
        val schemaIs = Resource.getAsStream( "factiva/schema.json" )
        val schemaJson = new JSONObject( new JSONTokener( schemaIs ) )
        SchemaLoader.load( schemaJson )
    }

    private val INDUSTRIES_CODES : Map[ String, String ] = {
        extractCodesFromResourceFile( "factiva/industry-codes.csv" )
    }

    private val NEWS_SUBJECTS : Map[ String, String ] = {
        extractCodesFromResourceFile( "factiva/subject-codes.csv" )
    }

    private val REGION_CODES : Map[ String, String ] = {
        extractCodesFromResourceFile( "factiva/region-codes.csv" )
    }


    def validate( factivaDocument : Array[ Byte ] ) : Boolean = {
        try {
            SCHEMA.validate( new JSONObject( new String( factivaDocument ) ) )
            return true
        }
        catch {
            case e : ValidationException => {
                LOG.warn( s"Error validating the following content : ${new String( factivaDocument )}" )
                LOG.warn( s"${e.getMessage}" )
                LOG.warn( s"${e.getErrorMessage}" )
                LOG.warn( s"${e.getPointerToViolation}" )
                e.getCausingExceptions.asScala.foreach( e => LOG.warn( e.getAllMessages.asScala.mkString( "\n" ) ) )
                false
            }
        }
    }


    def convertFactivaToCdr( documentId : String, json : String ) : Option[ CdrDocument ] = {
        try
            Some( dtoToCdr( documentId, objectMapper.readValue( json, classOf[ FactivaDocumentJson ] ) ) )
        catch {
            case e : Exception => {
                LOG.error( s"Error unmarshalling CDR json : ${e.getClass.getSimpleName} : ${e.getMessage}" )
                LOG.error( s"Error root cause : ${e.getCause}" )
                None
            }
        }
    }

    private def dtoToCdr( documentId : String, dto : FactivaDocumentJson ) : CdrDocument = {
        val timeStamp = offsetDateTimeFromEpochSec( Instant.now.getEpochSecond )
        val cdrMetadata : CdrMetadata = CdrMetadata( if ( null == dto.publicationDate || dto.publicationDatetime.isEmpty ) null else localDateFromEpochMilliSec( dto.publicationDatetime.toLong ),
                                                     if ( null == dto.modificationDateTime || dto.modificationDateTime.isEmpty ) null else localDateFromEpochMilliSec( dto.modificationDateTime.toLong ),
                                                     extractAuthor( dto.byline ),
                                                     dto.documentType,
                                                     dto.snippet,
                                                     dto.languageCode,
                                                     CLASSIFICATION,
                                                     dto.title,
                                                     dto.publisherName,
                                                     "",
                                                     None,
                                                     "",
                                                     "",
                                                     PRODUCER )


        val extractedText : String = {
            if ( dto.snippet == null ) dto.body
            else if ( dto.body == null ) dto.snippet
            else dto.snippet.concat( "\n" + dto.body )
        }

        CdrDocument( CAPTURE_SOURCE,
                     cdrMetadata,
                     CONTENT_TYPE,
                     Map.empty,
                     documentId,
                     extractedText,
                     null,
                     dto.an,
                     null,
                     timeStamp,
                     dtoToAnnotations( dto ) )

    }

    private def extractAuthor( byLine : String ) : String = {
        if ( null == byLine || byLine.isEmpty ) return ""
        byLine.replaceFirst( """^.*\b(b|B)(y|Y)\b(.|:)?\s{1,}""", "" ).trim
    }

    private def dtoToAnnotations( FactivaDocumentJson : FactivaDocumentJson ) : List[ CdrAnnotation[ _ ] ] = {

        val subjectAnnotation : Option[ CdrAnnotation[ _ ] ] = {
            if ( FactivaDocumentJson.subjectCodes != null && FactivaDocumentJson.subjectCodes.nonEmpty ) {
                Some( FacetAnnotation( "factiva-subjects", "1", mapCodesToFacetScoreList( FactivaDocumentJson.subjectCodes, NEWS_SUBJECTS ) ).markStatic() )
            }
            else None
        }

        val regionAnnotations : Option[ CdrAnnotation[ _ ] ] = {
            if ( FactivaDocumentJson.regionCodes != null && FactivaDocumentJson.regionCodes.nonEmpty ) {
                Some( FacetAnnotation( "factiva-regions", "1", mapCodesToFacetScoreList( FactivaDocumentJson.regionCodes, REGION_CODES ) ).markStatic() )
            }
            else None
        }

        val industryAnnotation : Option[ CdrAnnotation[ _ ] ] = {
            if ( FactivaDocumentJson.industryCodes != null && FactivaDocumentJson.industryCodes.nonEmpty ) {
                Some( FacetAnnotation( "factiva-industries", "1", mapCodesToFacetScoreList( FactivaDocumentJson.industryCodes, INDUSTRIES_CODES ) ).markStatic() )
            }
            else None

        }

        List( subjectAnnotation, regionAnnotations, industryAnnotation ).flatten
    }

    private def extractCodesFromResourceFile( filePath : String ) : Map[ String, String ] = {
        val industryCodesStream = Resource.getAsStream( filePath )
        val decoder = Codec.UTF8.decoder.onMalformedInput( CodingErrorAction.IGNORE )
        val industryCodesData = Source.fromInputStream( industryCodesStream )( decoder ).getLines
        industryCodesData.map( industryCodeMapping => {
            val x = industryCodeMapping.split( "," )
            (x( 0 ).toLowerCase, x( 1 ))
        } ).toMap
    }

    private def mapCodesToFacetScoreList( codeList : String, codesMap : Map[ String, String ] ) : List[ FacetScore ] = {
        codeList.split( "," )
          .withFilter( code => code.nonEmpty && codesMap.getOrElse( code, "" ).nonEmpty )
          .map( code => FacetScore( codesMap( code ), None ) )
          .toList
    }

}
