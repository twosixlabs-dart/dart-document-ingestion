package com.twosixlabs.dart.pipeline.processor

import com.fasterxml.jackson.databind.node.ObjectNode
import com.twosixlabs.cdr4s.core.{CdrDocument, CdrMetadata}
import com.twosixlabs.dart.datastore.DartDatastore
import com.twosixlabs.dart.pipeline.exception.UnrecognizedInputFormatException
import com.twosixlabs.dart.pipeline.stage.Overrides
import com.twosixlabs.dart.serialization.json.JsonDataFormats.{DART_JSON, FACTIVA_JSON, LADLE_JSON, PULSE_JSON}
import com.twosixlabs.dart.service.LanguageDetector
import com.twosixlabs.dart.service.dedup.Deduplication
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


class IngestionProcessor( cdrDatastore : DartDatastore, deduplication : Deduplication, languageDetector : LanguageDetector ) {

    implicit val ec : ExecutionContext = ExecutionContext.global

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    def extractContent( docId : String, content : ObjectNode ) : Try[ CdrDocument ] = {
        val documentJson = content.toString
        if ( FACTIVA_JSON.validate( documentJson.getBytes ) ) {
            FACTIVA_JSON.convertFactivaToCdr( docId, documentJson ) match {
                case Some( cdr ) => Success( cdr )
                case None => Failure( new Exception() )
            }
        }
        else if ( LADLE_JSON.validate( documentJson.getBytes ) ) {
            LADLE_JSON.unmarshalCdr( documentJson ) match {
                case Some( cdr ) => Success( cdr )
                case None => Failure( new Exception() )
            }
        }
        else if ( DART_JSON.validate( documentJson.getBytes ) ) {
            DART_JSON.unmarshalCdr( documentJson ) match {
                case Some( cdr ) => Success( cdr )
                case None => Failure( new Exception() )
            }
        } else if ( PULSE_JSON.validate( documentJson.getBytes ) ) {
            PULSE_JSON.convertPulseToCdr( docId, documentJson ) match {
                case Some( cdr ) => Success( cdr )
                case None => Failure( new Exception() )
            }
        }
        else Failure( new UnrecognizedInputFormatException( s"the cdr data for ${docId} was invalid" ) )

    }

    def findExistingDoc( docId : String ) : Future[ (Option[ CdrDocument ], Set[ String ]) ] = {
        for {
            doc <- cdrDatastore.getByDocId( docId )
            tenants <- cdrDatastore.tenantMembershipsFor( docId )
        } yield (doc, tenants)
    }

    def checkDuplicates( cdr : CdrDocument ) : Try[ List[ String ] ] = {
        deduplication.doDedup( cdr ) match {
            case Success( duplicates ) => Success( duplicates.duplicateDocs.map( _.docId ) )
            case Failure( e : Throwable ) => {
                LOG.error( s"${cdr.documentId} - internal failure running dedup on document - ${e.getClass.getSimpleName} : ${e.getMessage} : ${e.getCause}" )
                if ( LOG.isDebugEnabled ) e.printStackTrace()
                Failure( e )
            }
        }
    }

    def checkLanguage( doc : CdrDocument ) : String = {
        languageDetector.detectLanguage( doc.extractedText ) // TODO - make decision based on title and other fields later
    }

    /**
      * If there are new tenants, return the full list of tenants (because elasticsearch sucks), otherwise
      * return empty set (ie: no tenant updates)
      *
      * @param currentTenants
      * @param overrides
      * @return
      */
    def checkTenantUpdates( currentTenants : Set[ String ], overrides : Overrides ) : Set[ String ] = {
        if ( overrides.tenants.diff( currentTenants ).nonEmpty ) currentTenants ++ overrides.tenants
        else Set()
    }

    def mergeOverrides( original : CdrDocument, overrides : Overrides ) : CdrDocument = {
        val labels = if ( overrides.labels.nonEmpty ) original.labels ++ overrides.labels else original.labels
        val sourceUri = overrides.sourceUri.getOrElse( original.sourceUri )
        val metadata : CdrMetadata = {
            val genre = if ( overrides.genre != original.extractedMetadata.statedGenre ) overrides.genre else original.extractedMetadata.statedGenre
            original.extractedMetadata.copy( statedGenre = genre )
        }

        original.copy( labels = labels, sourceUri = sourceUri, extractedMetadata = metadata )
    }

    def reannotate( original : CdrDocument, updated : CdrDocument ) : Boolean = {
        original.extractedText != updated.extractedText ||
        original.extractedMetadata.title != updated.extractedMetadata.title ||
        original.extractedMetadata.description != updated.extractedMetadata.description
    }

}
