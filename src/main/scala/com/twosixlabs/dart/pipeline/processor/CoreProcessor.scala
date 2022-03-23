package com.twosixlabs.dart.pipeline.processor

import com.twosixlabs.cdr4s.core.CdrDocument
import com.twosixlabs.dart.datastore.DartDatastore
import com.twosixlabs.dart.pipeline.exception.{DartDatastoreException, SearchIndexException}
import com.twosixlabs.dart.pipeline.processor.CoreProcessor.{LIGATURE_REPAIR, NORMALIZER}
import com.twosixlabs.dart.pipeline.processor.Translations.Language
import com.twosixlabs.dart.service.{Preprocessor, SearchIndex}
import com.twosixlabs.dart.text.cleanup.{LigatureRepair, RuleBasedLigatureRepair}
import com.twosixlabs.dart.text.normalization.{Dashes, InvisibleFormattingChars, SmartQuotes, TextNormalizer, WeirdCommaSpaces}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object CoreProcessor {
    protected val NORMALIZER : TextNormalizer = new TextNormalizer() with InvisibleFormattingChars with WeirdCommaSpaces with SmartQuotes with Dashes
    protected val LIGATURE_REPAIR : LigatureRepair = new RuleBasedLigatureRepair
}

class CoreProcessor( datastore : DartDatastore,
                     searchIndex : SearchIndex,
                     translator : AnyRef,
                     preprocessors : Seq[ Preprocessor ] = Seq() ) {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private implicit val ec : ExecutionContext = ExecutionContext.global

    def saveCdr( doc : CdrDocument ) : Future[ CdrDocument ] = {
        val canonicalInsert : Future[ Unit ] = datastore.saveDocument( doc )
        canonicalInsert onComplete {
            case success@Success( _ ) => success
            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${doc.documentId} - unable to persist document", DartDatastore.SAVE_DOCUMENT_OP, e ) )
        }

        val searchInsert : Future[ Unit ] = searchIndex.upsertDocument( doc )
        searchInsert onComplete {
            case success@Success( _ ) => success
            case Failure( e : Throwable ) => Failure( new SearchIndexException( s"${doc.documentId} - unable to persist document", SearchIndex.UPSERT_OP, e ) )
        }

        val updateResults = Future.sequence( Set( canonicalInsert, searchInsert ) ) transform {
            case Success( _ ) => Success( doc )
            case Failure( e ) => {
                LOG.error( s"error persisting CDR data : ${e.getClass} : ${e.getCause} : ${e.getMessage}" )
                Failure( e )
            }
        }
        updateResults
    }

    def updateTenants( doc : CdrDocument, tenants : Set[ String ] ) : Future[ Set[ String ] ] = {
        val datastoreUpdates = tenants.toSeq.map( tenant => {
            datastore.updateTenant( doc.documentId, tenant ) transform {
                case Success( _ ) => Success()
                case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${doc.documentId} - unable to update tenants", DartDatastore.UPDATE_TENANT_OP, e ) )
            }
        } )

        val searchUpdate = searchIndex.updateTenants( doc.documentId, tenants )

        Future.sequence( datastoreUpdates :+ searchUpdate ) transform {
            case Success( _ ) => Success( tenants )
            case Failure( e ) => Failure( e )
        }
    }

    def checkLanguage( doc : CdrDocument ) : Language = Language.ENGLISH

    def fixText( doc : CdrDocument ) : Future[ CdrDocument ] = {
        val updatedText = NORMALIZER.normalize( LIGATURE_REPAIR.repairLigatureErrors( doc.extractedText ) )
        if ( updatedText != doc.extractedText ) saveCdr( doc.copy( extractedText = updatedText ) )
        else Future.successful( doc )
    }

    def executeProcessors( doc : CdrDocument ) : Future[ CdrDocument ] = {
        val processedDoc = preprocessors.foldLeft( doc )( ( cdr, preprocessor ) => preprocessor.preprocess( cdr ) )
        saveCdr( processedDoc )
    }
}
