package com.twosixlabs.dart.pipeline.stage

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.twosixlabs.cdr4s.core.{CdrDocument, CdrMetadata}
import com.twosixlabs.dart.AppContext
import com.twosixlabs.dart.pipeline.command.{AddNew, CheckDuplicates, CheckExisting, DocumentProcessing, DocumentProcessingError, Duplicate, ExistingDocument, NewDocument, NoOp, StartIngestion, UpdateExisting}
import com.twosixlabs.dart.pipeline.processor.IngestionProcessor
import com.twosixlabs.dart.serialization.json.JsonDataFormats.DART_JSON
import com.twosixlabs.dart.service.Languages
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}


object IngestionStage {
    private val BLOCKING_DEDUP_DISPATCHER : String = "akka.actor.blocking-dedup-dispatcher"

    def apply( appContext : AppContext, supervisor : ActorRef[ DocumentProcessing ] ) : Behavior[ DocumentProcessing ] = {
        val dedup = appContext.deduplication
        IngestionStage( new IngestionProcessor( appContext.cdrDatastore, dedup, appContext.languageDetector ), supervisor )
    }

    def apply( processor : IngestionProcessor, supervisor : ActorRef[ DocumentProcessing ] ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup( context => new IngestionStage( processor, supervisor, context ) )
    }
}

class IngestionStage( processor : IngestionProcessor,
                      supervisor : ActorRef[ DocumentProcessing ],
                      context : ActorContext[ DocumentProcessing ] ) extends AbstractBehavior[ DocumentProcessing ]( context ) {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private val STAGE_NAME : String = "ingestion"

    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case StartIngestion( docId, content, updates ) => { // extract text
                LOG.debug( s"StartIngestion -> ${docId} - ${updates}" )
                processor.extractContent( docId, content ) match {
                    case Success( extracted ) => {
                        LOG.debug( s"${docId} got cdr content" )
                        val language = processor.checkLanguage( extracted )
                        if ( language == Languages.ENGLISH ) {
                            LOG.debug( s"${docId} check existing" )
                            context.self ! CheckExisting( extracted, updates )
                        } else {
                            val warning = s"${language} is not supported for: ${extracted.documentId}"
                            LOG.warn( warning )
                            context.self ! DocumentProcessingError( extracted.documentId, STAGE_NAME, warning, new UnsupportedOperationException( warning ) )
                        }
                    }
                    case Failure( e ) => {
                        LOG.error( s"could not extract data from input body: ${docId}" )
                        if ( LOG.isDebugEnabled() ) LOG.debug( content.toString() )

                        context.self ! DocumentProcessingError( message.docId, STAGE_NAME, content.toString, e )
                    }
                }
                Behaviors.same
            }
            case CheckExisting( incomingDoc, updates ) => {
                implicit val ec : ExecutionContext = ExecutionContext.global

                LOG.debug( s"CheckExisting -> ${incomingDoc.documentId} - ${updates}" )
                processor.findExistingDoc( incomingDoc.documentId ) onComplete {
                    case Success( (None, _) ) => { // this is a new document
                        context.self ! NewDocument( incomingDoc, updates )
                    }
                    case Success( (Some( existingDoc ), currentTenants) ) => { // document already exists...
                        context.self ! ExistingDocument( existingDoc, currentTenants, updates )
                    }
                    case Failure( e : Throwable ) => {
                        e.printStackTrace()
                        context.self ! DocumentProcessingError( incomingDoc.documentId, STAGE_NAME, DART_JSON.marshalCdr( incomingDoc ).getOrElse( "failed to marshal document..." ), e )
                    }
                }
                Behaviors.same
            }
            case NewDocument( doc, updates ) => {
                LOG.debug( s"NewDocument -> ${doc.documentId} - ${updates}" )
                val updatedDoc : CdrDocument = processor.mergeOverrides( doc, updates )
                context.self ! CheckDuplicates( updatedDoc, updates )
                Behaviors.same
            }
            case ExistingDocument( doc, currentTenants, updates ) => {
                LOG.debug( s"ExistingDocument -> ${doc.documentId} - ${updates}" )
                val updatedDoc : CdrDocument = processor.mergeOverrides( doc, updates )
                val reAnnotate : Boolean = processor.reannotate( doc, updatedDoc ) || updates.runAnnotators
                val tenants = processor.checkTenantUpdates( currentTenants, updates )

                if ( tenants.nonEmpty || doc != updatedDoc || reAnnotate ) context.self ! UpdateExisting( updatedDoc, updates.copy( tenants = tenants, runAnnotators = reAnnotate ) )
                else context.self ! NoOp( doc.documentId )
                Behaviors.same
            }
            case CheckDuplicates( doc, updates, attempt ) => {
                LOG.debug( s"CheckDuplicates-> ${doc.documentId} - ${updates}" )
                processor.checkDuplicates( doc ) match {
                    case Success( List() ) => context.self ! AddNew( doc, updates )
                    case Success( duplicateDocs ) => context.self ! Duplicate( doc, duplicateDocs, updates )
                    case Failure( e ) => {
                        context.self ! DocumentProcessingError( doc.documentId, "deduplication", s"deduplication failed - ${e.getClass} : ${e.getMessage}", e )
                    }
                }
                Behaviors.same
            }
            case duplicate@Duplicate( doc, duplicates, updates ) => {
                LOG.debug( s"Duplicate -> ${doc.documentId}, duplicates = ${duplicates}" )

                // this is to prevent an edge case where the doc has been added to the duplicates but not persisted in the primary datastore
                val validDups = duplicates.filter( _ != doc.documentId )
                if ( validDups.nonEmpty ) {
                    supervisor ! duplicate
                    context.self ! CheckExisting( CdrDocument( documentId = validDups.head, extractedMetadata = CdrMetadata() ), updates )
                }
                else context.self ! AddNew( doc, updates.copy( runAnnotators = true ) ) // treat as a new document

                Behaviors.same
            }
            case newDoc@AddNew( doc, _ ) => {
                LOG.debug( s"adding new document: ${doc.documentId}" )
                supervisor ! newDoc.copy( updates = newDoc.updates.copy( runAnnotators = true ) )
                Behaviors.stopped
            }
            case existing@UpdateExisting( _, _ ) => {
                LOG.debug( s"applying updates to existing doc: ${existing.docId}" )
                supervisor ! existing
                Behaviors.stopped
            }
            case noop@NoOp( docId, updates ) => {
                LOG.debug( s"NoOp -> ${docId} - ${updates}" )
                supervisor ! noop
                Behaviors.stopped
            }
            case error@DocumentProcessingError( docId, _, _, cause, _ ) => {
                context.log.error( s"${docId} - error ingesting document" )
                LOG.debug( s"${docId} check existing" )
                if ( LOG.isDebugEnabled() ) cause.printStackTrace()
                supervisor ! error
                Behaviors.stopped
            }
            case unexpected : DocumentProcessing => {
                LOG.warn( s"unexpected message sent to ${unexpected.docId} ingestion actor: ${unexpected}" )
                Behaviors.unhandled
            }
        }
    }

}
