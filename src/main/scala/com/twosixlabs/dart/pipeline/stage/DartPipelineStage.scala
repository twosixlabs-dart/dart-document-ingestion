package com.twosixlabs.dart.pipeline.stage

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.twosixlabs.dart.AppContext
import com.twosixlabs.dart.exceptions.Exceptions
import com.twosixlabs.dart.operations.status.PipelineStatus.ProcessorType
import com.twosixlabs.dart.pipeline.command.{AddNew, AnnotationProcessingError, AnnotatorStageShutdown, AnnotatorStatus, CoreProcessingComplete, DocumentProcessing, DocumentProcessingError, Duplicate, IngestProxy, NoOp, NotifyDuplicate, NotifyError, NotifyUpdate, OperationalStatusFailure, OperationalStatusSuccess, StartAnnotatorWorkflow, StartIngestion, UpdateExisting}
import com.twosixlabs.dart.utils.Utils.actorUuid
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

object DartPipelineStage {
    def apply( documentId : String,
               appContext : AppContext,
               notifications : ActorRef[ DocumentProcessing ],
               operations : ActorRef[ DocumentProcessing ] ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup( context => new DartPipelineStage( documentId, appContext, notifications, operations, context )( context.executionContext ) )
    }

}

class DartPipelineStage( documentId : String,
                         appContext : AppContext,
                         notifications : ActorRef[ DocumentProcessing ],
                         operations : ActorRef[ DocumentProcessing ],
                         context : ActorContext[ DocumentProcessing ] )( implicit val executionContext : ExecutionContext ) extends AbstractBehavior[ DocumentProcessing ]( context ) {
    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private val enableAnnotators : Boolean = appContext.enableAnnotators
    private val streamAnnotatorUpdates : Boolean = appContext.config.getBoolean( "notifications.stream.annotation.updates" )


    LOG.debug( s"${documentId} starting processing" )

    // this is the main pipeline router
    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case ingest@IngestProxy( _, event ) => {
                LOG.debug( s"IngestProxy -> ${event.metadata}" )
                LOG.debug( s"starting processing for ${ingest.docId}" )
                val ingestStage = context.spawn( IngestionStage( appContext, context.self ), s"${ingest.docId}-ingestion-${actorUuid()}" )
                ingestStage ! StartIngestion( ingest.docId, ingest.content, ingest.updates )

                Behaviors.same
            }
            case add@AddNew( doc, updates ) => {
                LOG.debug( s"AddNew -> ${doc.documentId} - ${updates}" )
                operations ! OperationalStatusSuccess( doc.documentId, "ingestion", ProcessorType.CORE )

                LOG.debug( s"${doc.documentId} is a new document, processing..." )
                val coreProcessing = context.spawn( CoreProcessingStage( context.self, appContext ), s"${doc.documentId}-core-processing" )
                coreProcessing ! add

                Behaviors.same
            }
            case existing@UpdateExisting( doc, updates ) => {
                LOG.debug( s"UpdateExisting -> ${doc.documentId} - ${updates}" )
                operations ! OperationalStatusSuccess( doc.documentId, "ingestion", ProcessorType.CORE )

                LOG.debug( s"${doc.documentId} already exists, applying any updates..." )
                val coreProcessing = context.spawn( CoreProcessingStage( context.self, appContext ), s"${doc.documentId}-core-processing" )
                coreProcessing ! existing

                Behaviors.same
            }
            case CoreProcessingComplete( doc, updates ) => {
                LOG.debug( s"${doc.documentId} finished core processing" )
                notifications ! NotifyUpdate( doc )
                operations ! OperationalStatusSuccess( doc.documentId, "core-processing", ProcessorType.CORE )

                if ( enableAnnotators ) {
                    if ( updates.runAnnotators ) context.self ! StartAnnotatorWorkflow( doc )
                    else context.self ! NoOp( doc.documentId )
                } else context.self ! NoOp( doc.documentId )

                Behaviors.same
            }
            case startAnnotating@StartAnnotatorWorkflow( doc, _ ) => {
                LOG.debug( s"${doc.documentId} - starting annotator workflow" )
                val annotationManager = context.spawn( AnnotatorStageManager( doc.documentId, context.self, appContext, executionContext ), s"${doc.documentId}-annotator-manager-${actorUuid()}" )

                annotationManager ! startAnnotating
                Behaviors.same
            }
            case AnnotatorStatus( doc, annotator, hasResult, successful, _ ) => {
                if ( successful ) {
                    operations ! OperationalStatusSuccess( doc.documentId, annotator, ProcessorType.ANNOTATOR )
                    if ( hasResult && streamAnnotatorUpdates ) notifications ! NotifyUpdate( doc )
                } else operations ! OperationalStatusFailure( doc.documentId, annotator, ProcessorType.ANNOTATOR, null )
                Behaviors.same
            }
            case AnnotatorStageShutdown( docId, _ ) => {
                LOG.info( s"${docId} processing complete..." )
                Behaviors.stopped
            }
            case Duplicate( doc, duplicates, overrides ) => {
                LOG.info( s"${doc.documentId} marked as duplicate of ${duplicates.mkString( "," )}" )
                notifications ! NotifyDuplicate( doc.documentId, duplicates, overrides )
                operations ! OperationalStatusFailure( doc.documentId, "ingestion", ProcessorType.CORE, s"${documentId} duplicates: ${duplicates.mkString}" )
                Behaviors.same
            }
            case DocumentProcessingError( documentId, operation, message, exception, _ ) => {
                LOG.error( s"there was a fatal error for ${operation} on document: ${documentId} : ${exception.getClass} : ${exception.getMessage} : ${exception.getCause}" )
                notifications ! NotifyError( documentId, operation, exception.getMessage, exception )
                operations ! OperationalStatusFailure( documentId, "Document Processing Error", ProcessorType.CORE, Exceptions.getStackTraceText( exception ) )

                Behaviors.stopped
            }
            case AnnotationProcessingError( doc, annotator, message, exception, _ ) => {
                LOG.warn( s"${doc.documentId}-${annotator} - failed : ${message}" )
                notifications ! NotifyError( doc.documentId, annotator, exception.getMessage, exception ) // eventually send to a "retryable" dlq
                Behaviors.same
            }
            case NoOp( _, _ ) => Behaviors.stopped
            case _ => Behaviors.unhandled
        }
    }

}
