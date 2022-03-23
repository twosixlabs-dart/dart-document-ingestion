package com.twosixlabs.dart.pipeline.stage


import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, DispatcherSelector}
import com.twosixlabs.dart.datastore.DartDatastore
import com.twosixlabs.dart.pipeline.command.{AnnotationProcessingError, AnnotatorCompleted, AnnotatorGiveUp, AnnotatorStageShutdown, AnnotatorStatus, DocumentProcessing, PersistAnnotation, StartAnnotatorWorkflow, TryAnnotate}
import com.twosixlabs.dart.pipeline.exception.{DartDatastoreException, PersistenceException, SearchIndexException}
import com.twosixlabs.dart.pipeline.stage.AnnotatorStageManager.BLOCKING_ANNOTATOR_DISPATCHER
import com.twosixlabs.dart.service.SearchIndex
import com.twosixlabs.dart.service.annotator.AnnotatorDef
import com.twosixlabs.dart.utils.Utils.actorUuid
import com.twosixlabs.dart.{AnnotatorFactory, AppContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object AnnotatorStageManager {

    private val BLOCKING_ANNOTATOR_DISPATCHER : String = "akka.actor.blocking-annotator-dispatcher"

    def apply( docId : String,
               supervisor : ActorRef[ DocumentProcessing ],
               appContext : AppContext,
               ec : ExecutionContext ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup( context => new AnnotatorStageManager( docId,
                                                               supervisor,
                                                               appContext.annotatorFactory,
                                                               appContext.annotators,
                                                               appContext.cdrDatastore,
                                                               appContext.searchIndex,
                                                               appContext.config.getInt( "annotators.retry.count" ),
                                                               appContext.config.getInt( "annotators.retry.pause.millis" ),
                                                               context ) )
    }

    def apply( docId : String,
               supervisor : ActorRef[ DocumentProcessing ],
               annotatorFactory : AnnotatorFactory,
               annotators : Set[ AnnotatorDef ],
               DartDatastore : DartDatastore,
               searchIndex : SearchIndex,
               retries : Int,
               retryPauseMillis : Int ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup( context => new AnnotatorStageManager( docId,
                                                               supervisor,
                                                               annotatorFactory,
                                                               annotators,
                                                               DartDatastore,
                                                               searchIndex,
                                                               retries,
                                                               retryPauseMillis,
                                                               context ) )
    }

}

class AnnotatorStageManager( docId : String,
                             supervisor : ActorRef[ DocumentProcessing ],
                             annotatorFactory : AnnotatorFactory,
                             annotators : Set[ AnnotatorDef ],
                             datastore : DartDatastore,
                             searchIndex : SearchIndex,
                             retries : Int,
                             retryPauseMillis : Int,
                             context : ActorContext[ DocumentProcessing ] ) extends AbstractBehavior[ DocumentProcessing ]( context ) {

    private implicit val ec : ExecutionContext = context.system.dispatchers.lookup( DispatcherSelector.fromConfig( BLOCKING_ANNOTATOR_DISPATCHER ) )

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private var completedAnnotators : Int = 0

    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case StartAnnotatorWorkflow( doc, _ ) => {
                if ( doc.extractedText != null && doc.extractedText != "" ) {
                    annotators.foreach( annotatorConfig => {
                        LOG.debug( s"starting annotator ${docId}-${annotatorConfig.label}" )
                        val annotator = annotatorFactory.newAnnotator( annotatorConfig )
                        val annotatorStage = context.spawn( AnnotatorStage( context.self, annotator, annotatorConfig.retryCount, annotatorConfig.retryPauseMillis ), s"${docId}-${annotatorConfig.label}-${actorUuid()}" )

                        annotatorStage ! TryAnnotate( doc )
                    } )
                    Behaviors.same
                } else {
                    supervisor ! AnnotatorStageShutdown( doc.documentId )
                    Behaviors.stopped
                }
            }
            case persist@PersistAnnotation( doc, label, annotationOpt, _, attempts ) => {
                annotationOpt match {
                    case Some( annotation ) => {
                        val canonicalUpdate : Future[ Unit ] = datastore.addAnnotation( doc, annotation )
                        canonicalUpdate transform {
                            case success@Success( _ ) => success
                            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${doc.documentId} - unable to persist annotation ${annotation.label}", "CdrDatastore.addAnnotation", e ) )
                        }
                        val searchUpdate : Future[ Unit ] = searchIndex.updateAnnotation( doc, annotation )
                        searchUpdate transform {
                            case success@Success( _ ) => success
                            case Failure( e : Throwable ) => Failure( new SearchIndexException( s"${doc.documentId} - unable to upsert annotation ${annotation.label}", "SearchIndex.updateAnnotation", e ) )
                        }
                        val updateResults = Future.sequence( Set( canonicalUpdate, searchUpdate ) )
                        updateResults onComplete {
                            case Success( _ ) => {
                                val updated = doc.copy( annotations = doc.annotations :+ annotation )
                                supervisor ! AnnotatorStatus( updated, label, hasResult = true, successful = true )
                                context.self ! AnnotatorCompleted( updated, annotation.label )
                            }
                            case Failure( e : PersistenceException ) => {
                                if ( attempts < retries ) {
                                    LOG.warn( s"${doc.documentId} failed to persist, retrying ${persist.attempts + 1} of ${retries}" )
                                    val pauseTime = persist.attempts + 1 * retryPauseMillis
                                    context.scheduleOnce( Duration( pauseTime, TimeUnit.MILLISECONDS ), context.self, persist.copy( attempts = persist.attempts + 1 ) )
                                }
                                else context.self ! AnnotationProcessingError( doc, annotation.label, s"${e.operation} failed with ${e.getClass.getName} : ${e.getMessage}", e )
                            }
                            case Failure( e : Throwable ) => {
                                context.self ! AnnotationProcessingError( doc, annotation.label, "unexpected exception", e )
                            }
                        }
                    }
                    case None => {
                        supervisor ! AnnotatorStatus( doc, label, hasResult = false, successful = true )
                        context.self ! AnnotatorCompleted( doc, label )
                    }
                }
                Behaviors.same
            }
            case AnnotatorGiveUp( doc, annotatorName, operation, exception, _ ) => {
                LOG.warn( s"giving up on annotator ${doc.documentId}-${annotatorName} with error ${exception.getClass}" )
                context.self ! AnnotationProcessingError( doc, annotatorName, operation, exception )
                Behaviors.same
            }
            case AnnotatorCompleted( doc, annotatorName, _ ) => {
                LOG.debug( s"${doc.documentId}-${annotatorName} annotator completed successfully..." )

                completedAnnotators += 1
                if ( completedAnnotators == annotators.size ) {
                    supervisor ! AnnotatorStageShutdown( doc.documentId )
                    Behaviors.stopped
                }
                else Behaviors.same
            }
            case error@AnnotationProcessingError( doc, label, _, _, _ ) => {
                LOG.debug( s"${docId}-${label} annotator encountered error..." )

                supervisor ! AnnotatorStatus( doc, label, hasResult = false, successful = false )
                supervisor ! error
                context.self ! AnnotatorCompleted( doc, label )
                Behaviors.same
            }
            case _ => Behaviors.unhandled
        }
    }

}
