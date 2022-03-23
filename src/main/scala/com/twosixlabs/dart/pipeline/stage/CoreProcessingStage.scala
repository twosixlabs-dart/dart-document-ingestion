package com.twosixlabs.dart.pipeline.stage

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument}
import com.twosixlabs.dart.AppContext
import com.twosixlabs.dart.pipeline.command.{AddNew, CoreProcessingComplete, DoTextCleanup, DoTranslation, DocumentProcessing, DocumentProcessingError, RunProcessorChain, SaveDocument, SyncTenants, UpdateExisting}
import com.twosixlabs.dart.pipeline.exception.{DartDatastoreException, SearchIndexException}
import com.twosixlabs.dart.pipeline.processor.CoreProcessor
import com.twosixlabs.dart.pipeline.processor.Translations.Language.ENGLISH
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object CoreProcessingStage {
    def apply( supervisor : ActorRef[ DocumentProcessing ], appContext : AppContext ) : Behavior[ DocumentProcessing ] = {
        CoreProcessingStage( supervisor, new CoreProcessor( appContext.cdrDatastore, appContext.searchIndex, appContext.translator, appContext.preprocessors ) )
    }

    def apply( supervisor : ActorRef[ DocumentProcessing ], processor : CoreProcessor ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup( context => new CoreProcessingStage( context, supervisor, processor ) )
    }
}

class CoreProcessingStage( context : ActorContext[ DocumentProcessing ], supervisor : ActorRef[ DocumentProcessing ],
                           processor : CoreProcessor ) extends AbstractBehavior[ DocumentProcessing ]( context ) {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private implicit val ec : ExecutionContext = ExecutionContext.global

    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case AddNew( doc, updates ) => {
                LOG.debug( s"AddNew -> ${doc.documentId} : ${updates}" )
                processor.saveCdr( doc ) onComplete { // we need to immediately add it to the datastore
                    case Success( _ ) => {
                        processor.checkLanguage( doc ) match {
                            case ENGLISH => context.self ! DoTextCleanup( doc, updates )
                            case other => context.self ! DoTranslation( doc, other, updates )
                        }
                    }
                    case Failure( e : Throwable ) => handleFailure( doc.documentId, e )
                }
                Behaviors.same
            }
            case UpdateExisting( doc, updates ) => {
                LOG.debug( s"UpdateExisting - > ${doc.documentId} labels = ${doc.labels}" )

                val updated = if ( updates.runAnnotators ) {
                    doc.copy( annotations = doc.annotations.filter( _.classification == CdrAnnotation.STATIC ) )
                } else doc

                context.self ! SaveDocument( doc = updated, updates = updates )
                Behaviors.same
            }
            case DoTextCleanup( doc, updates ) => {
                LOG.debug( s"DoTextCleanup -> ${doc.documentId}" )
                processor.fixText( doc ) onComplete {
                    case Success( fixed : CdrDocument ) => context.self ! RunProcessorChain( fixed, updates )
                    case Failure( e : Throwable ) => handleFailure( doc.documentId, e )
                }
                Behaviors.same
            }
            case RunProcessorChain( doc, updates ) => {
                LOG.debug( s"RunProcessorChain -> ${doc.documentId}" )
                processor.executeProcessors( doc ) onComplete {
                    case Success( processed : CdrDocument ) => context.self ! SaveDocument( doc = processed, updates = updates )
                    case Failure( e : Throwable ) => handleFailure( doc.documentId, e )
                }
                Behaviors.same
            }
            case SaveDocument( doc, updates ) => {
                LOG.debug( s"SaveDocument -> ${doc.documentId} : ${updates}" )
                processor.saveCdr( doc ) onComplete {
                    case Success( doc : CdrDocument ) => {
                        if ( updates.tenants.nonEmpty ) {
                            LOG.debug( s"need to update tenants ${updates.tenants}" )
                            context.self ! SyncTenants( doc, updates )
                        }
                        else context.self ! CoreProcessingComplete( doc, updates )
                    }
                    case Failure( e ) => Failure( e )
                }
                Behaviors.same
            }
            case SyncTenants( doc, updates ) => {
                LOG.debug( s"SyncTenants -> ${doc.documentId} : ${updates.tenants}" )
                processor.updateTenants( doc, updates.tenants ) onComplete {
                    case Success( _ ) => context.self ! CoreProcessingComplete( doc, updates )
                    case Failure( e ) => handleFailure( doc.documentId, e )
                }
                Behaviors.same
            }
            case complete@CoreProcessingComplete( doc, _ ) => {
                LOG.debug( s"CoreProcessingComplete -> ${doc.documentId}" )
                supervisor ! complete
                Behaviors.stopped
            }
            case error@DocumentProcessingError( docId, _, _, _, _ ) => {
                context.log.error( s"DocumentProcessingError -> ${docId}" )
                supervisor ! error
                Behaviors.stopped
            }
            case _ => Behaviors.unhandled
        }
    }

    private def handleFailure( docId : String, exception : Throwable ) : Unit = {
        exception match {
            case e : DartDatastoreException => context.self ! DocumentProcessingError( docId, e.operation, e.message, e )
            case e : SearchIndexException => context.self ! DocumentProcessingError( docId, e.operation, e.message, e )
            case e : Throwable => context.self ! DocumentProcessingError( docId, e.getMessage, "unknown error", new IllegalStateException( e ) )
        }
    }

}
