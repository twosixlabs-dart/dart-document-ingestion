package com.twosixlabs.dart.pipeline.stage

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.twosixlabs.cdr4s.core.{CdrAnnotation, DictionaryAnnotation, DocumentGenealogyAnnotation, FacetAnnotation, OffsetTagAnnotation, TextAnnotation, TranslationAnnotation}
import com.twosixlabs.dart.pipeline.command.{AnnotatorFailed, AnnotatorGiveUp, DocumentProcessing, PersistAnnotation, TryAnnotate}
import com.twosixlabs.dart.service.annotator.Annotator
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object AnnotatorStage {

    def apply( supervisor : ActorRef[ DocumentProcessing ], annotator : Annotator, retryCount : Int, retryPauseMillis : Long ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup( context => new AnnotatorStage( supervisor, annotator, retryCount, retryPauseMillis, context ) )
    }
}

class AnnotatorStage( supervisor : ActorRef[ DocumentProcessing ],
                      annotator : Annotator,
                      maxRetries : Int,
                      pauseMillis : Long,
                      context : ActorContext[ DocumentProcessing ] ) extends AbstractBehavior[ DocumentProcessing ]( context ) {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private var attempts : Int = 0

    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case TryAnnotate( doc, _ ) => {
                annotator.annotate( doc ) match {
                    case Success( result ) => {
                        result match {
                            case Some( annotation : CdrAnnotation[ _ ] ) => context.self ! PersistAnnotation( doc, annotator.label, Some( overwriteLabel( annotation, annotator.label ) ) )
                            case None => context.self ! PersistAnnotation( doc, annotator.label, None )
                            case _ => Behaviors.unhandled
                        }
                    }
                    case Failure( e ) => context.self ! AnnotatorFailed( doc, e )
                }
                Behaviors.same
            }
            case success@PersistAnnotation( _, _, _, _, _ ) => {
                supervisor ! success
                Behaviors.stopped
            }
            case AnnotatorFailed( doc, exception, _ ) => {
                LOG.warn( s"received failure from annotator service : ${doc.documentId}-${annotator.label}" )
                attempts = attempts + 1
                if ( attempts <= maxRetries ) {
                    val pauseTime = pauseMillis * attempts
                    LOG.debug( s"scheduling retry: ${attempts} of ${maxRetries} with pause of ${pauseTime} ms" )
                    context.scheduleOnce( Duration( pauseTime, TimeUnit.MILLISECONDS ), context.self, TryAnnotate( doc ) )
                    Behaviors.same
                }
                else {
                    supervisor ! AnnotatorGiveUp( doc, annotator.label, Annotator.ANNOTATE_OP, exception )
                    Behaviors.stopped
                }
            }
            case _ => Behaviors.unhandled
        }
    }

    /**
      *
      * I want to do this so that we have control of the names
      *
      * @param annotation
      * @param newLabel
      * @return
      */
    private def overwriteLabel( annotation : CdrAnnotation[ _ ], newLabel : String ) : CdrAnnotation[ _ ] = {
        annotation match {
            case text : TextAnnotation => text.copy( label = newLabel )
            case dict : DictionaryAnnotation => dict.copy( label = newLabel )
            case tags : OffsetTagAnnotation => tags.copy( label = newLabel )
            case facets : FacetAnnotation => facets.copy( label = newLabel )
            case genealogy : DocumentGenealogyAnnotation => genealogy.copy( label = newLabel )
            case translation : TranslationAnnotation => translation.copy( label = newLabel )
            case _ => null
        }
    }

}
