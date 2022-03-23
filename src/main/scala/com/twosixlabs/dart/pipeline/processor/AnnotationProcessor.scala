package com.twosixlabs.dart.pipeline.processor

import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument}
import com.twosixlabs.dart.pipeline.exception.{AnnotatorFailedException, ServiceIntegrationException}
import com.twosixlabs.dart.service.annotator.Annotator

import scala.util.{Failure, Success, Try}

class AnnotationProcessor( annotator : Annotator ) {

    def annotateDocument( doc : CdrDocument ) : Try[ Option[ CdrAnnotation[ _ ] ] ] = {
        annotator.annotate( doc ) match {
            case success@Success( _ ) => success
            case Failure( e : ServiceIntegrationException ) => {
                Failure( new AnnotatorFailedException( s"annotation failed for ${e.system}", e ) )
            }
            case Failure( e : Throwable ) => Failure( new AnnotatorFailedException( "unknown exception from annotator", e ) )
        }
    }

    def addAnnotation( doc : CdrDocument, annotation : CdrAnnotation[ _ ] ) : CdrDocument = {
        doc.copy( annotations = doc.annotations :+ annotation )
    }

}
