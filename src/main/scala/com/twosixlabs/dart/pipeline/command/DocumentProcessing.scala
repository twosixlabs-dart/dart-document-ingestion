package com.twosixlabs.dart.pipeline.command

import akka.actor.typed.ActorRef
import com.fasterxml.jackson.databind.node.ObjectNode
import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument}
import com.twosixlabs.dart.operations.status.PipelineStatus.ProcessorType
import com.twosixlabs.dart.pipeline.processor.Translations.Language
import com.twosixlabs.dart.pipeline.stage.Overrides
import com.twosixlabs.dart.serialization.json.IngestProxyEvent


sealed trait DocumentProcessing {
    val docId : String
    val updates : Overrides
}

// ingestion
final case class IngestProxy( docId : String, event : IngestProxyEvent ) extends DocumentProcessing {
    override val updates : Overrides = Overrides.fromIngestMetadata( event )

    val content : ObjectNode = event.document
}

final case class StartIngestion( docId : String, content : ObjectNode, updates : Overrides = Overrides() ) extends DocumentProcessing

final case class CheckExisting( doc : CdrDocument, updates : Overrides ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class CheckDuplicates( doc : CdrDocument, updates : Overrides, attempt : Int = 0 ) extends DocumentProcessing {
    override val docId = doc.documentId
}

final case class Duplicate( doc : CdrDocument, duplicateOf : List[ String ], updates : Overrides ) extends DocumentProcessing {
    override val docId = doc.documentId
}

final case class NewDocument( doc : CdrDocument, updates : Overrides ) extends DocumentProcessing {
    override val docId = doc.documentId
}

final case class ExistingDocument( doc : CdrDocument, currentTenants : Set[ String ], updates : Overrides ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class NoOp( docId : String, updates : Overrides = Overrides() ) extends DocumentProcessing

// core doc processing
final case class AddNew( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId = doc.documentId
}

final case class UpdateExisting( doc : CdrDocument, updates : Overrides ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class DoTranslation( doc : CdrDocument, language : Language, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId = doc.documentId
}

final case class DoTextCleanup( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId = doc.documentId
}

final case class RunProcessorChain( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class SaveDocument( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class CoreProcessingComplete( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class SyncTenants( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

// annotations
final case class StartAnnotatorWorkflow( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class TryAnnotate( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class PersistAnnotation( doc : CdrDocument, label : String, annotation : Option[ CdrAnnotation[ _ ] ], updates : Overrides = Overrides(), attempts : Int = 0 ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class AnnotatorStatus( doc : CdrDocument, label : String, hasResult : Boolean, successful : Boolean, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class AnnotatorFailed( doc : CdrDocument, exception : Throwable, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class AnnotatorGiveUp( doc : CdrDocument, annotatorName : String, operation : String, exception : Throwable, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class AnnotatorCompleted( doc : CdrDocument, annotatorName : String, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class AnnotatorStageShutdown( docId : String, updates : Overrides = Overrides() ) extends DocumentProcessing

// operations
final case class NotifyUpdate( doc : CdrDocument, updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class NotifyError( docId : String, operation : String, message : String, cause : Throwable, updates : Overrides = Overrides() ) extends DocumentProcessing

final case class NotifyDuplicate( docId : String, duplicateOf : List[ String ], updates : Overrides = Overrides() ) extends DocumentProcessing

final case class SearchInsert( docId : String, cdr : CdrDocument, parent : ActorRef[ DocumentProcessing ], updates : Overrides = Overrides() ) extends DocumentProcessing

final case class SearchUpdateAnnotation( doc : CdrDocument,
                                         annotation : CdrAnnotation[ _ ],
                                         parent : ActorRef[ DocumentProcessing ],
                                         updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}

final case class OperationalStatusSuccess( docId : String, stageName : String, processorType : ProcessorType, updates : Overrides = Overrides() ) extends DocumentProcessing

final case class OperationalStatusFailure( docId : String, stageName : String, processorType : ProcessorType, message : String, updates : Overrides = Overrides() ) extends DocumentProcessing

// errors
final case class DocumentProcessingError( docId : String, operation : String, message : String, exception : Throwable, updates : Overrides = Overrides() ) extends DocumentProcessing

final case class AnnotationProcessingError( doc : CdrDocument,
                                            annotator : String,
                                            message : String = null,
                                            exception : Throwable = null,
                                            updates : Overrides = Overrides() ) extends DocumentProcessing {
    override val docId : String = doc.documentId
}
