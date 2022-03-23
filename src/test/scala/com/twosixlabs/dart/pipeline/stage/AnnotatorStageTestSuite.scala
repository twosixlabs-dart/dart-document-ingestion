package com.twosixlabs.dart.pipeline.stage

import akka.actor.typed.ActorRef
import com.twosixlabs.cdr4s.annotations.OffsetTag
import com.twosixlabs.cdr4s.core.{CdrAnnotation, OffsetTagAnnotation}
import com.twosixlabs.dart.pipeline.command.{AnnotatorGiveUp, DocumentProcessing, PersistAnnotation, TryAnnotate}
import com.twosixlabs.dart.service.annotator.Annotator
import com.twosixlabs.dart.test.base.AkkaTestBase
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE

import java.lang.Thread.sleep
import scala.util.{Failure, Success}

class AnnotatorStageTestSuite extends AkkaTestBase {

    val annotator : Annotator = mock[ Annotator ]

    override def afterEach( ) = reset( annotator )

    override def beforeEach( ) = when( annotator.label ).thenReturn( "mocked-annotator" )

    "annotator stage" should "successfully annotate a document" in {
        val numRetries : Int = 1
        val pauseMillis : Int = 0

        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )
        val returnedAnnotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "test", "1", List( OffsetTag( 0, 1, None, "tag", None ), OffsetTag( 0, 1, None, "tag", None ) ) )
        val expectedAnnotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "mocked-annotator", "1", List( OffsetTag( 0, 1, None, "tag", None ), OffsetTag( 0, 1, None, "tag", None ) ) )

        when( annotator.annotate( doc ) ).thenReturn( Success( Some( returnedAnnotation ) ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val annotatorStage : ActorRef[ DocumentProcessing ] = testKit.spawn( AnnotatorStage( probe.ref, annotator, numRetries, pauseMillis ), "test-annotator" )

        annotatorStage ! TryAnnotate( doc )

        val result = probe.expectMessageType[ PersistAnnotation ]

        result.docId shouldBe doc.documentId
        result.doc shouldBe doc
        result.annotation.isDefined shouldBe true
        result.annotation.get shouldBe expectedAnnotation
    }

    "annotator stage" should "successfully retry processing an annotation after a failure" in {
        val numRetries : Int = 1
        val pauseMillis : Int = 0

        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )
        val returnedAnnotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "test", "1", List( OffsetTag( 0, 1, None, "tag", None ), OffsetTag( 0, 1, None, "tag", None ) ) )
        val expectedAnnotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "mocked-annotator", "1", List( OffsetTag( 0, 1, None, "tag", None ), OffsetTag( 0, 1, None, "tag", None ) ) )

        when( annotator.annotate( doc ) ).thenReturn( Failure( new Exception() ), Success( Some( returnedAnnotation ) ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val annotatorStage : ActorRef[ DocumentProcessing ] = testKit.spawn( AnnotatorStage( probe.ref, annotator, numRetries, pauseMillis ), "test-annotator" )

        annotatorStage ! TryAnnotate( doc )

        val result = probe.expectMessageType[ PersistAnnotation ]

        result.docId shouldBe doc.documentId
        result.doc shouldBe doc
        result.annotation.isDefined shouldBe true
        result.annotation.get shouldBe expectedAnnotation
    }

    "annotator stage" should "successfully handle when no annotations are produced" in {
        val numRetries : Int = 1
        val pauseMillis : Int = 0

        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        when( annotator.annotate( doc ) ).thenReturn( Success( None ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val annotatorStage : ActorRef[ DocumentProcessing ] = testKit.spawn( AnnotatorStage( probe.ref, annotator, numRetries, pauseMillis ), "test-annotator" )

        annotatorStage ! TryAnnotate( doc )

        val result = probe.expectMessageType[ PersistAnnotation ]
        result.label shouldBe "mocked-annotator"
        result.annotation shouldBe None
    }

    "annotator stage" should "give up after exceeding the number of allowed retries" in {
        val numRetries : Int = 1
        val pauseMillis : Int = 0

        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        when( annotator.annotate( doc ) ).thenReturn( Failure( new Exception( "first exception" ) ), Failure( new Exception( "second exception" ) ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val annotatorStage : ActorRef[ DocumentProcessing ] = testKit.spawn( AnnotatorStage( probe.ref, annotator, numRetries, pauseMillis ), "test-annotator" )

        annotatorStage ! TryAnnotate( doc )

        sleep( 500 )

        val result = probe.expectMessageType[ AnnotatorGiveUp ]
        result.docId shouldBe doc.documentId
        result.annotatorName shouldBe "mocked-annotator"
        result.exception.getMessage shouldBe "second exception"
    }

}
