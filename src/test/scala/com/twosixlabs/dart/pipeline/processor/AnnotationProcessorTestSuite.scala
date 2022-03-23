//package com.twosixlabs.dart.pipeline.processor
//
//import com.twosixlabs.cdr4s.annotations.OffsetTag
//import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument, OffsetTagAnnotation}
//import com.twosixlabs.dart.pipeline.exception.{AnnotatorFailedException, ServiceIntegrationException}
//import com.twosixlabs.dart.service.annotator.Annotator
//import com.twosixlabs.dart.test.base.StandardTestBase3x
//import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE
//import org.scalatest.BeforeAndAfterEach
//
//import scala.util.{Failure, Success}
//
//class AnnotationProcessorTestSuite extends StandardTestBase3x with BeforeAndAfterEach {
//
//    private val annotator : Annotator = mock[ Annotator ]
//
//    override def afterEach( ) = reset( annotator )
//
//    "annotation processor" should "return an annotation response" in {
//        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )
//        val annotation : CdrAnnotation[ _ ] = new OffsetTagAnnotation( "test", "1", List( OffsetTag( 0, 1, None, "tag" ), OffsetTag( 0, 1, None, "tag" ) ) )
//
//        when( annotator.annotate( doc ) ).thenReturn( Success( Some( annotation ) ) )
//
//        val processor = new AnnotationProcessor( annotator )
//
//        val result = processor.annotateDocument( doc )
//        result.isSuccess shouldBe true
//        result.get.isDefined shouldBe true
//        result.get.get shouldBe annotation
//    }
//
//    "annotation processor" should "return an empty response" in {
//        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )
//
//        when( annotator.annotate( doc ) ).thenReturn( Success( None ) )
//
//        val processor = new AnnotationProcessor( annotator )
//
//        val result = processor.annotateDocument( doc )
//        result.isSuccess shouldBe true
//        result.get.isDefined shouldBe false
//    }
//
//    "annotation processor" should "add annotation" in {
//        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )
//        val annotation : CdrAnnotation[ _ ] = new OffsetTagAnnotation( "test", "1", List( OffsetTag( 0, 1, None, "tag" ), OffsetTag( 0, 1, None, "tag" ) ) )
//
//        val processor = new AnnotationProcessor( annotator )
//
//        val result = processor.addAnnotation( doc, annotation )
//
//        result.annotations should contain( annotation )
//    }
//
//    "annotation processor" should "wrap a failed annotator in the proper exception class" in {
//        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )
//
//        when( annotator.annotate( doc ) ).thenReturn( Failure( new ServiceIntegrationException( system = "test-system", message = "failed" ) ) )
//
//        val processor = new AnnotationProcessor( annotator )
//
//        processor.annotateDocument( doc ) match {
//            case Failure( e : AnnotatorFailedException ) => e.getMessage should include( "annotation failed for test-system" )
//            case _ => fail( "unexpected outcome" )
//        }
//    }
//
//    "annotation processor" should "wrap unexpected exceptions" in {
//        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )
//
//        when( annotator.annotate( doc ) ).thenReturn( Failure( new Exception( "failed" ) ) )
//
//        val processor = new AnnotationProcessor( annotator )
//
//        processor.annotateDocument( doc ) match {
//            case Failure( e : AnnotatorFailedException ) => e.getMessage should include( "unknown exception" )
//            case _ => fail( "unexpected outcome" )
//        }
//    }
//}
