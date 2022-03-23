package com.twosixlabs.dart.service.annotator

import com.twosixlabs.cdr4s.annotations.{FacetScore, OffsetTag}
import com.twosixlabs.cdr4s.core.{CdrDocument, DictionaryAnnotation, FacetAnnotation, OffsetTagAnnotation, TextAnnotation}
import com.twosixlabs.dart.HttpClientFactory
import com.twosixlabs.dart.pipeline.exception.ServiceIntegrationException
import com.twosixlabs.dart.serialization.json.JsonDataFormats.DART_JSON
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE
import com.twosixlabs.dart.test.utils.TestUtils.randomPort
import com.typesafe.config.ConfigFactory
import okhttp3.mockwebserver.{MockResponse, MockWebServer}

import java.net.ConnectException
import scala.util.{Failure, Success}

class RestAnnotatorTestSuite extends StandardTestBase3x {

    private val client = HttpClientFactory.newClient( ConfigFactory.empty() )

    private val RETRY_COUNT = 0
    private val RETRY_MIN : Long = 10

    "rest annotator" should "successfully produce a facet annotation for a document" in {

        val config = AnnotatorDef( "test-facet", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val expectedAnnotation = FacetAnnotation( "test-facets", "1", List( FacetScore( "test", Some( 1.0 ) ),
                                                                            FacetScore( "test-2", None ) ) )

        val responseJson : String = DART_JSON.marshalAnnotation( expectedAnnotation ).get

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 200 ).setBody( responseJson ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isDefined shouldBe true
        result.get.get shouldBe expectedAnnotation

        val request = mockServer.takeRequest()
        request.getPath shouldBe s"/${config.path}"
    }

    "rest annotator" should "successfully produce a dictionary annotation for a document" in {

        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val expectedAnnotation = DictionaryAnnotation( "test-dict", "1", Map( "test" -> "michael",
                                                                              "test-2" -> "derp" ) )

        val responseJson : String = DART_JSON.marshalAnnotation( expectedAnnotation ).get

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 200 ).setBody( responseJson ) )

        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isDefined shouldBe true
        result.get.get shouldBe expectedAnnotation

        val request = mockServer.takeRequest()
        request.getPath shouldBe s"/${config.path}"
    }

    "rest annotator" should "successfully produce a text annotation for a document" in {

        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val expectedAnnotation = TextAnnotation( "test-dict", "1", "michael" )
        val responseJson : String = DART_JSON.marshalAnnotation( expectedAnnotation ).get

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 200 ).setBody( responseJson ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isDefined shouldBe true
        result.get.get shouldBe expectedAnnotation

        val request = mockServer.takeRequest()
        request.getPath shouldBe s"/${config.path}"
    }

    "rest annotator" should "successfully produce a tag annotation for a document with no post-processing" in {

        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val expectedAnnotation = OffsetTagAnnotation( "test-tag", "1", List( OffsetTag( 0, 1, None, "tag-1", None ), OffsetTag( 0, 1, None, "tag-1", None ) ) )

        val responseJson : String = DART_JSON.marshalAnnotation( expectedAnnotation ).get

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 200 ).setBody( responseJson ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isDefined shouldBe true
        result.get.get shouldBe expectedAnnotation

        val request = mockServer.takeRequest()
        request.getPath shouldBe s"/${config.path}"
    }

    "rest annotator" should "successfully produce a tag annotation for a document with post-processing" in {

        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), true, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "akka is fun", annotations = List() )

        val responseJson : String = {
            val annotation = OffsetTagAnnotation( "test-tag", "1", List( OffsetTag( 0, 4, None, "tag-1", None ),
                                                                         OffsetTag( 8, 11, None, "tag-1", None ) ) )
            DART_JSON.marshalAnnotation( annotation ).get
        }

        val expectedAnnotation = OffsetTagAnnotation( "test-tag", "1", List( OffsetTag( 0, 4, Some( "akka" ), "tag-1", None ),
                                                                             OffsetTag( 8, 11, Some( "fun" ), "tag-1", None ) ) )

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 200 ).setBody( responseJson ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isDefined shouldBe true
        result.get.get shouldBe expectedAnnotation

        val request = mockServer.takeRequest()
        request.getPath shouldBe s"/${config.path}"
    }

    "rest annotator" should "successfully return no annotations" in {

        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 404 ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isEmpty shouldBe true
    }

    "rest annotator" should "properly null content" in {
        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a", annotations = List( FacetAnnotation( "facets", "1", null ) ) )

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 404 ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isEmpty shouldBe true
    }

    "rest annotator" should "successfully return an empty response when the annotation content is empty" in {

        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val expectedAnnotation = FacetAnnotation( "test-facets", "1", List() )

        val responseJson : String = DART_JSON.marshalAnnotation( expectedAnnotation ).get

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 200 ).setBody( responseJson ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        val result = annotator.annotate( cdr )
        result.isSuccess shouldBe true
        result.get.isEmpty shouldBe true
    }

    "rest annotator" should "return an exception for an unexpected response from the annotator" in {
        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 500 ) )
        mockServer.start( config.port )

        val annotator = new RestAnnotator( config, client )

        annotator.annotate( cdr ) match {
            case Success( _ ) => fail( "success not expected for this test" )
            case Failure( e : ServiceIntegrationException ) => {
                e.system shouldBe annotator.systemName
                e.message should include( "unexpected response code" )
            }
            case Failure( e : Throwable ) => fail( s"unexpected failure ${e.getClass}" )
        }
    }

    "rest annotator" should "return an exception for an unexpected exception being thrown" in {
        val config = AnnotatorDef( "test-dictionary", "localhost", randomPort(), false, "annotate", RETRY_COUNT, RETRY_MIN )

        val cdr = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val mockServer : MockWebServer = new MockWebServer()
        mockServer.enqueue( new MockResponse().setResponseCode( 500 ) )
        mockServer.start( 9999 ) // wrong port will cause a connection refused exception

        val annotator = new RestAnnotator( config, client )

        annotator.annotate( cdr ) match {
            case Success( _ ) => fail( "success not expected for this test" )
            case Failure( e : ServiceIntegrationException ) => {
                e.system shouldBe annotator.systemName
                e.message should include( "unexpected exception" )
                e.cause.getClass shouldBe classOf[ ConnectException ]
            }
            case Failure( e : Throwable ) => fail( s"unexpected failure ${e.getClass}" )
        }
    }

}
