package com.twosixlabs.dart.pipeline.processor

import com.twosixlabs.cdr4s.core.CdrDocument
import com.twosixlabs.dart.datastore.DartDatastore
import com.twosixlabs.dart.pipeline.exception.{DartDatastoreException, SearchIndexException}
import com.twosixlabs.dart.pipeline.processor.Translations.Language
import com.twosixlabs.dart.service.SearchIndex.UPDATE_TENANTS_OP
import com.twosixlabs.dart.service.{Preprocessor, SearchIndex}
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE
import com.twosixlabs.dart.utils.AsyncDecorators._
import org.mockito.captor.ArgCaptor
import org.scalatest.BeforeAndAfterEach

import java.net.ConnectException
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CoreProcessorTestSuite extends StandardTestBase3x with BeforeAndAfterEach {

    implicit val ec : ExecutionContext = ExecutionContext.global

    private val repository : DartDatastore = mock[ DartDatastore ]( withSettings.lenient() )
    private val searchIndex : SearchIndex = mock[ SearchIndex ]

    override def afterEach( ) : Unit = reset( repository, searchIndex )

    //====================================================================================
    // Persistence
    //====================================================================================
    "Core Processor" should "save a document" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )

        when( repository.saveDocument( doc ) ).thenReturn( Future.successful() )
        when( searchIndex.upsertDocument( doc ) ).thenReturn( Future.successful() )
        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.saveCdr( doc ).synchronously()
        result.isSuccess shouldBe true
        result.get shouldBe doc
    }

    "Core Processor" should "handle datastore persistence failures" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )

        when( repository.saveDocument( doc ) ).thenReturn( Future.failed( new DartDatastoreException( "failed", DartDatastore.SAVE_DOCUMENT_OP ) ) )
        when( searchIndex.upsertDocument( doc ) ).thenReturn( Future.successful() )
        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.saveCdr( doc ).synchronously()
        result.isFailure shouldBe true
        result match {
            case Failure( e : DartDatastoreException ) => {
                e.operation shouldBe DartDatastore.SAVE_DOCUMENT_OP
            }
            case _ => fail( "unexpected condition occurred during test" )
        }
    }

    "Core Processor" should "handle search index persistence failures" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )

        when( repository.saveDocument( doc ) ).thenReturn( Future.successful() )
        when( searchIndex.upsertDocument( doc ) ).thenReturn( Future.failed( new SearchIndexException( "failed", SearchIndex.UPSERT_OP ) ) )
        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.saveCdr( doc ).synchronously()
        result.isFailure shouldBe true
        result match {
            case Failure( e : SearchIndexException ) => {
                e.operation shouldBe SearchIndex.UPSERT_OP
            }
            case _ => fail( "unexpected condition occurred during test" )
        }
    }


    //TODO - implement translation and add tests here
    //====================================================================================
    // Translation and language detection
    //====================================================================================
    "Core Processor" should "identify the document language as English" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )

        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.checkLanguage( doc )
        result shouldBe Language.ENGLISH
    }

    //====================================================================================
    // Text cleanup and normalization
    //====================================================================================
    "Core Processor" should "not modify text that has no normalization/extract issues" in {
        val originalText = "this text has no issues"
        val original = CDR_TEMPLATE.copy( documentId = "1a", extractedText = originalText )

        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.fixText( original ).synchronously()
        result.isSuccess shouldBe true
        result.get shouldBe original
    }

    "Core Processor" should "apply normalization to text" in {
        val originalText = "this is an annoying dash －"
        val original = CDR_TEMPLATE.copy( documentId = "1a", extractedText = originalText )

        val resultCaptor = ArgCaptor[ CdrDocument ]
        when( repository.saveDocument( resultCaptor.capture ) ).thenReturn( Future.successful() )
        when( searchIndex.upsertDocument( * ) ).thenReturn( Future.successful() )
        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.fixText( original ).synchronously()
        val expectedText = "this is an annoying dash -"
        val expected = original.copy( extractedText = expectedText )

        result.isSuccess shouldBe true
        resultCaptor.value shouldBe expected
    }

    "Core Processor" should "apply ligature fixes for extraction errors" in {
        val originalText = "this is a ligature extraction error: confl ict"
        val original = CDR_TEMPLATE.copy( documentId = "1a", extractedText = originalText )

        val resultCaptor = ArgCaptor[ CdrDocument ]
        when( repository.saveDocument( resultCaptor.capture ) ).thenReturn( Future.successful() )
        when( searchIndex.upsertDocument( * ) ).thenReturn( Future.successful() )
        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.fixText( original ).synchronously()
        val expectedText = "this is a ligature extraction error: conflict"
        val expected = original.copy( extractedText = expectedText )

        result.isSuccess shouldBe true
        resultCaptor.value shouldBe expected
    }

    "Core Processor" should "apply all text fixes (normalization & extraction issues)" in {
        val originalText = "this is an annoying dash －, and a ligature error: confl ict"
        val original = CDR_TEMPLATE.copy( documentId = "1a", extractedText = originalText )

        val resultCaptor = ArgCaptor[ CdrDocument ]
        when( repository.saveDocument( resultCaptor.capture ) ).thenReturn( Future.successful() )
        when( searchIndex.upsertDocument( * ) ).thenReturn( Future.successful() )
        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.fixText( original ).synchronously()
        val expectedText = "this is an annoying dash -, and a ligature error: conflict"
        val expected = original.copy( extractedText = expectedText )

        result.isSuccess shouldBe true
        resultCaptor.value shouldBe expected
    }

    "Core Processor" should "report errors that occurred during text cleanup" in {
        val originalText = "this is an annoying dash －, and a ligature error: confl ict"

        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = originalText )

        when( repository.saveDocument( * ) ).thenReturn( Future.failed( new DartDatastoreException( "failed", DartDatastore.SAVE_DOCUMENT_OP ) ) )
        when( searchIndex.upsertDocument( * ) ).thenReturn( Future.failed( new SearchIndexException( "update text failed", SearchIndex.UPSERT_OP ) ) )
        val processor = new CoreProcessor( repository, searchIndex, null, null )

        val result = processor.fixText( doc ).synchronously()
        result.isFailure shouldBe true
        result match {
            case Failure( e : DartDatastoreException ) => {
                e.operation shouldBe DartDatastore.SAVE_DOCUMENT_OP
            }
            case _ => fail( "unexpected condition occurred during test" )
        }
    }

    // TODO @michael -- can all the logic be implemented as a processor chain?
    //====================================================================================
    // Preprocessors
    //====================================================================================
    "Core Processor" should "apply the ntriples preprocessor" in {
        val captureSourceOverride = "test"
        class CaptureSourceOverridePreprocessor extends Preprocessor {
            override def preprocess( doc : CdrDocument ) : CdrDocument = doc.copy( captureSource = captureSourceOverride )
        }
        val original = CDR_TEMPLATE.copy( documentId = "1a" )


        when( repository.saveDocument( * ) ).thenReturn( Future.successful() )
        when( searchIndex.upsertDocument( * ) ).thenReturn( Future.successful() )
        val processor = new CoreProcessor( repository, searchIndex, null, Seq( new CaptureSourceOverridePreprocessor() ) )

        //@formatter:off
        val result = processor.executeProcessors( original ).synchronously()
        result.isSuccess shouldBe true
        result.get.captureSource shouldBe captureSourceOverride
        //@formatter:on
    }

    "Core Processor" should "apply the whole sequence of pre-processors" in {
        val captureSourceOverride = "test"
        class CaptureSourceOverridePreprocessor extends Preprocessor {
            override def preprocess( doc : CdrDocument ) : CdrDocument = doc.copy( captureSource = captureSourceOverride )
        }

        val uriOverride = "test"
        class UriOverridePreprocessor extends Preprocessor {
            override def preprocess( doc : CdrDocument ) : CdrDocument = doc.copy( uri = uriOverride )
        }

        val doc = CDR_TEMPLATE.copy( documentId = "1a" )

        val preprocessors : Seq[ Preprocessor ] = Seq( new UriOverridePreprocessor, new CaptureSourceOverridePreprocessor )

        when( repository.saveDocument( * ) ).thenReturn( Future.successful() )
        when( searchIndex.upsertDocument( * ) ).thenReturn( Future.successful() )
        val processor = new CoreProcessor( repository, searchIndex, null, preprocessors )

        val result = processor.executeProcessors( doc ).synchronously()

        //@formatter:off
        result.isSuccess shouldBe true
        result.get.uri shouldBe uriOverride
        result.get.captureSource shouldBe captureSourceOverride
        //@formatter:on
    }

    "Core Processor" should "report failures from preprocessing" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )

        class CaptureSourceOverridePreprocessor extends Preprocessor {
            override def preprocess( doc : CdrDocument ) : CdrDocument = doc.copy( captureSource = null )
        }
        val preprocessors : Seq[ Preprocessor ] = Seq( new CaptureSourceOverridePreprocessor )
        
        when( repository.saveDocument( * ) ).thenReturn( Future.failed( new DartDatastoreException( "failed", DartDatastore.SAVE_DOCUMENT_OP ) ) )
        when( searchIndex.upsertDocument( * ) ).thenReturn( Future.successful() )

        val processor = new CoreProcessor( repository, searchIndex, null, preprocessors )

        //@formatter:off
        val result = processor.executeProcessors( doc ).synchronously()
        result.isFailure shouldBe true
        result match {
            case Failure( e : DartDatastoreException ) => {
                e.operation shouldBe DartDatastore.SAVE_DOCUMENT_OP
            }
            case Failure(other) => {
                other.printStackTrace()
                fail( "unexpected condition occurred during test" )
            }
        }
        //@formatter:on
    }

    "Core Processor" should "successfully update tenants" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )
        val newTenants : List[ String ] = List( "tenant-1", "tenant-2" )

        when( repository.updateTenant( doc.documentId, newTenants( 0 ) ) ).thenReturn( Future.successful( newTenants( 0 ) ) )
        when( repository.updateTenant( doc.documentId, newTenants( 1 ) ) ).thenReturn( Future.successful( newTenants( 1 ) ) )
        when( searchIndex.updateTenants( doc.documentId, newTenants.toSet ) ).thenReturn( Future.successful() )

        val processor = new CoreProcessor( repository, searchIndex, null )
        val result = processor.updateTenants( doc, newTenants.toSet ).synchronously()

        result match {
            case Success( tenants : Set[ String ] ) => tenants shouldBe newTenants.toSet
            case _ => fail()
        }
    }

    "Core Processor" should "handle datastore failures updating tenants" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )
        val newTenants : List[ String ] = List( "tenant-1", "tenant-2" )

        when( repository.updateTenant( doc.documentId, newTenants( 0 ) ) ).thenReturn( Future.successful( newTenants( 0 ) ) )
        when( repository.updateTenant( doc.documentId, newTenants( 1 ) ) ).thenReturn( Future.failed( new ConnectException() ) )
        when( searchIndex.updateTenants( doc.documentId, newTenants.toSet ) ).thenReturn( Future.successful() )

        val processor = new CoreProcessor( repository, searchIndex, null )
        val result = processor.updateTenants( doc, newTenants.toSet ).synchronously()

        result match {
            case Failure( e : DartDatastoreException ) => e.operation shouldBe DartDatastore.UPDATE_TENANT_OP
            case _ => fail()
        }
    }

    "Core Processor" should "handle search index failures updating tenants" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a" )
        val newTenants : List[ String ] = List( "tenant-1", "tenant-2" )

        when( repository.updateTenant( doc.documentId, newTenants( 0 ) ) ).thenReturn( Future.successful( newTenants( 0 ) ) )
        when( repository.updateTenant( doc.documentId, newTenants( 1 ) ) ).thenReturn( Future.successful( newTenants( 1 ) ) )
        when( searchIndex.updateTenants( doc.documentId, newTenants.toSet ) ).thenReturn( Future.failed( new SearchIndexException( s"${doc.documentId} - tenants update failed", UPDATE_TENANTS_OP, new ConnectException() ) ) )

        val processor = new CoreProcessor( repository, searchIndex, null )
        val result = processor.updateTenants( doc, newTenants.toSet ).synchronously()

        result match {
            case Failure( e : SearchIndexException ) => e.operation shouldBe SearchIndex.UPDATE_TENANTS_OP
            case _ => fail()
        }
    }

}
