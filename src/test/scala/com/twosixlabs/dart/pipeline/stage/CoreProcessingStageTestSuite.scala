package com.twosixlabs.dart.pipeline.stage

import com.twosixlabs.cdr4s.core.{CdrAnnotation, TextAnnotation}
import com.twosixlabs.dart.pipeline.command.{AddNew, CoreProcessingComplete, DocumentProcessing, DocumentProcessingError, UpdateExisting}
import com.twosixlabs.dart.pipeline.exception.{DartDatastoreException, SearchIndexException}
import com.twosixlabs.dart.pipeline.processor.CoreProcessor
import com.twosixlabs.dart.pipeline.processor.Translations.Language.ENGLISH
import com.twosixlabs.dart.test.base.AkkaTestBase
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE

import scala.concurrent.Future

class CoreProcessingStageTestSuite extends AkkaTestBase {

    private val processor : CoreProcessor = mock[ CoreProcessor ]
    private val datastoreException : DartDatastoreException = new DartDatastoreException( "datastore exception", "datastore.op", new Exception() )
    private val searchIndexException : SearchIndexException = new SearchIndexException( "search exception", "search.index.op", new Exception() )

    override def afterEach( ) : Unit = reset( processor )

    "Core Processing Stage" should "process a brand new document with tenants" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document" )
        val tenants : Seq[ String ] = Seq( "tenant-1" )
        val updates : Overrides = Overrides( tenants = tenants.toSet )

        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.checkLanguage( doc ) ).thenReturn( ENGLISH )
        when( processor.fixText( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.executeProcessors( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.updateTenants( doc, tenants.toSet ) ).thenReturn( Future.successful( tenants.toSet ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! AddNew( doc, updates )

        probe.expectMessageType[ CoreProcessingComplete ]
    }

    "Core Processing Stage" should "process a brand new document with no tenants (global corpus only)" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document" )
        val updates : Overrides = Overrides()

        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.checkLanguage( doc ) ).thenReturn( ENGLISH )
        when( processor.fixText( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.executeProcessors( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! AddNew( doc, updates )

        probe.expectMessageType[ CoreProcessingComplete ]
    }

    "Core Processing Stage" should "update an existing document's tenants" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document" )
        val tenants : Set[ String ] = Set( "tenant-1" )
        val updates : Overrides = Overrides( tenants = tenants )

        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.checkLanguage( doc ) ).thenReturn( ENGLISH )
        when( processor.fixText( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.executeProcessors( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.updateTenants( doc, tenants ) ).thenReturn( Future.successful( tenants ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! UpdateExisting( doc, updates )

        probe.expectMessageType[ CoreProcessingComplete ]
    }

    "Core Processing Stage" should "handle datastore failures saving a new document" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document" )
        val updates : Overrides = Overrides()

        when( processor.saveCdr( doc ) ).thenReturn( Future.failed( datastoreException ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! AddNew( doc, updates )

        val result = probe.expectMessageType[ DocumentProcessingError ]
        result.exception shouldBe datastoreException
    }

    "Core Processing Stage" should "handle the reannotate flag and clear out all non-static annotations" in {
        val testAnnotations : List[ CdrAnnotation[ _ ] ] = {
            List( TextAnnotation( "label-1", "version-1", "content-1", classification = CdrAnnotation.DERIVED ),
                  TextAnnotation( "label-2", "version-2", "content-2", classification = CdrAnnotation.STATIC ) )

        }
        val originalDoc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document", annotations = testAnnotations )
        val expectedUpdatedDoc = originalDoc.copy( annotations = testAnnotations.filter( _.classification == CdrAnnotation.STATIC ) )

        when( processor.saveCdr( expectedUpdatedDoc ) ).thenReturn( Future.successful( expectedUpdatedDoc ) ) // we expect the annotations to be cleared
        when( processor.checkLanguage( * ) ).thenReturn( ENGLISH )
        when( processor.fixText( * ) ).thenReturn( Future.successful( expectedUpdatedDoc ) )
        when( processor.executeProcessors( * ) ).thenReturn( Future.successful( expectedUpdatedDoc ) )
        when( processor.saveCdr( * ) ).thenReturn( Future.successful( expectedUpdatedDoc ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]

        val updates : Overrides = Overrides( runAnnotators = true )
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! UpdateExisting( originalDoc, updates )

        val result = probe.expectMessageType[ CoreProcessingComplete ]
        result.updates.runAnnotators shouldBe true
        result.doc shouldBe expectedUpdatedDoc
    }

    "Core Processing Stage" should "handle search index failures saving a new document" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document" )
        val updates : Overrides = Overrides()

        when( processor.saveCdr( doc ) ).thenReturn( Future.failed( searchIndexException ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! AddNew( doc, updates )

        val result = probe.expectMessageType[ DocumentProcessingError ]
        result.exception shouldBe searchIndexException
    }

    "Core Processing Stage" should "handle errors if one of the preprocessors fails" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document" )
        val tenants : Seq[ String ] = Seq( "tenant-1" )
        val updates : Overrides = Overrides( tenants = tenants.toSet )

        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.checkLanguage( doc ) ).thenReturn( ENGLISH )
        when( processor.fixText( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.executeProcessors( doc ) ).thenReturn( Future.failed( datastoreException ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! AddNew( doc, updates )

        val result = probe.expectMessageType[ DocumentProcessingError ]
        result.exception shouldBe datastoreException
    }

    "Core Processing Stage" should "handle errors updating tenants" in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = "this is a new document" )
        val tenants : Set[ String ] = Set( "tenant-1" )
        val updates : Overrides = Overrides( tenants = tenants.toSet )

        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.checkLanguage( doc ) ).thenReturn( ENGLISH )
        when( processor.fixText( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.executeProcessors( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.saveCdr( doc ) ).thenReturn( Future.successful( doc ) )
        when( processor.updateTenants( doc, tenants ) ).thenReturn( Future.failed( datastoreException ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val coreProcessing = testKit.spawn( CoreProcessingStage( probe.ref, processor ) )
        coreProcessing ! AddNew( doc, updates )

        val result = probe.expectMessageType[ DocumentProcessingError ]
        result.exception shouldBe datastoreException
    }

}
