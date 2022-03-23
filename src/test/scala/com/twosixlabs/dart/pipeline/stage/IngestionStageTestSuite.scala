package com.twosixlabs.dart.pipeline.stage

import com.twosixlabs.dart.arangodb.Serialization.CDR_JSON_FORMAT
import com.twosixlabs.dart.pipeline.command.{AddNew, DocumentProcessing, DocumentProcessingError, Duplicate, NoOp, StartIngestion, UpdateExisting}
import com.twosixlabs.dart.pipeline.processor.IngestionProcessor
import com.twosixlabs.dart.service.Languages
import com.twosixlabs.dart.test.base.AkkaTestBase
import com.twosixlabs.dart.test.utils.JsonTestUtils.toObjectNode
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE

import scala.concurrent.Future
import scala.util.Success

class IngestionStageTestSuite extends AkkaTestBase {

    private val processor : IngestionProcessor = mock[ IngestionProcessor ]

    override def afterEach( ) : Unit = reset( processor )

    "Ingestion Stage" should "identify a completely new document for processing" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )

        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( (None, Set()) ) )
        when( processor.mergeOverrides( CDR_TEMPLATE, Overrides() ) ).thenReturn( CDR_TEMPLATE )
        when( processor.checkDuplicates( CDR_TEMPLATE ) ).thenReturn( Success( List() ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json )

        probe.expectMessageType[ AddNew ]
    }

    "Ingestion Stage" should "properly overlay update input data on a new document" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val updates = Overrides( labels = Set( "new-label" ) )

        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( (None, Set()) ) )
        when( processor.mergeOverrides( CDR_TEMPLATE, updates ) ).thenReturn( CDR_TEMPLATE.copy( labels = updates.labels ) )
        when( processor.checkDuplicates( * ) ).thenReturn( Success( List() ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val result = probe.expectMessageType[ AddNew ]
        result.doc.labels shouldBe updates.labels
    }

    "Ingestion Stage" should "not do anything if there are no changes to document data or tenants" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val updates : Overrides = Overrides()

        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn {
            Future.successful( (Some( CDR_TEMPLATE ), Set( "test-tenant" )) )
        }
        when( processor.mergeOverrides( CDR_TEMPLATE, Overrides() ) ).thenReturn( CDR_TEMPLATE )
        when( processor.checkTenantUpdates( *, * ) ).thenReturn( Set() )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val result = probe.expectMessageType[ NoOp ]
        result.docId shouldBe CDR_TEMPLATE.documentId
    }

    "Ingestion Stage" should "signal an update if there are document content updates to an existing doc" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val newLabels : Set[ String ] = Set( "new-label" )
        val updates : Overrides = Overrides( labels = newLabels )

        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn {
            Future.successful( (Some( CDR_TEMPLATE ), Set( "test-tenant" )) )
        }
        when( processor.mergeOverrides( CDR_TEMPLATE, updates ) ).thenReturn( CDR_TEMPLATE.copy( labels = newLabels ) )
        when( processor.checkTenantUpdates( *, * ) ).thenReturn( Set() )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val result = probe.expectMessageType[ UpdateExisting ]
        result.doc.labels shouldBe newLabels
    }

    "Ingestion Stage" should "signal an update if there are tenant updates to an existing doc" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val newTenants : Set[ String ] = Set( "new-label" )
        val updates : Overrides = Overrides( tenants = newTenants )

        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn {
            Future.successful( (Some( CDR_TEMPLATE ), Set( "test-tenant" )) )
        }
        when( processor.mergeOverrides( CDR_TEMPLATE, updates ) ).thenReturn( CDR_TEMPLATE )
        when( processor.checkTenantUpdates( *, * ) ).thenReturn( newTenants )


        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val result = probe.expectMessageType[ UpdateExisting ]

        result.updates.tenants shouldBe newTenants
        result.updates.runAnnotators shouldBe false
    }

    "Ingestion Stage" should "indicate that annotators need to be rerun for a document whose content has changed" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val updates : Overrides = Overrides()

        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn {
            Future.successful( (Some( CDR_TEMPLATE ), Set( "test-tenant" )) )
        }
        when( processor.mergeOverrides( CDR_TEMPLATE, updates ) ).thenReturn( CDR_TEMPLATE.copy( extractedMetadata = CDR_TEMPLATE.extractedMetadata.copy( title = "new title" ) ) )
        when( processor.checkTenantUpdates( *, * ) ).thenReturn( Set() )
        when( processor.reannotate( *, * ) ).thenReturn( true )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val result = probe.expectMessageType[ UpdateExisting ]
        result.updates.runAnnotators shouldBe true
    }

    "Ingestion Stage" should "identify a duplicate and not update the original doc if there are no metadata updates" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val updates : Overrides = Overrides()

        val duplicateDoc = CDR_TEMPLATE.copy( documentId = "2" )

        when( processor.extractContent( *, * ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( * ) ).thenReturn( Future.successful( (None, Set()) ), Future.successful( (Some( duplicateDoc ), Set()) ) )
        when( processor.mergeOverrides( *, * ) ).thenReturn( CDR_TEMPLATE, duplicateDoc )
        when( processor.checkDuplicates( * ) ).thenReturn( Success( List( duplicateDoc.documentId ) ) )
        when( processor.reannotate( *, * ) ).thenReturn( false )
        when( processor.checkTenantUpdates( *, * ) ).thenReturn( Set() )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val duplicateNotification = Duplicate( CDR_TEMPLATE, List( duplicateDoc.documentId ), updates ) // annotators will be flipped to true because it's a new doc
        val noUpdateNotification = NoOp( duplicateDoc.documentId )
        val x = probe.receiveMessages( 2 )

        x should contain( noUpdateNotification )
        x should contain( duplicateNotification )
    }

    "Ingestion Stage" should "identify a duplicate and update the original doc if there are no metadata updates" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val newTenants = Set( "new-tenant" )
        val updates : Overrides = Overrides( tenants = newTenants )

        val duplicateDocs = List( "2" )
        val duplicateDoc = CDR_TEMPLATE.copy( documentId = duplicateDocs.head )
        // identify duplicate...
        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( (None, Set()) ) )
        when( processor.mergeOverrides( CDR_TEMPLATE, updates ) ).thenReturn( CDR_TEMPLATE )
        when( processor.checkDuplicates( * ) ).thenReturn( Success( duplicateDocs ) )

        // look up the previously existing doc and apply updates
        when( processor.findExistingDoc( duplicateDoc.documentId ) ).thenReturn {
            Future.successful( (Some( duplicateDoc ), Set( "test-tenant" )) )
        }
        when( processor.mergeOverrides( duplicateDoc, updates ) ).thenReturn( duplicateDoc.copy( labels = newTenants ) )
        when( processor.reannotate( CDR_TEMPLATE, duplicateDoc ) ).thenReturn( false )
        when( processor.checkTenantUpdates( *, * ) ).thenReturn( newTenants )


        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val messages = probe.receiveMessages( 2 )

        messages.count( _.getClass == classOf[ Duplicate ] ) shouldBe 1
        messages.count( _.getClass == classOf[ UpdateExisting ] ) shouldBe 1
    }

    "Ingestion Stage" should "reject non English language documents (for now)" in {
        val doc = CDR_TEMPLATE.copy( extractedText = "Bom dia" )
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( doc ).get )

        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( doc ) )
        when( processor.checkLanguage( doc ) ).thenReturn( Languages.PORTUGUESE )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json )

        val error : DocumentProcessingError = probe.expectMessageType[ DocumentProcessingError ]
        error.exception shouldBe a[ UnsupportedOperationException ]
    }

    "[DEFECT][DART-1009] Ingestion Stage" should "not do anything if the only document in the duplicates list is the same document (for some reason)" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val updates : Overrides = Overrides()

        // identify duplicate...
        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( (None, Set()) ) )
        when( processor.mergeOverrides( CDR_TEMPLATE, updates ) ).thenReturn( CDR_TEMPLATE )
        when( processor.checkDuplicates( * ) ).thenReturn( Success( List( CDR_TEMPLATE.documentId ) ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val result = probe.expectMessageType[ AddNew ]
        result.docId shouldBe CDR_TEMPLATE.documentId
        result.updates.runAnnotators shouldBe true
    }

    "[DEFECT][DART-1009] Ingestion Stage" should "treat a document as new if it is the only document in the duplicate list and there are overrides" in {
        val json = toObjectNode( CDR_JSON_FORMAT.marshalCdr( CDR_TEMPLATE ).get )
        val newTenants = Set( "new-tenant" )
        val updates : Overrides = Overrides( tenants = newTenants )

        // identify duplicate...
        when( processor.extractContent( CDR_TEMPLATE.documentId, json ) ).thenReturn( Success( CDR_TEMPLATE ) )
        when( processor.checkLanguage( * ) ).thenReturn( Languages.ENGLISH )
        when( processor.findExistingDoc( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( (None, Set()) ) )
        when( processor.mergeOverrides( CDR_TEMPLATE, updates ) ).thenReturn( CDR_TEMPLATE )
        when( processor.checkDuplicates( * ) ).thenReturn( Success( List( CDR_TEMPLATE.documentId ) ) )

        val probe = testKit.createTestProbe[ DocumentProcessing ]
        val ingestionStage = testKit.spawn( IngestionStage( processor, probe.ref ) )
        ingestionStage ! StartIngestion( CDR_TEMPLATE.documentId, json, updates )

        val result = probe.expectMessageType[ AddNew ]
        result.docId shouldBe CDR_TEMPLATE.documentId
        result.updates.runAnnotators shouldBe true
        result.updates.tenants shouldBe newTenants
    }

}
