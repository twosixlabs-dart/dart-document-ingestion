package com.twosixlabs.dart.pipeline.stage

import com.twosixlabs.cdr4s.annotations.OffsetTag
import com.twosixlabs.cdr4s.core.{CdrAnnotation, OffsetTagAnnotation}
import com.twosixlabs.dart.AnnotatorFactory
import com.twosixlabs.dart.datastore.DartDatastore
import com.twosixlabs.dart.pipeline.command.{AnnotationProcessingError, AnnotatorStageShutdown, AnnotatorStatus, DocumentProcessing, StartAnnotatorWorkflow}
import com.twosixlabs.dart.pipeline.exception.{DartDatastoreException, SearchIndexException}
import com.twosixlabs.dart.service.SearchIndex
import com.twosixlabs.dart.service.annotator.{Annotator, AnnotatorDef}
import com.twosixlabs.dart.test.base.{AkkaTestBase, AsyncTest}
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE

import scala.concurrent.Future
import scala.util.{Failure, Success}

class AnnotatorStageManagerTestSuite extends AkkaTestBase {

    private val datastore : DartDatastore = mock[ DartDatastore ]
    private val searchIndex : SearchIndex = mock[ SearchIndex ]

    private val provider : AnnotatorFactory = mock[ AnnotatorFactory ]
    private val annotatorOne : Annotator = mock[ Annotator ]
    private val annotatorTwo : Annotator = mock[ Annotator ]

    private val annotationOne : CdrAnnotation[ _ ] = OffsetTagAnnotation( "annotator-one", "1", List( OffsetTag( 0, 1, None, "tag", None ), OffsetTag( 0, 1, None, "tag", None ) ) )
    private val annotationTwo : CdrAnnotation[ _ ] = OffsetTagAnnotation( "annotator-two", "1", List( OffsetTag( 0, 1, None, "tag", None ), OffsetTag( 0, 1, None, "tag", None ) ) )

    override def afterEach( ) = {
        reset( datastore )
        reset( searchIndex )
        reset( provider )
        reset( annotatorOne )
    }

    override def beforeEach( ) = {
        when( annotatorOne.label ).thenReturn( "annotator-one" )
        when( annotatorTwo.label ).thenReturn( "annotator-two" )
    }

    "annotator stage" should "do scenario: one successful annotator" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators.head ) ).thenReturn( annotatorOne )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 2 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 1
        statuses.count( ann => ann.successful && ann.hasResult ) shouldBe 1
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: one successful annotator that produces no result" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators.head ) ).thenReturn( annotatorOne )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( None ) )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 2 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 1
        statuses.count( ann => ann.successful && !ann.hasResult ) shouldBe 1
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: multiple successful annotators" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ),
                                                      AnnotatorDef( "annotator-two", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators( 0 ) ) ).thenReturn( annotatorOne )
        when( provider.newAnnotator( annotators( 1 ) ) ).thenReturn( annotatorTwo )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when( annotatorTwo.annotate( doc ) ).thenReturn( Success( Some( annotationTwo ) ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( datastore.addAnnotation( doc, annotationTwo ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationTwo ) ).thenReturn( Future.successful() )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 3 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 2
        statuses.count( ann => ann.successful && ann.hasResult ) shouldBe 2
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: multiple successful annotations with one result and one without" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ),
                                                      AnnotatorDef( "annotator-two", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators( 0 ) ) ).thenReturn( annotatorOne )
        when( provider.newAnnotator( annotators( 1 ) ) ).thenReturn( annotatorTwo )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when( annotatorTwo.annotate( doc ) ).thenReturn( Success( None ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 3 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 2
        statuses.count( ann => ann.successful && ann.hasResult ) shouldBe 1
        statuses.count( ann => ann.successful && !ann.hasResult ) shouldBe 1
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: one successful annotator, another failed annotator" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ),
                                                      AnnotatorDef( "annotator-two", "localhost", 45000, false, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators( 0 ) ) ).thenReturn( annotatorOne )
        when( provider.newAnnotator( annotators( 1 ) ) ).thenReturn( annotatorTwo )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when( annotatorTwo.annotate( doc ) ).thenReturn( Failure( new Exception() ), Failure( new Exception() ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 4 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 2
        statuses.count( _.successful ) shouldBe 1
        messages.contains( AnnotationProcessingError( doc, "annotator-two", Annotator.ANNOTATE_OP, new Exception() ) )
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: both annotators succeed after retries" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ),
                                                      AnnotatorDef( "annotator-two", "localhost", 45000, false, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators( 0 ) ) ).thenReturn( annotatorOne )
        when( provider.newAnnotator( annotators( 1 ) ) ).thenReturn( annotatorTwo )
        when( annotatorOne.annotate( doc ) ).thenReturn( Failure( new Exception() ), Success( Some( annotationOne ) ) )
        when( annotatorTwo.annotate( doc ) ).thenReturn( Failure( new Exception() ), Success( Some( annotationTwo ) ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( datastore.addAnnotation( doc, annotationTwo ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationTwo ) ).thenReturn( Future.successful() )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 3 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 2
        statuses.count( _.successful ) shouldBe 2
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: one annotator fails for persistence error" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ),
                                                      AnnotatorDef( "annotator-two", "localhost", 45000, false, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators( 0 ) ) ).thenReturn( annotatorOne )
        when( provider.newAnnotator( annotators( 1 ) ) ).thenReturn( annotatorTwo )
        when( annotatorOne.annotate( doc ) ).thenReturn( Failure( new Exception() ), Success( Some( annotationOne ) ) )
        when( annotatorTwo.annotate( doc ) ).thenReturn( Failure( new Exception() ), Success( Some( annotationTwo ) ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( datastore.addAnnotation( doc, annotationTwo ) ).thenReturn( Future.failed( new DartDatastoreException( "save failed", DartDatastore.ADD_ANNOTATION_OP ) ) )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationTwo ) ).thenReturn( Future.successful() )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 4 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 2
        statuses.count( _.successful ) shouldBe 1
        messages.contains( AnnotationProcessingError( doc, "annotator-two", DartDatastore.ADD_ANNOTATION_OP, new DartDatastoreException( "save failed", DartDatastore.ADD_ANNOTATION_OP ) ) )
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: one annotator fails for search indexer failure" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ),
                                                      AnnotatorDef( "annotator-two", "localhost", 45000, false, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators( 0 ) ) ).thenReturn( annotatorOne )
        when( provider.newAnnotator( annotators( 1 ) ) ).thenReturn( annotatorTwo )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when( annotatorTwo.annotate( doc ) ).thenReturn( Success( Some( annotationTwo ) ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( datastore.addAnnotation( doc, annotationTwo ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationTwo ) ).thenReturn( Future.failed( new SearchIndexException( "search index failed", SearchIndex.UPDATE_ANNOTATION_OP ) ) )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 4 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 2
        statuses.count( _.successful ) shouldBe 1
        messages.contains( AnnotationProcessingError( doc, "annotator-two", SearchIndex.UPDATE_ANNOTATION_OP, new SearchIndexException( "search index failed", SearchIndex.UPDATE_ANNOTATION_OP ) ) )
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: retry persistence if datastore fails" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators.head ) ).thenReturn( annotatorOne )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.failed( new DartDatastoreException( "failed", DartDatastore.ADD_ANNOTATION_OP ) ), Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 2 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 1
        statuses.count( ann => ann.successful && ann.hasResult ) shouldBe 1
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: retry persistence if search index fails" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )

        when( provider.newAnnotator( annotators.head ) ).thenReturn( annotatorOne )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when( datastore.addAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.failed( new SearchIndexException( "failed", "addAnnotation" ) ), Future.successful() )
        // retries

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 2 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 1
        statuses.count( ann => ann.successful && ann.hasResult ) shouldBe 1
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "do scenario: fail if persistence attempts are greater than the max retries" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", annotations = List() )

        val exception = new DartDatastoreException( "failed", DartDatastore.ADD_ANNOTATION_OP )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )
        // @formatter:off
        when( provider.newAnnotator( annotators.head ) ).thenReturn( annotatorOne )
        when( annotatorOne.annotate( doc ) ).thenReturn( Success( Some( annotationOne ) ) )
        when(
            datastore
                .addAnnotation( doc, annotationOne ) )
                .thenReturn( Future.failed( exception ), Future.failed( exception ), Future.failed( exception ), Future.failed( exception ), Future.failed( exception ) )
        when( searchIndex.updateAnnotation( doc, annotationOne ) ).thenReturn( Future.successful() )
        // @formatter:on

        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 3 )
        val statuses = getStatuses( messages )
        statuses.size shouldBe 1
        statuses.count( _.successful ) shouldBe 0
        messages.contains( AnnotationProcessingError( doc, "annotator-one", DartDatastore.ADD_ANNOTATION_OP, exception ) )
        messages.contains( AnnotatorStageShutdown( doc.documentId ) ) shouldBe true
    }

    "annotator stage" should "shold not annotate documents with no extracted text" taggedAs ( AsyncTest ) in {
        val doc = CDR_TEMPLATE.copy( documentId = "1a", extractedText = null, annotations = List() )

        val exception = new DartDatastoreException( "failed", DartDatastore.ADD_ANNOTATION_OP )

        val annotators : List[ AnnotatorDef ] = List( AnnotatorDef( "annotator-one", "localhost", 45000, true, "api/v1/annotate/cdr", 1, 300 ) )


        val root = testKit.createTestProbe[ DocumentProcessing ]
        val manager = testKit.spawn( AnnotatorStageManager( doc.documentId, root.ref, provider, annotators.toSet, datastore, searchIndex, 5, 1 ) )

        manager ! StartAnnotatorWorkflow( doc )

        val messages : Seq[ DocumentProcessing ] = root.receiveMessages( 1 )
        messages.head shouldBe AnnotatorStageShutdown( doc.documentId )

    }

    private def getStatuses( messages : Seq[ DocumentProcessing ] ) : Seq[ AnnotatorStatus ] = {
        messages.filter( _.getClass == classOf[ AnnotatorStatus ] ).asInstanceOf[ Seq[ AnnotatorStatus ] ]
    }

}
