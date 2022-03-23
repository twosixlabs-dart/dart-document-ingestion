package com.twosixlabs.dart.service

import com.twosixlabs.dart.test.base.StandardTestBase3x
import org.scalatest.{BeforeAndAfterEach, Ignore}

// saving the code but the integration test implementation wasn't fully baked out, will return to this later...
@Ignore
class ElasticsearchIndexIntegrationTestSuite extends StandardTestBase3x with BeforeAndAfterEach {
    //
    //    import com.sksamuel.elastic4s.ElasticDsl._
    //
    //    implicit val executionContext : ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
    //
    //    val inGitlab : Boolean = System.getenv( "RUN_IN_DOCKER" ) != null
    //
    //    val testConfig = {
    //        if ( inGitlab ) ConfigFactory.parseResources( s"env/gitlab-integration.conf" ).resolve()
    //        else ConfigFactory.parseResources( s"env/local-integration.conf" ).resolve()
    //    }
    //
    //    private val TEST_HOST : String = testConfig.getString( "es.host" )
    //    private val TEST_PORT : Int = testConfig.getInt( "es.port" )
    //
    //    private implicit val ec : ExecutionContext = scala.concurrent.ExecutionContext.global
    //
    //    val client : ElasticClient = ElasticClient( JavaClient( ElasticProperties( s"http://${TEST_HOST}:${TEST_PORT}" ) ) )
    //
    //    val testDocId : String = "634f221da953e225ff9669bd620415df"
    //
    //    override def afterEach( ) {
    //        client.execute( deleteById( "cdr_search", testDocId ) )
    //        client.execute( deleteById( "cdr_search", "cd8703d44edf93848e945e3e0c6cd4b7" ) )
    //        sleep( 1500 )
    //    }
    //
    //    "Elasticsearch search indexer" should "be able to index and then update annotations for a real doc right in a row" in {
    //        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )
    //
    //        val cdrJson = Resource.getAsString( "test-cdr.json" )
    //        val doc = DART_JSON.unmarshalCdr( cdrJson ).getOrElse( fail( "cdr could not be unmarshalled" ) )
    //
    //        val cdrAnnotationJson1 = Resource.getAsString( "test-annotation-1.json" )
    //        val cdrAnnotation1 = DART_JSON.unmarshalAnnotation( cdrAnnotationJson1 ).get
    //        val cdrAnnotationJson2 = Resource.getAsString( "test-annotation-2.json" )
    //        val cdrAnnotation2 = DART_JSON.unmarshalAnnotation( cdrAnnotationJson2 ).get
    //        val cdrAnnotationJson3 = Resource.getAsString( "test-annotation-3.json" )
    //        val cdrAnnotation3 = DART_JSON.unmarshalAnnotation( cdrAnnotationJson3 ).get
    //
    //        Await.result( searchIndex.insert( doc ), Duration( 3, SECONDS ) )
    //
    //        Await.result( searchIndex.upsert( doc, cdrAnnotation1 ), Duration( 3, SECONDS ) )
    //        Await.result( searchIndex.upsert( doc, cdrAnnotation2 ), Duration( 3, SECONDS ) )
    //        Await.result( searchIndex.upsert( doc, cdrAnnotation3 ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1000 )
    //
    //        val upsertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //        upsertResult.annotations.size shouldBe 3
    //        upsertResult.annotations.count( _.getClass == classOf[ FacetAnnotation ] ) shouldBe 1
    //        upsertResult.annotations.count( _.getClass == classOf[ OffsetTagAnnotation ] ) shouldBe 2
    //        upsertResult.annotations.count( _.label == cdrAnnotation1.label ) shouldBe 1
    //        upsertResult.annotations.count( _.label == cdrAnnotation2.label ) shouldBe 1
    //        upsertResult.annotations.count( _.label == cdrAnnotation3.label ) shouldBe 1
    //        upsertResult.annotations should contain( cdrAnnotation1 )
    //        upsertResult.annotations should contain( cdrAnnotation2 )
    //        upsertResult.annotations should contain( cdrAnnotation3 )
    //    }
    //
    //    "Elasticsearch search indexer" should "insert a document" in {
    //        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )
    //
    //        val doc = CDR_TEMPLATE.copy( documentId = testDocId )
    //
    //        // Ensure relevant doc is not yet in index
    //        an[ NoSuchElementException ] should be thrownBy ( Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) ) )
    //
    //        Await.result( searchIndex.insert( doc ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val insertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //
    //        insertResult.documentId shouldBe doc.documentId
    //        insertResult.extractedText shouldBe doc.extractedText
    //        insertResult.extractedNtriples shouldBe doc.extractedNtriples
    //    }
    //
    //    "Elasticsearch search indexer" should "add annotations to a document with no existing annotations" in {
    //        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )
    //
    //        val doc = CDR_TEMPLATE.copy( documentId = testDocId )
    //
    //        // Confirm doc has been deleted
    //        an[ NoSuchElementException ] should be thrownBy ( Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) ) )
    //
    //        // Ensure doc is in index
    //        Await.result( searchIndex.insert( doc ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val insertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //
    //        insertResult.documentId shouldBe doc.documentId
    //        insertResult.extractedText shouldBe doc.extractedText
    //        insertResult.extractedNtriples shouldBe doc.extractedNtriples
    //
    //        val annotation : CdrAnnotation[ _ ] = FacetAnnotation( "test-label", "1.0.0", List( FacetConfidence( "value-1", None ), FacetConfidence( "value-2", None ) ), CdrAnnotation.DERIVED )
    //        Await.result( searchIndex.upsert( doc, annotation ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val upsertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //        upsertResult.annotations.size shouldBe 1
    //    }
    //
    //    "Elasticsearch search indexer" should "add annotations to a document with no annotations field at all" in {
    //        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )
    //
    //        val doc = CDR_TEMPLATE.copy( documentId = testDocId, annotations = null )
    //
    //        // Confirm doc has been deleted
    //        an[ NoSuchElementException ] should be thrownBy ( Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) ) )
    //
    //        // Ensure doc is in index
    //        Await.result( searchIndex.insert( doc ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val insertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //
    //        insertResult.documentId shouldBe doc.documentId
    //        insertResult.extractedText shouldBe doc.extractedText
    //        insertResult.extractedNtriples shouldBe doc.extractedNtriples
    //
    //        val annotation : CdrAnnotation[ _ ] = FacetAnnotation( "test-label", "1.0.0", List( FacetConfidence( "value-1", None ), FacetConfidence( "value-2", None ) ), CdrAnnotation.DERIVED )
    //        Await.result( searchIndex.upsert( doc, annotation ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val upsertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //        upsertResult.annotations.size shouldBe 1
    //    }
    //
    //    "Elasticsearch search indexer" should "add annotations to a document with pre-existing annotations" in {
    //        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )
    //
    //        val doc = CDR_TEMPLATE.copy( documentId = testDocId, annotations = List( FacetAnnotation( "factiva-existing", "1", List( FacetConfidence( "test", None ), FacetConfidence( "test 2", Some( 1.0 ) ) ), CdrAnnotation.STATIC ) ) )
    //        Await.result( searchIndex.insert( doc ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val annotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "test-tags", "1.0", List( OffsetTag( 0, 1, Some( "test" ), "ORG" ), OffsetTag( 0, 1, Some( "test 2" ), "GPE" ) ) )
    //        Await.result( searchIndex.upsert( doc, annotation ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val upsertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //        upsertResult.annotations.count( _.getClass == classOf[ OffsetTagAnnotation ] ) shouldBe 1
    //        upsertResult.annotations.count( _.getClass == classOf[ FacetAnnotation ] ) shouldBe 1
    //        upsertResult.annotations.size shouldBe 2
    //    }
    //
    //    "Elasticsearch search indexer" should "add replace annotation with the same label" in {
    //        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )
    //
    //        val doc = CDR_TEMPLATE.copy( documentId = testDocId,
    //                                     annotations = List( FacetAnnotation( "factiva-existing", "1", List( FacetConfidence( "test", None ), FacetConfidence( "test 2", Some( 1.0 ) ) ), CdrAnnotation.STATIC ),
    //                                                         OffsetTagAnnotation( "factiva-existing-2", "1", List( OffsetTag( 0, 10, None, "tag-1" ), OffsetTag( 15, 25, None, "tag-2" ) ), CdrAnnotation.DERIVED ),
    //                                                         OffsetTagAnnotation( "factiva-existing-3", "1", List( OffsetTag( 2, 8, None, "tag-a" ), OffsetTag( 17, 23, None, "tag-b" ) ), CdrAnnotation.STATIC ) ) )
    //        Await.result( searchIndex.insert( doc ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val annotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "factiva-existing-2", "2.0", List( OffsetTag( 0, 1, Some( "test" ), "ORG" ), OffsetTag( 0, 1, Some( "test 2" ), "GPE" ) ) )
    //        Await.result( searchIndex.upsert( doc, annotation ), Duration( 3, SECONDS ) )
    //
    //        sleep( 1500 )
    //
    //        val upsertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
    //        upsertResult.annotations.size shouldBe 3
    //        upsertResult.annotations.count( _.getClass == classOf[ FacetAnnotation ] ) shouldBe 1
    //        upsertResult.annotations.count( _.getClass == classOf[ OffsetTagAnnotation ] ) shouldBe 2
    //        upsertResult.annotations.count( _.label == "factiva-existing" ) shouldBe 1
    //        upsertResult.annotations.count( _.label == "factiva-existing-2" ) shouldBe 1
    //        upsertResult.annotations.count( _.label == "factiva-existing-3" ) shouldBe 1
    //        upsertResult.annotations.find( _.label == "factiva-existing-2" ).get.asInstanceOf[ OffsetTagAnnotation ].content.exists( _.tag == "ORG" )
    //        upsertResult.annotations.find( _.label == "factiva-existing-2" ).get.asInstanceOf[ OffsetTagAnnotation ].version shouldBe "2.0"
    //    }

}
