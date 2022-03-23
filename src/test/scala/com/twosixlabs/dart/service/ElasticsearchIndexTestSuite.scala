package com.twosixlabs.dart.service

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.twosixlabs.cdr4s.annotations.{FacetScore, OffsetTag}
import com.twosixlabs.cdr4s.core.{CdrAnnotation, FacetAnnotation, OffsetTagAnnotation, TextAnnotation}
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE
import org.scalatest.Ignore

import java.lang.Thread.sleep
import java.util.concurrent.TimeUnit.SECONDS
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

@Ignore // "need to come up with a way to test against ES as part of unit test"
class ElasticsearchIndexTestSuite extends StandardTestBase3x {

    private val TEST_HOST : String = "localhost"
    private val TEST_PORT : Int = 9200

    private implicit val ec : ExecutionContext = scala.concurrent.ExecutionContext.global

    val client = ElasticClient( JavaClient( ElasticProperties( s"http://${TEST_HOST}:${TEST_PORT}" ) ) )

    "Elasticsearch search indexer" should "insert a document" in {
        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )

        val doc = CDR_TEMPLATE.copy( documentId = Random.alphanumeric.take( 16 ).mkString )
        Await.result( searchIndex.upsertDocument( doc ), Duration( 3, SECONDS ) )

        sleep( 1500 )

        val insertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )

        insertResult.documentId shouldBe doc.documentId
        insertResult.extractedText shouldBe doc.extractedText
        insertResult.extractedNtriples shouldBe doc.extractedNtriples
    }

    "Elasticsearch search indexer" should "add annotations to a document with no existing annotations" in {
        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )

        val doc = CDR_TEMPLATE.copy( documentId = Random.alphanumeric.take( 16 ).mkString )
        Await.result( searchIndex.upsertDocument( doc ), Duration( 3, SECONDS ) )

        sleep( 1500 )

        val annotation : CdrAnnotation[ _ ] = TextAnnotation( "test-text", "1.0", "my content" )
        Await.result( searchIndex.updateAnnotation( doc, annotation ), Duration( 3, SECONDS ) )

        sleep( 1500 )

        val upsertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
        upsertResult.annotations.size shouldBe 1
    }

    "Elasticsearch search indexer" should "add annotations to a document with pre-existing annotations" in {
        val searchIndex : ElasticsearchIndex = new ElasticsearchIndex( client )

        val doc = CDR_TEMPLATE.copy( documentId = Random.alphanumeric.take( 16 ).mkString, annotations = List( FacetAnnotation( "factiva-existing", "1", List( FacetScore( "test", None ), FacetScore( "test 2", Some( 1.0 ) ) ), CdrAnnotation.STATIC ) ) )
        Await.result( searchIndex.upsertDocument( doc ), Duration( 3, SECONDS ) )

        sleep( 1500 )

        val annotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "test-tags", "1.0", List( OffsetTag( 0, 1, Some( "test" ), "ORG", None ), OffsetTag( 0, 1, Some( "test 2" ), "GPE", None ) ) )
        Await.result( searchIndex.updateAnnotation( doc, annotation ), Duration( 3, SECONDS ) )

        sleep( 1500 )

        val upsertResult = Await.result( searchIndex.get( doc.documentId ), Duration( 3, SECONDS ) )
        upsertResult.annotations.count( _.getClass == classOf[ OffsetTagAnnotation ] ) shouldBe 1
        upsertResult.annotations.count( _.getClass == classOf[ FacetAnnotation ] ) shouldBe 1
    }

}
