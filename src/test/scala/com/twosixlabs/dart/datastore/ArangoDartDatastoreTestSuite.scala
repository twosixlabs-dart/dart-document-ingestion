package com.twosixlabs.dart.datastore

import com.twosixlabs.cdr4s.annotations.OffsetTag
import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument, OffsetTagAnnotation}
import com.twosixlabs.dart.arangodb.exception.ArangodbException
import com.twosixlabs.dart.arangodb.tables.{CanonicalDocsTable, TenantDocsTables}
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE
import com.twosixlabs.dart.utils.AsyncDecorators.DecoratedFuture
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Future

class ArangoDartDatastoreTestSuite extends StandardTestBase3x with BeforeAndAfterAll {

    val canonicalDocsTable : CanonicalDocsTable = mock[ CanonicalDocsTable ]
    val tenantsTable : TenantDocsTables = mock[ TenantDocsTables ]

    override def beforeAll( ) : Unit = reset( canonicalDocsTable, tenantsTable )

    "ArangoDB CDR Datastore" should "save a doc to the canonical docs table" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )

        when( canonicalDocsTable.upsert( doc ) ).thenReturn( Future.successful() )

        val result = datastore.saveDocument( doc ).synchronously()
        result.isSuccess shouldBe true
    }

    "ArangoDB CDR Datastore" should "fail to save a doc from the canonical docs table" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )

        when( canonicalDocsTable.upsert( doc ) ).thenReturn( Future.failed( new Exception() ) )

        val result = datastore.saveDocument( doc ).synchronously()
        result.isSuccess shouldBe false
    }

    "ArangoDB CDR Datastore" should "retrieve an existing doc from the canonical docs table" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )

        when( canonicalDocsTable.getDocument( doc.documentId ) ).thenReturn( Future.successful( Some( doc ) ) )

        val result = datastore.getByDocId( doc.documentId ).synchronously()
        result.isSuccess shouldBe true
        result.get.isDefined shouldBe true
        result.get.get shouldBe doc
    }

    "ArangoDB CDR Datastore" should "return nothing for a non-existing doc from the canonical docs table" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )

        when( canonicalDocsTable.getDocument( doc.documentId ) ).thenReturn( Future.successful( None ) )

        val result = datastore.getByDocId( doc.documentId ).synchronously()
        result.isSuccess shouldBe true
        result.get.isDefined shouldBe false
    }

    "ArangoDB CDR Datastore" should "upsert an annotation to the canonical docs table" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )

        val annotation : CdrAnnotation[ _ ] = OffsetTagAnnotation( "tags", "1.0", List( OffsetTag( 0, 1, None, "N", None ), OffsetTag( 2, 3, None, "V", None ) ) )
        when( canonicalDocsTable.upsertAnnotation( doc.documentId, annotation ) ).thenReturn( Future.successful() )

        val result = datastore.addAnnotation( doc, annotation ).synchronously()
        result.isSuccess shouldBe true
    }

    "ArangoDB CDR Datastore" should "perform a positive existence check on the canonical docs table" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )

        when( canonicalDocsTable.exists( doc.documentId ) ).thenReturn( Future.successful( true ) )

        val result = datastore.exists( doc.documentId ).synchronously()
        result.isSuccess shouldBe true
        result.get shouldBe true
    }

    "ArangoDB CDR Datastore" should "perform a negative existence check on the canonical docs table" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = "1a" )

        when( canonicalDocsTable.exists( doc.documentId ) ).thenReturn( Future.successful( false ) )

        val result = datastore.exists( doc.documentId ).synchronously()
        result.isSuccess shouldBe true
        result.get shouldBe false
    }

    "ArangoDB CDR Datastore" should "return a success when adding a document to a tenant already containing it" in {
        val datastore : DartDatastore = new ArangoDartDatastore( canonicalDocsTable, tenantsTable )

        when( tenantsTable.addDocToTenant( "test-tenant", "1a" ) )
          .thenReturn( Future.failed( ArangodbException( s"test-tenant - Tenant already exists", "test-table", new IllegalStateException( "Tenant already exists" ) ) ) )

        val result = datastore.updateTenant( "1a", "test-tenant" ).synchronously()
        result.isSuccess shouldBe true
        result.get shouldBe "test-tenant"
    }

}
