package com.twosixlabs.dart.datastore

import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument}
import com.twosixlabs.dart.arangodb.Arango
import com.twosixlabs.dart.arangodb.exception.ArangodbException
import com.twosixlabs.dart.arangodb.tables.{CanonicalDocsTable, TenantDocsTables}
import com.twosixlabs.dart.pipeline.exception.DartDatastoreException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


object ArangoDartDatastore {
    def apply( arangodb : Arango ) : ArangoDartDatastore = {
        new ArangoDartDatastore( new CanonicalDocsTable( arangodb ), new TenantDocsTables( arangodb ) )
    }
}

class ArangoDartDatastore( canonicalDocs : CanonicalDocsTable, tenants : TenantDocsTables ) extends DartDatastore {
    import com.twosixlabs.dart.datastore.DartDatastore._

    // TODO - handle in a more unified way
    implicit val ec : ExecutionContext = scala.concurrent.ExecutionContext.global

    override def getByDocId( docId : String ) : Future[ Option[ CdrDocument ] ] = {
        canonicalDocs.getDocument( docId ) transform {
            case success@Success( _ ) => success
            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${docId} get failed", GET_BY_ID_OP, e ) )
        }
    }

    override def saveDocument( doc : CdrDocument ) : Future[ Unit ] = {
        canonicalDocs.upsert( doc ) transform {
            case success@Success( _ ) => success
            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${doc.documentId} save failed", SAVE_DOCUMENT_OP, e ) )
        }
    }

    override def addAnnotation( doc : CdrDocument, annotation : CdrAnnotation[ _ ] ) : Future[ Unit ] = {
        canonicalDocs.upsertAnnotation( doc.documentId, annotation ) transform {
            case success@Success( _ ) => success
            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${doc.documentId} add annotation failed failed", ADD_ANNOTATION_OP, e ) )
        }
    }

    override def exists( docId : String ) : Future[ Boolean ] = {
        canonicalDocs.exists( docId ) transform {
            case success@Success( _ ) => success
            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${docId} existence check failed", EXISTS_OP, e ) )
        }
    }

    override def tenantMembershipsFor( docId : String ) : Future[ Set[ String ] ] = {
        tenants.getTenantsByDoc( docId ) transform {
            case Success( tenants : Iterator[ String ] ) => Success( tenants.toSet )
            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${docId} tenant memberships lookup failed", TENANT_MEMBERSHIPS_OP, e ) )
        }
    }

    override def updateTenant( docId : String, tenant : String ) : Future[ String ] = {
        tenants.addDocToTenant( tenant, docId ) transform {
            case Success( _ ) => Success( tenant )
            case Failure( e : ArangodbException ) if e.getMessage.contains( "Tenant already exists" ) => Success( tenant )
            case Failure( e : Throwable ) => Failure( new DartDatastoreException( s"${docId} failed to update tenant", UPDATE_TENANT_OP, e ) )
        }
    }
}
