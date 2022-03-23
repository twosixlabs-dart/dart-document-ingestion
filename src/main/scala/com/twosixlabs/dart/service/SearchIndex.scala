package com.twosixlabs.dart.service

import com.sksamuel.elastic4s.requests.searches.SearchResponse
import com.sksamuel.elastic4s.{ElasticClient, ElasticRequest, HttpEntity, HttpResponse, Response}
import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument}
import com.twosixlabs.dart.json.JsonFormat
import com.twosixlabs.dart.pipeline.exception.SearchIndexException
import com.twosixlabs.dart.serialization.json.JsonDataFormats.DART_JSON
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit.SECONDS
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object SearchIndex {
    val UPSERT_OP = "SearchIndex.upsertDocument"
    val UPDATE_ANNOTATION_OP = "SearchIndex.updateAnnotation"
    val UPDATE_TENANTS_OP = "SearchIndex.updateTenants"
    val GET_OPERATION = "SearchIndex"
}

trait SearchIndex {
    def upsertDocument( doc : CdrDocument ) : Future[ Unit ]

    def updateAnnotation( doc : CdrDocument, annotation : CdrAnnotation[ _ ] ) : Future[ Unit ]

    def updateTenants( docId : String, tenants : Set[ String ] ) : Future[ Unit ]
}

class ElasticsearchIndex( client : ElasticClient ) extends SearchIndex with JsonFormat {

    import com.twosixlabs.dart.service.SearchIndex._

    // TODO - handle in a more unified way
    implicit val executionContext = scala.concurrent.ExecutionContext.global

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private val INDEX : String = "cdr_search"

    override def upsertDocument( doc : CdrDocument ) : Future[ Unit ] = {
        val json : String = DART_JSON.marshalCdr( doc ).get

        import com.sksamuel.elastic4s.ElasticDsl._
        LOG.debug( s"upserting ${doc.documentId}... " )
        client.execute( updateById( INDEX, doc.documentId ).docAsUpsert( json ) ).transform {
            case Success( _ ) => Success()
            case Failure( e ) => Failure( new SearchIndexException( s"${doc.documentId} upsert failed", UPSERT_OP, e ) )
        }
        //@formatter:on
    }

    override def updateAnnotation( doc : CdrDocument, annotation : CdrAnnotation[ _ ] ) : Future[ Unit ] = {
        LOG.debug( s"upsert annotation ${doc.documentId}-${annotation.label}" )

        // because of limitations in the elastic4s API we have to drop down into a raw ES query
        val annotationJson = DART_JSON.marshalAnnotation( annotation ).get

        val updateAnnotationJson =
            s"""{
               |  "script": {
               |    "source": "if (ctx._source.annotations == null) { ctx._source.annotations = [ params.anno ] } else { ctx._source.annotations.add(params.anno) }",
               |    "lang": "painless",
               |    "params": {
               |      "anno": $annotationJson
               |    }
               |  }
               |}
               |""".stripMargin

        lowLevelEsRequest( updateAnnotationJson, "POST", s"/${INDEX}/_update/${doc.documentId}?retry_on_conflict=100" )
          .transform {
              case Success( r : HttpResponse ) => {
                  if ( LOG.isDebugEnabled() ) LOG.debug( r.toString )
                  Success()
              }
              case Failure( e ) => Failure( new SearchIndexException( s"${doc.documentId}-${annotation.label} - update failed", UPDATE_ANNOTATION_OP, e ) )
          }
        //@formatter:on
    }

    override def updateTenants( docId : String, tenants : Set[ String ] ) : Future[ Unit ] = {
        val tenantsArray = tenants.map( tenant => s""""${tenant}"""" ).mkString( ", " )
        val updateTenantsJson =
            s"""{
               |  "script": {
               |    "source": "ctx._source.tenants = params.tenants",
               |    "lang": "painless",
               |    "params": {
               |      "tenants": [${tenantsArray}]
               |    }
               |  }
               |}
               |""".stripMargin

        LOG.debug( updateTenantsJson )

        lowLevelEsRequest( updateTenantsJson, "POST", s"/${INDEX}/_update/${docId}?retry_on_conflict=100" )
          .transform {
              case Success( r : HttpResponse ) => {
                  if ( LOG.isDebugEnabled() ) LOG.debug( r.toString )
                  Success()
              }
              case Failure( e ) => Failure( new SearchIndexException( s"${docId} - tenants update failed - ${tenantsArray}", UPDATE_TENANTS_OP, e ) )
          }
        //@formatter:on
    }

    def get( docId : String ) : Future[ CdrDocument ] = {
        import com.sksamuel.elastic4s.ElasticDsl._
        //@formatter:off
        client.execute{ search( INDEX )
                          .query( idsQuery( docId ) )
                          .fetchSource( true )
                          .size( 1 )
                          .timeout( FiniteDuration( 4, SECONDS ) )
        }.transform {
                case Success( response : Response[ SearchResponse ] ) => Success( DART_JSON.unmarshalCdr( response.result.hits.hits.head.sourceAsString ).get )
                case Success( _ ) => Failure( new SearchIndexException( s"${docId} - GET failed, no response", GET_OPERATION ) )
                case Failure( e ) => Failure( new SearchIndexException( s"${docId} - GET failed, un expected response", GET_OPERATION, e ) )
            }
        //@formatter:on
    }

    // performs an ES request using the Java REST client that is not covered in the scope of the Elastic4s library
    private def lowLevelEsRequest( requestBody : String, method : String, url : String ) : Future[ HttpResponse ] = {
        if ( LOG.isTraceEnabled ) {
            LOG.trace( "using lower level Java Elasticsearch API..." )
            LOG.trace( s"${url}" )
            LOG.trace( s"${requestBody}" )
        }

        val request = ElasticRequest( method, url, HttpEntity( requestBody, "application/json" ) )

        val promise = Promise[ HttpResponse ]()
        val callback : Either[ Throwable, HttpResponse ] => Unit = {
            case Left( failure ) => promise.tryFailure( failure )
            case Right( response ) => {
                if ( response.statusCode == 200 ) promise.trySuccess( response )
                else promise.tryFailure( new Exception( response.entity.getOrElse( "" ).toString ) )
            }
        }

        client.client.send( request, callback )

        promise.future
    }

}
