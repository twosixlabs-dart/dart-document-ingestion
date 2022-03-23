package com.twosixlabs.dart.service.annotator

import com.twosixlabs.cdr4s.annotations.OffsetTag
import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument, DictionaryAnnotation, DocumentGenealogyAnnotation, FacetAnnotation, OffsetTagAnnotation, TextAnnotation}
import com.twosixlabs.dart.pipeline.exception.ServiceIntegrationException
import com.twosixlabs.dart.serialization.json.JsonDataFormats.DART_JSON
import com.twosixlabs.dart.service.RestIntegration
import okhttp3.{OkHttpClient, Request, RequestBody, Response}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object Annotator {
    final val ANNOTATE_OP : String = "Annotator.annotate"
}

trait Annotator {
    def label : String

    def annotate( doc : CdrDocument ) : Try[ Option[ CdrAnnotation[ _ ] ] ]

}

class RestAnnotator( config : AnnotatorDef, client : OkHttpClient ) extends Annotator with RestIntegration {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    private val url = s"http://${config.host}:${config.port}/${config.path}"

    override val systemName : String = config.label

    override def label : String = config.label

    override def annotate( doc : CdrDocument ) : Try[ Option[ CdrAnnotation[ _ ] ] ] = {
        LOG.debug( s"executing ${label} annotation request for ${doc.documentId}" )

        val cdrJson : String = DART_JSON.marshalCdr( doc ).get
        try {
            val body = RequestBody.create( cdrJson, JSON_MEDIA_TYPE )
            val request : Request = new Request.Builder().url( url ).post( body ).build()
            val response : Response = client.newCall( request ).execute()
            response.code() match {
                case 200 => {
                    val responseStr = response.body.string()
                    val annotation : Option[ CdrAnnotation[ _ ] ] = DART_JSON.unmarshalAnnotation( responseStr ) // be careful, response.body.string modifies the resp
                    annotation match {
                        case Some( value ) => {
                            if ( !contentIsNullOrEmpty( value ) ) {
                                value match {
                                    case tag : OffsetTagAnnotation => {
                                        if ( config.postProcess ) {
                                            response.close()
                                            return Success( Some( postProcessTagAnnotation( doc, tag ) ) )
                                        }
                                        else return {
                                            response.close()
                                            Success( Some( tag ) )
                                        }
                                    }
                                    case annotation : CdrAnnotation[ _ ] => {
                                        response.close()
                                        return Success( Some( annotation ) )
                                    }
                                }
                            }
                            else {
                                response.close()
                                return Success( None )
                            }
                        }
                        case None => {
                            response.close()
                            return Success( None )
                        }
                    }
                }
                case 404 => {
                    response.close()
                    return Success( None )
                }
                case _ => {
                    val errorBody = response.body.string()
                    LOG.error( s" ${config.label} : ${response.code} :: ${errorBody}" )
                    response.close()
                    Failure( new ServiceIntegrationException( s"unexpected response code from annotator: ${response.code()} : ${request.url().toString}", systemName ) )
                }
            }
        } catch {
            case e : Throwable => {
                LOG.error( s"unexpected failure invoking annotator service ${config.label} - ${e.getClass} : ${e.getMessage} : ${e.getCause}" )
                Failure( new ServiceIntegrationException( s"unexpected exception from invoking annotator: ${config.label}", systemName, e ) )
            }
        }
    }

    private def postProcessTagAnnotation( document : CdrDocument, annotation : OffsetTagAnnotation ) : CdrAnnotation[ _ ] = {
        val updatedTags = annotation.content.map( ( tag : OffsetTag ) => {
            val textValue = document.extractedText.substring( tag.offsetStart, tag.offsetEnd )
            tag.copy( value = Some( textValue ) )
        } )
        annotation.copy( content = updatedTags )
    }

    private def contentIsNullOrEmpty( annotation : CdrAnnotation[ _ ] ) : Boolean = {
        if ( annotation.content == null ) return true
        else {
            annotation match {
                case tags : OffsetTagAnnotation => return tags.content == null || tags.content.isEmpty
                case dict : DictionaryAnnotation => return dict.content == null || dict.content.isEmpty
                case text : TextAnnotation => return text.content == null || text.content.isEmpty
                case facet : FacetAnnotation => return facet.content == null || facet.content.isEmpty
                case gene : DocumentGenealogyAnnotation => return gene.content == null
            }
        }
    }

}
