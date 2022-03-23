package com.twosixlabs.dart.pipeline.stage

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.twosixlabs.cdr4s.core.CdrDocument
import com.twosixlabs.cdr4s.json.dart.{ThinCdrAnnotationDto, ThinCdrDocumentDto, ThinCdrMetadataDto}
import com.twosixlabs.dart.AppContext
import com.twosixlabs.dart.datastore.DartDatastore
import com.twosixlabs.dart.exceptions.Exceptions
import com.twosixlabs.dart.json.JsonFormat
import com.twosixlabs.dart.notifications.{DlqWrapper, DuplicateNotification, Notifications}
import com.twosixlabs.dart.ontologies.OntologyRegistryService
import com.twosixlabs.dart.ontologies.api.{DocumentUpdateNotification, TenantOntologyMapping}
import com.twosixlabs.dart.pipeline.command.{DocumentProcessing, NotifyDuplicate, NotifyError, NotifyUpdate}
import com.twosixlabs.dart.utils.AsyncDecorators._
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


object NotificationPublisher {

    def apply( appContext : AppContext, config : Config ) : Behavior[ DocumentProcessing ] = {
        val producer : KafkaProducer[ String, String ] = appContext.kafkaProvider.newProducer( config )
        NotificationPublisher( producer, appContext.cdrDatastore, appContext.ontologyRegistry )
    }

    def apply( producer : KafkaProducer[ String, String ], datastore : DartDatastore, ontologyRegistry : OntologyRegistryService ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup[ DocumentProcessing ]( context => {
            new NotificationPublisher( context, producer, datastore, ontologyRegistry )
        } )
    }
}

class NotificationPublisher( context : ActorContext[ DocumentProcessing ],
                             producer : KafkaProducer[ String, String ],
                             datastore : DartDatastore,
                             ontologyRegistry : OntologyRegistryService ) extends AbstractBehavior[ DocumentProcessing ]( context ) {

    private implicit val executionContext : ExecutionContext = scala.concurrent.ExecutionContext.global

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case NotifyUpdate( doc, _ ) => {
                val ontologies : Set[ TenantOntologyMapping ] = getOntologyMappings( doc.documentId )
                val docUpdate = thinCdrDtoShim( doc )
                val notification = updateNotificationJson( DocumentUpdateNotification( Some( docUpdate ), ontologies ) )
                send( new ProducerRecord[ String, String ]( Notifications.UPDATE_TOPIC, doc.documentId, notification ) )
                Behaviors.same
            }
            case NotifyDuplicate( docId, duplicates, _ ) => {
                val body = DuplicateNotification( docId, duplicates ).toJson()
                send( new ProducerRecord[ String, String ]( Notifications.DUPLICATE_TOPIC, docId, body ) )
                Behaviors.same
            }
            case NotifyError( documentId, body, operation, cause, _ ) => {
                val dlqMsg = DlqWrapper( documentId, operation, cause.getMessage, Exceptions.getStackTraceText( cause ), System.currentTimeMillis() )
                send( new ProducerRecord[ String, String ]( Notifications.DLQ_TOPIC, documentId, dlqMsg.toString ) )
                Behaviors.same
            }
            case _ => Behaviors.unhandled
        }
    }

    private def send( record : ProducerRecord[ String, String ] ) : Unit = {
        //@formatter:off
            Future {
                producer.send( record ).get()
            }.onComplete {
                case Failure( e ) => {
                    LOG.error( s"failed to send message for key = ${record.key()}, topic = ${record.topic()}" )
                    if( LOG.isDebugEnabled() ) {
                        LOG.debug( s"value = ${record.value()}" )
                        e.printStackTrace()
                    }
                }
                case Success( _ ) => {}
            }
        //@formatter:on
    }

    private def getOntologyMappings( docId : String ) : Set[ TenantOntologyMapping ] = {
        datastore.tenantMembershipsFor( docId ).synchronously( 5000 ) match {
            case Success( tenants ) => {
                val ontologies = tenants.flatMap( t => {
                    ontologyRegistry.latest( t ) match {
                        case Success( value ) => value
                        case Failure( e ) => {
                            LOG.error( s"there was an error sending notification for ${docId}, notifications may not be working..." )
                            None
                        }
                    }
                } )
                ontologies.map( o => TenantOntologyMapping( o.tenant, o.id ) )
            }
            case Failure( e ) => {
                LOG.error( s"unable to retrieve ontology mappings for ${docId}, notifications may not be working -  ${e.getClass} : ${e.getMessage} : ${e.getCause}" )
                e.printStackTrace()
                Set()
            }
        }
    }

    private def thinCdrDtoShim( doc : CdrDocument ) : ThinCdrDocumentDto = {
        val thin = doc.makeThin
        val dtoMeta : ThinCdrMetadataDto = ThinCdrMetadataDto( creationDate = thin.extractedMetadata.creationDate,
                                                               modificationDate = thin.extractedMetadata.modificationDate,
                                                               author = thin.extractedMetadata.author,
                                                               docType = thin.extractedMetadata.docType,
                                                               description = thin.extractedMetadata.description,
                                                               originalLanguage = thin.extractedMetadata.language,
                                                               classification = thin.extractedMetadata.classification,
                                                               title = thin.extractedMetadata.title,
                                                               publisher = thin.extractedMetadata.publisher,
                                                               url = thin.extractedMetadata.url,
                                                               pages = thin.extractedMetadata.pages,
                                                               subject = thin.extractedMetadata.subject,
                                                               creator = thin.extractedMetadata.creator,
                                                               producer = thin.extractedMetadata.producer,
                                                               statedGenre = thin.extractedMetadata.statedGenre,
                                                               predictedGenre = thin.extractedMetadata.predictedGenre )

        val dtoAnnotations : List[ ThinCdrAnnotationDto ] = {
            thin.annotations.map( a => {
                ThinCdrAnnotationDto( Some( a.annotation ), a.label )
            } )
        }

        ThinCdrDocumentDto( captureSource = thin.captureSource,
                            extractedMetadata = dtoMeta,
                            contentType = thin.contentType,
                            extractedNumeric = thin.extractedNumeric,
                            documentId = thin.documentId,
                            extractedText = thin.extractedText,
                            uri = thin.uri,
                            sourceUri = thin.sourceUri,
                            extractedNtriples = thin.extractedNtriples,
                            timestamp = thin.timestamp,
                            annotations = dtoAnnotations,
                            labels = thin.labels )

    }

    private def updateNotificationJson( notification : DocumentUpdateNotification ) : String = {
        JsonFormat.marshalFrom( notification ).get
    }
}
