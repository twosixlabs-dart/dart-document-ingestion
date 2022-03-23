package com.twosixlabs.dart.notifications

import com.fasterxml.jackson.annotation.JsonProperty
import com.twosixlabs.dart.json.JsonFormat

import scala.beans.BeanProperty

object Notifications {

    val DUPLICATE_TOPIC : String = "dart.duplicate"
    val INGEST_PROXY_TOPIC : String = "dart.ingest.proxy"
    val UPDATE_TOPIC : String = "dart.cdr.streaming.updates"
    val DLQ_TOPIC : String = "dart.generic.dlq"

}

case class DuplicateNotification( @BeanProperty @JsonProperty( "doc_id" ) docId : String,
                                  @BeanProperty @JsonProperty( "duplicated_docs" ) duplicatedDocs : List[ String ] ) {
    def toJson( ) : String = {
        JsonFormat.marshalFrom( this ).get
    }
}

// TODO implement json marshalling
case class DlqWrapper(
  @BeanProperty @JsonProperty( "doc_id" ) docId : String,
  @BeanProperty @JsonProperty( "operation" ) operation : String,
  @BeanProperty @JsonProperty( "error_msg" ) errorMsg : String,
  @BeanProperty @JsonProperty( "stacktrace" ) stackTrace : String,
  @BeanProperty @JsonProperty( "timestamp" ) timestamp : Long
)
