package com.twosixlabs.dart.service.dedup

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.twosixlabs.cdr4s.core.CdrDocument
import com.typesafe.config.Config

import scala.beans.BeanProperty
import scala.util.{Success, Try}

@JsonInclude( Include.NON_EMPTY )
case class DuplicatedDoc( @BeanProperty @JsonProperty( "doc_id" ) docId : String,
                          @BeanProperty @JsonProperty( "score" ) score : BigDecimal )

@JsonInclude( Include.NON_EMPTY )
case class DeduplicationResponse( @BeanProperty @JsonProperty( "duplicated_docs" ) duplicateDocs : List[ DuplicatedDoc ] = List() )

trait Deduplication {
    val maxWords : Int

    def doDedup( cdr : CdrDocument ) : Try[ DeduplicationResponse ]

}

/**
  * This class is provided mostly for testing purposes, but can be used if duplication is not
  * important to the pipeline
  */
class NoOpDeduplication extends Deduplication {
    override val maxWords = 0

    override def doDedup( cdr : CdrDocument ) : Try[ DeduplicationResponse ] = Success( DeduplicationResponse() )
}

/**
  * Factory methods
  */
object Deduplication {

    /**
      * Accepts config structure from the root `dedup`
      *
      * @param conf
      * @return
      */
    def fromConfig( conf : Config ) : Deduplication = {
        val maxWords : Int = conf.getInt( "max.words" )
        conf.getString( "mode" ).toLowerCase match {
            case "embedded" => EmbeddedDeduplication.mkFromConfig( conf.getConfig( "embedded" ), maxWords )
            case "rest" => throw new UnsupportedOperationException( "REST deduplication is no longer supported..." )
            case "noop" => new NoOpDeduplication
            case o => throw new IllegalArgumentException( s"${o} is not a supported deduplication mode" )
        }
    }

}
