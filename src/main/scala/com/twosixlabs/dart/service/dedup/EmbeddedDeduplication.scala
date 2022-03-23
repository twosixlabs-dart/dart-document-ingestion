package com.twosixlabs.dart.service.dedup

import better.files.File
import com.github.reynoldsm88.dedup.Dedup
import com.github.reynoldsm88.dedup.shingleprint.{OnDiskShingleprintCache, ShingleprintDedup}
import com.twosixlabs.cdr4s.core.CdrDocument
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object EmbeddedDeduplication {

    /**
      * Accepts a config structure from the root `dedup.embedded`
      *
      * @param conf
      * @return
      */
    def mkFromConfig( conf : Config, maxWords : Int ) : Deduplication = {
        conf.getString( "algorithm" ).toLowerCase match {
            case "shingleprint" => {
                val window = conf.getInt( "shingleprint.window" )
                val cache = OnDiskShingleprintCache( File( conf.getString( "shingleprint.cache.dir" ) ) )
                val dedup = new ShingleprintDedup( maxWords, 0.9, window, cache )
                new EmbeddedDeduplication( dedup, maxWords )
            }
            case "lsh" => throw new UnsupportedOperationException( "an LSH algorithm for embedded dedup has not been implemented yet" )
            case o => throw new IllegalArgumentException( s"${o} is not a supported algorithm for embedded deduplication" )
        }
    }

}

class EmbeddedDeduplication( dedup : Dedup, override val maxWords : Int ) extends Deduplication {

    private implicit val ec : ExecutionContext = ExecutionContext.global

    override def doDedup( cdr : CdrDocument ) : Try[ DeduplicationResponse ] = {
        try {
            val duplicates = dedup.check( cdr.extractedText )
            dedup.update( cdr.documentId, cdr.extractedText )
            val response = {
                if ( duplicates.isEmpty ) DeduplicationResponse()
                else DeduplicationResponse( duplicates.map( d => DuplicatedDoc( d.docId, 1.0 ) ).toList )
            }
            Success( response )
        } catch {
            case e : Exception => Failure( e )
        }
    }

}
