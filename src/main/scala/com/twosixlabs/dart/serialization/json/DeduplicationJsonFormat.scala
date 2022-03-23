package com.twosixlabs.dart.serialization.json

import com.fasterxml.jackson.core.JsonProcessingException
import com.twosixlabs.dart.json.JsonFormat
import com.twosixlabs.dart.service.dedup.DeduplicationResponse
import org.slf4j.{Logger, LoggerFactory}

class DeduplicationJsonFormat extends JsonFormat {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    def unmarshalDuplicateInfo( json : String ) : Option[ DeduplicationResponse ] = {
        try Some( objectMapper.readValue( json, classOf[ DeduplicationResponse ] ) )
        catch {
            case e : JsonProcessingException => {
                LOG.error( s"duplicate info response is invalid..." )
                LOG.error( json )
                e.printStackTrace()
                None
            }
        }

    }

    def marshalDuplicateInfo( info : DeduplicationResponse ) : Option[ String ] = {
        try Some( objectMapper.writeValueAsString( info ) )
        catch {
            case e : JsonProcessingException => {
                LOG.debug( s"Error creating exchange JSON : ${e.getClass.getSimpleName} : ${e.getMessage} : ${e.getCause}" )
                e.printStackTrace()
                None
            }
        }
    }

}
