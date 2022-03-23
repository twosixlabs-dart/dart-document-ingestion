package com.twosixlabs.dart.test.utils

import com.fasterxml.jackson.databind.node.ObjectNode
import com.twosixlabs.cdr4s.core.CdrDocument
import com.twosixlabs.dart.json.JsonFormat
import com.twosixlabs.dart.serialization.json.JsonDataFormats


class TestCdrDocumentDecorator( cdr : CdrDocument ) {
    def asLadleJson( ) : String = JsonDataFormats.LADLE_JSON.marshalCdr( cdr ).get

    def asDartJson( ) : String = JsonDataFormats.DART_JSON.marshalCdr( cdr ).get
}

object JsonTestUtils extends JsonFormat {

    def marshalTo[ T ]( json : String, clazz : Class[ T ] ) : T = {
        objectMapper.readValue( json, clazz )
    }

    def toObjectNode( json : String ) : ObjectNode = marshalTo[ ObjectNode ]( json, classOf[ ObjectNode ] )

    implicit def decorateCdr( cdr : CdrDocument ) : TestCdrDocumentDecorator = new TestCdrDocumentDecorator( cdr )

}
