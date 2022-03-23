package com.twosixlabs.dart

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.twosixlabs.dart.service.{ElasticsearchIndex, SearchIndex}

class SearchIndexProvider( host : String, port : Int ) {

    def newClient( ) : SearchIndex = {
        val client = ElasticClient( JavaClient( ElasticProperties( s"http://${host}:${port}" ) ) )
        new ElasticsearchIndex( client )
    }

}
