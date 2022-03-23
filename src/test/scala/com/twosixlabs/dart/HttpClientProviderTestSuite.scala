package com.twosixlabs.dart

import better.files.File
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.typesafe.config.{Config, ConfigFactory}
import okhttp3.OkHttpClient

class HttpClientProviderTestSuite extends StandardTestBase3x {

    private val TEST_CONFIG : Config = ConfigFactory.parseFile( File( "src/test/resources/env/test.conf" ).toJava ).resolve()

    "http client provider" should "use proper defaults when no config is specified" in {
        val conf = TEST_CONFIG.getConfig( "http-client-default" )

        val client : OkHttpClient = HttpClientFactory.newClient( conf )

        client.connectTimeoutMillis shouldBe HttpClientFactory.DEFAULT_TIMEOUT
        client.callTimeoutMillis shouldBe HttpClientFactory.DEFAULT_TIMEOUT
        client.readTimeoutMillis shouldBe HttpClientFactory.DEFAULT_TIMEOUT
        client.writeTimeoutMillis shouldBe HttpClientFactory.DEFAULT_TIMEOUT
    }

    "http client provider" should "use specified config" in {
        val conf = TEST_CONFIG.getConfig( "http-client-config" )

        val client : OkHttpClient = HttpClientFactory.newClient( conf )

        client.connectTimeoutMillis shouldBe 10000
        client.callTimeoutMillis shouldBe 10000
        client.readTimeoutMillis shouldBe 10000
        client.writeTimeoutMillis shouldBe 10000
    }

}
