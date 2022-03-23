package com.twosixlabs.dart.service.dedup

import better.files.File
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.{CDR_TEMPLATE, TEST_CONFIG}
import com.typesafe.config.ConfigValueFactory

import scala.util.{Failure, Success}

class EmbeddedDeduplicationTestSuite extends StandardTestBase3x {

    System.setProperty( "chronicle.announcer.disable", "true" )

    "Embedded Deduplication" should "initialize a valid configuration" in {
        clean()
        Deduplication.fromConfig( TEST_CONFIG.getConfig( "dedup" ) )
    }

    "Embedded Deduplication" should "raise an error for invalid configuration" in {
        clean()
        val invalidConfig = TEST_CONFIG.withValue( "dedup.embedded.algorithm", ConfigValueFactory.fromAnyRef( "asdf" ) )
        intercept[ IllegalArgumentException ] {
            Deduplication.fromConfig( invalidConfig.getConfig( "dedup" ) )
        }
    }

    "Embedded Deduplication" should "update and find duplicate documents from the cache" in {
        clean()
        val deduplication : Deduplication = Deduplication.fromConfig( TEST_CONFIG.getConfig( "dedup" ) )
        val doc = CDR_TEMPLATE.copy( documentId = "1", extractedText = "hello this is some example extracted text to use in a test" )

        // this should insert the document
        deduplication.doDedup( doc ) match {
            case Success( value ) => value.duplicateDocs.isEmpty shouldBe true
            case Failure( e ) => {
                e.printStackTrace()
            }
        }

        // this should find the original and update the cache
        deduplication.doDedup( doc.copy( documentId = "2" ) ) match {
            case Success( value ) => {
                value.duplicateDocs should contain( DuplicatedDoc( "1", 1.0 ) )
            }
            case Failure( exception ) => fail( exception )
        }

        // this should find the two previous docs and update the cache
        deduplication.doDedup( doc.copy( documentId = "3" ) ) match {
            case Success( value ) => {
                value.duplicateDocs should contain( DuplicatedDoc( "1", 1.0 ) )
                value.duplicateDocs should contain( DuplicatedDoc( "2", 1.0 ) )
            }
            case Failure( exception ) => fail( exception )
        }

    }

    private def clean( ) : Unit = {
        val dir = File( TEST_CONFIG.getString( "dedup.embedded.shingleprint.cache.dir" ) )
        dir.delete( swallowIOExceptions = true )
        dir.createDirectory()
    }

}
