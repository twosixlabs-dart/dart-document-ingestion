package com.twosixlabs.dart.test.utils

import better.files.File
import com.twosixlabs.cdr4s.core.{CdrDocument, CdrMetadata}
import com.twosixlabs.dart.serialization.json.{IngestProxyEvent, IngestProxyEventMetadata}
import com.twosixlabs.dart.utils.DatesAndTimes
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.Source


object TestObjectMother {
    val TEST_CONFIG : Config = ConfigFactory.parseFile( File( "src/test/resources/env/test.conf" ).toJava ).resolve()

    val EVENT_TEMPLATE : IngestProxyEvent = IngestProxyEvent( document = null, metadata = IngestProxyEventMetadata( labels = Set(), tenants = Set() ) )

    val VALID_FACTIVA_JSON : String =
        """{
          |  "copyright": "(c) Copyright 2018. ",
          |  "subject_codes": ",nanl,ncat,",
          |  "art": "",
          |  "modification_datetime": "1536847807000",
          |  "body": "Lorem Ipsum",
          |  "company_codes_occur": "",
          |  "company_codes_about": ",rosgos,",
          |  "company_codes_lineage": "",
          |  "snippet": "Lorem Ipsum",
          |  "publication_date": "1536796800000",
          |  "market_index_codes": "",
          |  "credit": "",
          |  "section": "",
          |  "currency_codes": "",
          |  "region_of_origin": "ASIA EEURZ EUR RUSS USSRZ ",
          |  "ingestion_datetime": "1536847817000",
          |  "modification_date": "1541255000190",
          |  "source_name": "source",
          |  "language_code": "en",
          |  "region_codes": ",usa,balkz,",
          |  "company_codes_association": "",
          |  "person_codes": ",110451339,110451339,",
          |  "company_codes_relevance": ",eurcb,bnkeng,",
          |  "source_code": "SKRANE",
          |  "an": "SKRANE0020180913ee9d0002t",
          |  "word_count": "881",
          |  "company_codes": ",bnkeng,eurcb,rosgos,",
          |  "industry_codes": ",i1300006,isptech,",
          |  "title": "Lorem Ipsum",
          |  "publication_datetime": "1536796800000",
          |  "publisher_name": "publisher",
          |  "action": "add",
          |  "byline": "By Gene Epstein",
          |  "document_type": "article"
          |}""".stripMargin


    val CDR_TEMPLATE : CdrDocument = CdrDocument( captureSource = "ManualCuration",
                                                  extractedMetadata = CdrMetadata( creationDate = DatesAndTimes.timeStamp.toLocalDate,
                                                                                   modificationDate = DatesAndTimes.timeStamp.toLocalDate,
                                                                                   author = "michael",
                                                                                   docType = "pdf",
                                                                                   description = "Lorum Ipsum",
                                                                                   originalLanguage = "en",
                                                                                   classification = "UNCLASSIFIED",
                                                                                   title = "Lorum Ipsum",
                                                                                   publisher = "Lorum Ipsum",
                                                                                   url = "https://www.lorumipsum.com" ),
                                                  contentType = "text/html",
                                                  extractedNumeric = Map.empty,
                                                  documentId = "123abc",
                                                  extractedText = "Lorum Ipsum",
                                                  uri = "https://lorumipsum.com",
                                                  sourceUri = "Lorum Ipsum",
                                                  extractedNtriples = "",
                                                  timestamp = DatesAndTimes.timeStamp,
                                                  annotations = List() )

    val VALID_PULSE_CRAWL_SAMPLE = Source.fromURL( getClass.getResource( "/pulse/crawl-sample-1.json" ) ).mkString
    val VALID_PULSE_TELEGRAM_SAMPLE = Source.fromURL( getClass.getResource( "/pulse/telegram-sample-1.json" ) ).mkString
    val VALID_PULSE_TWITTER_SAMPLE = Source.fromURL( getClass.getResource( "/pulse/twitter-sample-1.json" ) ).mkString

}
