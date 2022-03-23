package com.twosixlabs.dart.pipeline.processor

import com.github.pemistahl.lingua.api.Language
import com.twosixlabs.cdr4s.core.CdrDocument
import com.twosixlabs.dart.datastore.DartDatastore
import com.twosixlabs.dart.pipeline.exception.{ServiceIntegrationException, UnrecognizedInputFormatException}
import com.twosixlabs.dart.pipeline.stage.Overrides
import com.twosixlabs.dart.service.dedup.{Deduplication, DeduplicationResponse, DuplicatedDoc}
import com.twosixlabs.dart.service.{LanguageDetector, LinguaLanguageDetector}
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.JsonTestUtils._
import com.twosixlabs.dart.test.utils.TestObjectMother.{CDR_TEMPLATE, VALID_FACTIVA_JSON, VALID_PULSE_CRAWL_SAMPLE, VALID_PULSE_TELEGRAM_SAMPLE, VALID_PULSE_TWITTER_SAMPLE}
import com.twosixlabs.dart.utils.AsyncDecorators.DecoratedFuture
import org.scalatest.BeforeAndAfterEach

import java.net.ConnectException
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class IngestionProcessorTestSuite extends StandardTestBase3x with BeforeAndAfterEach {

    private val deduplication : Deduplication = mock[ Deduplication ]
    private val datastore : DartDatastore = mock[ DartDatastore ]
    private val languageDetector : LanguageDetector = new LinguaLanguageDetector

    override def afterEach( ) : Unit = reset( deduplication, datastore )

    "Ingestion Processor" should "extract content from an incoming Pulse crawled document into DART format" in {
        val docId : String = "1"
        val pulseJson = toObjectNode( VALID_PULSE_CRAWL_SAMPLE )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val result : Try[ CdrDocument ] = processor.extractContent( docId, pulseJson )

        result.isSuccess shouldBe true
        result.get.documentId shouldBe docId
        result.get.extractedText shouldBe "crawl sample 1 body"
        result.get.captureSource shouldBe "pulse_crawl"
        result.get.extractedMetadata.title shouldBe "SAS troops fool Taliban by disguising as ‘devout’ women in burqas in dramatic Kabul escape"
    }

    "Ingestion Processor" should "extract content from an incoming Pulse telegram document into DART format" in {
        val docId : String = "1"
        val pulseJson = toObjectNode( VALID_PULSE_TELEGRAM_SAMPLE )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val result : Try[ CdrDocument ] = processor.extractContent( docId, pulseJson )

        result.isSuccess shouldBe true
        result.get.extractedText shouldBe "telegram sample 1 body"
        result.get.captureSource shouldBe "pulse_telegram_stream"
        result.get.extractedMetadata.title shouldBe ""
    }

    "Ingestion Processor" should "extract content from an incoming Pulse twitter document into DART format" in {
        val docId : String = "1"
        val pulseJson = toObjectNode( VALID_PULSE_TWITTER_SAMPLE )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val result : Try[ CdrDocument ] = processor.extractContent( docId, pulseJson )

        result.isSuccess shouldBe true
        result.get.documentId shouldBe docId
        result.get.extractedText shouldBe "twitter sample 1 body"
        result.get.captureSource shouldBe "pulse_tweet_traptor"
        result.get.extractedMetadata.title shouldBe ""
    }

    "Ingestion Processor" should "extract content from an incoming Factiva document into DART format" in {
        val docId : String = "1"
        val factivaJson = toObjectNode( VALID_FACTIVA_JSON )


        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val result : Try[ CdrDocument ] = processor.extractContent( docId, factivaJson )

        result.isSuccess shouldBe true
        result.get.documentId shouldBe docId
        result.get.extractedMetadata.author shouldBe "Gene Epstein"
    }

    "Ingestion Processor" should "convert an incoming Ladle document into a DART document" in {
        val docId = "1"
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = docId, extractedMetadata = CDR_TEMPLATE.extractedMetadata.copy( author = "michael reynolds" ) )
        val ladleJson = toObjectNode( doc.asLadleJson() )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val result : Try[ CdrDocument ] = processor.extractContent( docId, ladleJson )

        result.isSuccess shouldBe true
        result.get.documentId shouldBe doc.documentId
        result.get.extractedMetadata.author shouldBe doc.extractedMetadata.author
    }

    "Ingestion Processor" should "process a DART document" in {
        val docId = "1"
        val doc : CdrDocument = CDR_TEMPLATE.copy( documentId = docId, extractedMetadata = CDR_TEMPLATE.extractedMetadata.copy( author = "michael reynolds" ) )
        val dartJson = toObjectNode( doc.asDartJson() )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val result : Try[ CdrDocument ] = processor.extractContent( docId, dartJson )

        result.isSuccess shouldBe true
        result.get.documentId shouldBe doc.documentId
        result.get.extractedMetadata.author shouldBe doc.extractedMetadata.author
    }

    "Ingestion Processor" should "report an error for invalid content" in {
        val docId = "1"
        val badJson = toObjectNode( "{}" )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val result : Try[ CdrDocument ] = processor.extractContent( docId, badJson )

        result match {
            case Failure( _ : UnrecognizedInputFormatException ) => succeed
            case _ => fail( "the expected error was not thrown" )
        }
    }

    "Ingestion Processor" should "return an existing document along with it's tenant memberships" in {
        val testTenants : Set[ String ] = Set( "tenant-1" )
        when( datastore.getByDocId( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( Some( CDR_TEMPLATE ) ) )
        when( datastore.tenantMembershipsFor( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( testTenants ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.findExistingDoc( CDR_TEMPLATE.documentId ) synchronously() match {
            case Success( (Some( doc ), tenants) ) => {
                doc shouldBe CDR_TEMPLATE
                tenants shouldBe testTenants
            }
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "successfully return an empty document value (and empty tenants) if the doc doesn't exist" in {
        val testTenants : Set[ String ] = Set()
        when( datastore.getByDocId( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( None ) )
        when( datastore.tenantMembershipsFor( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( testTenants ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.findExistingDoc( CDR_TEMPLATE.documentId ) synchronously() match {
            case Success( (None, tenants) ) => tenants.isEmpty shouldBe true
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "return empty tenants if the document exists but is not a member of any tenants" in {
        val testTenants : Set[ String ] = Set()
        when( datastore.getByDocId( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( Some( CDR_TEMPLATE ) ) )
        when( datastore.tenantMembershipsFor( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( testTenants ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.findExistingDoc( CDR_TEMPLATE.documentId ) synchronously() match {
            case Success( (Some( doc ), tenants) ) => {
                doc shouldBe CDR_TEMPLATE
                tenants.isEmpty shouldBe true
            }
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "surface failures resulting from document lookup" in {
        val testTenants : Set[ String ] = Set()
        when( datastore.getByDocId( CDR_TEMPLATE.documentId ) ).thenReturn( Future.failed( new ConnectException() ) )
        when( datastore.tenantMembershipsFor( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( testTenants ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.findExistingDoc( CDR_TEMPLATE.documentId ) synchronously() match {
            case Failure( e : ConnectException ) => succeed
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "surface failures resulting from tenant lookup" in {
        when( datastore.getByDocId( CDR_TEMPLATE.documentId ) ).thenReturn( Future.successful( Some( CDR_TEMPLATE ) ) )
        when( datastore.tenantMembershipsFor( CDR_TEMPLATE.documentId ) ).thenReturn( Future.failed( new ConnectException() ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.findExistingDoc( CDR_TEMPLATE.documentId ) synchronously match {
            case Failure( e : ConnectException ) => succeed
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "successfully identify a new document" in {
        val dedupResult = DeduplicationResponse()
        when( deduplication.doDedup( CDR_TEMPLATE ) ).thenReturn( Success( dedupResult ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.checkDuplicates( CDR_TEMPLATE ) match {
            case Success( Nil ) => succeed
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "successfully identify duplicates" in {
        val duplicateDocs = List( DuplicatedDoc( "1", 1.0 ), DuplicatedDoc( "2", 1.0 ) )
        val dedupResult = DeduplicationResponse( duplicateDocs )
        when( deduplication.doDedup( CDR_TEMPLATE ) ).thenReturn( Success( dedupResult ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.checkDuplicates( CDR_TEMPLATE ) match {
            case Success( results : List[ String ] ) => results shouldBe duplicateDocs.map( _.docId )
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "report a failure during deduplication" in {
        val expectedException = new ServiceIntegrationException( "failed", "dedup" )
        when( deduplication.doDedup( CDR_TEMPLATE ) ).thenReturn( Failure( expectedException ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        processor.checkDuplicates( CDR_TEMPLATE ) match {
            case Failure( e : ServiceIntegrationException ) => e shouldBe expectedException
            case _ => fail( "unexpected results" )
        }
    }

    "Ingestion Processor" should "identify that there are tenant updates" in {
        val existing = Set( "tenant-1", "tenant-2" )
        val incoming = Set( "tenant-1", "tenant-3" )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        val newTenants = processor.checkTenantUpdates( existing, Overrides( tenants = incoming ) )
        newTenants shouldBe existing ++ incoming
    }

    "Ingestion Processor" should "should apply incoming updates to an existing document" in {
        val existingDoc = CDR_TEMPLATE.copy( labels = Set( "label-3" ) )
        val updates = Overrides( labels = Set( "label-1", "label-2" ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        val updatedDoc = processor.mergeOverrides( existingDoc, updates )

        updatedDoc.labels shouldBe Set( "label-1", "label-2", "label-3" )
    }

    "Ingestion Processor" should "apply sourceUri override to an existing document" in {
        val existingDoc = CDR_TEMPLATE.copy( labels = Set( "label-3" ), sourceUri = "test-source-uri" )
        val updates = Overrides( labels = Set( "label-1", "label-2" ), sourceUri = Some( "overridden-source-uri" ) )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        val updatedDoc = processor.mergeOverrides( existingDoc, updates )

        updatedDoc.labels shouldBe Set( "label-1", "label-2", "label-3" )
        updatedDoc.sourceUri shouldBe "overridden-source-uri"
    }

    "Ingestion Processor" should "apply stated genre override to an existing document" in {
        val existingDoc = CDR_TEMPLATE.copy()
        val genre = "news_article"
        val updates = Overrides( genre = genre )

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        val updatedDoc = processor.mergeOverrides( existingDoc, updates )

        updatedDoc.extractedMetadata.statedGenre shouldBe genre
    }

    "Ingestion Processor" should "apply default/empty overrides" in {
        val existingDoc = CDR_TEMPLATE.copy( labels = Set( "label-3" ), sourceUri = "test-source-uri" )
        val updates = Overrides()

        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )
        val updatedDoc = processor.mergeOverrides( existingDoc, updates )

        val expectedDoc = existingDoc.copy( extractedMetadata = existingDoc.extractedMetadata.copy( statedGenre = Overrides.DEFAULT_GENRE ) )

        updatedDoc shouldBe expectedDoc
    }

    "Ingestion Processor" should "detect English language documents" in {
        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val doc = CDR_TEMPLATE.copy( extractedText = "Hello world" )
        processor.checkLanguage( doc ) shouldBe Language.ENGLISH.getIsoCode639_3.toString
    }

    "Ingestion Processor" should "detect foreign language documents" in {
        val processor = new IngestionProcessor( datastore, deduplication, languageDetector )

        val latinCharDoc = CDR_TEMPLATE.copy( extractedText = "BELÍSSIMO VÍDEO E BELÍSSIMA PAISAGEM,JUNTANDO OS DOIS DA ESTE MARAVILHOSO VÍDEO FORTE ABRAÇOS E OBRIGADO AMIGO" )
        processor.checkLanguage( latinCharDoc ) shouldBe Language.PORTUGUESE.getIsoCode639_3.toString

        val russianLanguageDoc = CDR_TEMPLATE.copy( extractedText = "Ai, жители все равно не прочитают. Во многих деревнях просто нет связи, не то что интернета" )
        processor.checkLanguage( russianLanguageDoc ) shouldBe Language.RUSSIAN.getIsoCode639_3.toString

        val arabicLanguageDoc = CDR_TEMPLATE.copy( extractedText = "مشروع عواصم المحافظات يستهدف إنشاء 500 ألف وحدة سكنية\\nقال الدكتور مصطفى مدبولي، رئيس مجلس الوزراء، إن مشروع عواصم المدن والمحافظات يستهدف إنشاء نحو 500 ألف وحدة سكنية.\\nوأضاف « مدبولي» خلال كلمته اليوم الأربعاء، بملتقى بناة مصر بمشاركة وفود عربية وإفريقية: «سعداء بعودة إصدار تقرير التنمية البشرية بمصر بعد توقف طويل».\\nوأوضح رئيس الوزراء، أن هذا العام سيتم الانتهاء من تغطية جيمع المدن بالصرف الصحي، مؤكدا أن الدولة بذلت جهدا هائلا بالقطاع على مدار 3 سنوات ومازال تطوير مستمر" )
        processor.checkLanguage( arabicLanguageDoc ) shouldBe Language.ARABIC.getIsoCode639_3.toString
    }
}
