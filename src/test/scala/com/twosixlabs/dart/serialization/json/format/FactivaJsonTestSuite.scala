package com.twosixlabs.dart.serialization.json.format

import com.twosixlabs.cdr4s.annotations.FacetScore
import com.twosixlabs.cdr4s.core.{CdrAnnotation, FacetAnnotation}
import com.twosixlabs.dart.serialization.json.FactivaJsonFormat
import com.twosixlabs.dart.test.base.StandardTestBase3x

class FactivaDocumentJsonTestSuite extends StandardTestBase3x {

    private val factivaFormat : FactivaJsonFormat = new FactivaJsonFormat

    "Factiva Json Format" should "handle 'by Author Name" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v", "byline": "by Author Name"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.extractedMetadata.author shouldBe "Author Name"
    }

    "Factiva Json Format" should "handle 'By Author Name" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v", "byline": "By Author Name"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.extractedMetadata.author shouldBe "Author Name"
    }

    "Factiva Json Format" should "handle 'BY Author Name" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v", "byline": "BY Author Name"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.extractedMetadata.author shouldBe "Author Name"
    }

    "Factiva Json Format" should "handle 'by. Author Name" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v", "byline": "by. Author Name"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.extractedMetadata.author shouldBe "Author Name"
    }

    "Factiva Json Format" should "handle 'by: Author Name" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v", "byline": "by: Author Name"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.extractedMetadata.author shouldBe "Author Name"
    }

    "Factiva Json Format" should "handle 'Written by Author Name" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v", "byline": "Written by Author Name"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.extractedMetadata.author shouldBe "Author Name"
    }

    "Factiva Json Format" should "not mess up a confusing name like Abby Smith" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v", "byline": "Abby Smith"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.extractedMetadata.author shouldBe "Abby Smith"
    }

    "Factiva Json Format" should "not contain annotations if their codes are not present" in {
        val factivaJson : String =
            """{
              |  "copyright": "(c) Copyright 2018",
              |  "subject_codes": "",
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
              |  "source_name": "SKRIN Analytics",
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
              |  "publisher_name": "Lorem Ipsum",
              |  "action": "add",
              |  "byline": "By Gene Epstein",
              |  "document_type": "article"
              |}""".stripMargin

        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get

        cdr.annotations.length shouldBe 2

        cdr.annotations.contains( FacetAnnotation( label = "factiva-regions",
                                                   version = "1",
                                                   content = List( FacetScore( "United States", None ), FacetScore( "Balkan States", None ) ),
                                                   classification = CdrAnnotation.STATIC ) ) shouldBe true

        cdr.annotations.contains( FacetAnnotation( label = "factiva-industries",
                                                   version = "1",
                                                   content = List( FacetScore( "Pipeline Transportation", None ), FacetScore( "Sports Technologies", None ) ),
                                                   classification = CdrAnnotation.STATIC ) ) shouldBe true

    }

    "Factiva Json Format" should "not have any code annotations when are not specified" in {
        val factivaJson : String = """{"an": "B000000020120204e8260000v"}""""
        val cdr = factivaFormat.convertFactivaToCdr( "1", factivaJson ).get
        cdr.annotations.length shouldBe 0
    }

    "Factiva Json Format" should "validate a Facitva document that has the extended ticker_exchange fields" in {
        val factivaJson : String =
            """{
              |  "copyright": "(c) Copyright 2018",
              |  "subject_codes": "",
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
              |  "source_name": "SKRIN Analytics",
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
              |  "publisher_name": "Lorem Ipsum",
              |  "action": "add",
              |  "byline": "By Gene Epstein",
              |  "document_type": "article"
              |}""".stripMargin

        factivaFormat.validate( factivaJson.getBytes ) shouldBe true
    }

    "[DEFECT] [DART-594] - Factiva Json Format" should "properly handle null or empty code lists" in {
        val factivaJson =
            """{
              |  "snippet": "Lorem Ipsum",
              |  "company_codes_relevance": ",utdnat,unscou,rsatmn,opexpc,nusrah,",
              |  "copyright": "Copyright © 2017",
              |  "region_of_origin": "ASIA EEURZ EUR RUSS USSRZ ",
              |  "person_codes": ",114468258,114468258,1248484,1248484,62558193,62558193,99308501,99308501,",
              |  "dateline": "Wednesday, Oct  4 ",
              |  "publication_datetime": "1507218313000",
              |  "body": "Lorem Ipsum",
              |  "title": "Lorem Ipsum",
              |  "ingestion_datetime": "1507207527000",
              |  "company_codes_about": ",opexpc,",
              |  "industry_codes": "",
              |  "language_code": "en",
              |  "word_count": "8321",
              |  "company_codes_occur": ",utdnat,unscou,rsatmn,opexpc,nusrah,",
              |  "modification_date": "1541256695406",
              |  "publication_date": "1507218313000",
              |  "action": "add",
              |  "region_codes": null,
              |  "company_codes": ",nusrah,nusrah,opexpc,opexpc,rsatmn,rsatmn,unscou,unscou,utdnat,utdnat,opexpc,",
              |  "source_name": "Russian Government News",
              |  "document_type": "article",
              |  "publisher_name": "Lorem Ipsum",
              |  "modification_datetime": "1507207527000",
              |  "an": "RUSGOV0020171005eda500001",
              |  "source_code": "RUSGOV"
              |}""".stripMargin

        factivaFormat.validate( factivaJson.getBytes )
        factivaFormat.convertFactivaToCdr( "123", factivaJson ).isDefined shouldBe true
    }

}
