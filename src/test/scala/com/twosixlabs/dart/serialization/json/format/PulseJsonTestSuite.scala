package com.twosixlabs.dart.serialization.json.format

import com.twosixlabs.dart.serialization.json.PulseJsonFormat
import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.{VALID_PULSE_CRAWL_SAMPLE, VALID_PULSE_TELEGRAM_SAMPLE, VALID_PULSE_TWITTER_SAMPLE}

class PulseJsonTestSuite extends StandardTestBase3x {

    private val pulseFormat : PulseJsonFormat = new PulseJsonFormat

    "Pulse Json Format" should "fail validation if the document does not contain `norm.body`" in {
        val failedJson = """{"uid":"c25ab1e854d8b1b5339d713de8c4fe6b882638ec80829dd454f1f1f8e1dcebdb","project_id":"default","meta":{"post_type":[{"results":["post"],"attribs":{"website":"github.com/istresearch","source":"Explicit","type":"Post Type Extractor","version":"1.0"}}],"image_archiver":[{"results":[{"path":"https://s3.amazonaws.com/pipeline-image-store/images/default/default/3/f/4/9/3f49f77ecc4cc0f4e248db566060ff88325cdaf1ed5e630c8cc4331f2094ea4c.webp","url":"https://telegrabber-attachments-iprod.s3.amazonaws.com/12026797175/6303378683184899424-2_975282889899901660.webp","dhash_hex":"0000000000000000","image_hash":"3f49f77ecc4cc0f4e248db566060ff88325cdaf1ed5e630c8cc4331f2094ea4c","dhash_bits":"0000000000000000000000000000000000000000000000000000000000000000"}],"attribs":{"website":"github.com/istresearch","type":"Image Archiver","version":"1.1"}}],"author_name":[{"results":["AilAilAilAilAA"],"attribs":{"website":"github.com/istresearch","source":"Explicit","type":"String Extractor","version":"1.0"}}],"rule_matcher":[{"results":[{"rule_id":"1462615","rule_type":"telegram_link","appid":"double-helix","project_version_id":"7429B4DA-D6D2-8650-726C-9074455CC247","project_id":"C597311D-E217-C2BB-1683-14D0321D068C","organization_id":"BE8FD33E-0159-9B86-6842-8B8321FA29B5","metadata":{"campaign_title":"Campaign monitoring Iraq","project_title":"SMG Inspirational"},"sub_organization_id":"C1090179-8BA1-9960-1E66-BDB7C97DFD50","value":"t.me/AilAilAilAilAA","campaign_id":"440E661E-EA3C-AFA7-E1CC-89055B0BB504","rule_tag":"TEL-SMG-INSPIRATION-AR"},{"rule_id":"1548817","rule_type":"telegram_link","appid":"double-helix","project_version_id":"04D55F2D-77A2-42F4-A21E-3344CA5327ED","project_id":"A38DD270-4603-A72F-5EA5-807007AD0959","organization_id":"BE8FD33E-0159-9B86-6842-8B8321FA29B5","metadata":{"campaign_title":"Iraq Monitoring Campaign","project_title":"IST Suggested SMG & IRGC"},"sub_organization_id":"C1090179-8BA1-9960-1E66-BDB7C97DFD50","value":"t.me/AilAilAilAilAA","campaign_id":"440E661E-EA3C-AFA7-E1CC-89055B0BB504","rule_tag":"SMG"}],"attribs":{"website":"github.com/istresearch","source":"Explicit","type":"telegram_stream","version":"1.0"}}],"author_id":[{"results":["0"],"attribs":{"website":"github.com/istresearch","source":"Explicit","type":"String Extractor","version":"1.0"}}],"images":[{"results":["https://telegrabber-attachments-iprod.s3.amazonaws.com/12026797175/6303378683184899424-2_975282889899901660.webp"],"attribs":{"website":"github.com/istresearch","source":"Explicit","type":"Image Link Extractor","version":"1.1"}}]},"organization_id":"BE8FD33E-0159-9B86-6842-8B8321FA29B5","sub_organization_id":"C1090179-8BA1-9960-1E66-BDB7C97DFD50","doc":{"group_id":"1467619716","event":"message","attachment_url":"https://telegrabber-attachments-iprod.s3.amazonaws.com/12026797175/6303378683184899424-2_975282889899901660.webp","peer":{"name":"AilAilAilAilAA","peer_id":0},"id":"385e843b-d7bf-5f9c-ab87-f736a4f99c00","date":1.631714842E9,"sender":{"name":"AilAilAilAilAA","peer_id":0},"group":"AilAilAilAilAA"},"system_timestamp":"2021-09-15T14:17:20.161482+00:00","norm_attribs":{"website":"github.com/istresearch","type":"telegram_stream","version":"1.0"},"project_version_id":"default","type":"telegram_stream_message","campaign_id":"440E661E-EA3C-AFA7-E1CC-89055B0BB504","norm":{"author":"AilAilAilAilAA","domain":"telegram.org","id":"385e843b-d7bf-5f9c-ab87-f736a4f99c00","timestamp":"2021-09-15T14:07:22+00:00"}}"""
        pulseFormat.validate( failedJson.getBytes() ) shouldBe false
    }


    "Pulse Json Format" should "should convert pulse crawled data " in {
        val cdr = pulseFormat.convertPulseToCdr( "1", VALID_PULSE_CRAWL_SAMPLE ).get
        cdr.extractedText shouldBe "crawl sample 1 body"
        cdr.captureSource shouldBe "pulse_crawl"
        cdr.extractedMetadata.title shouldBe "SAS troops fool Taliban by disguising as ‘devout’ women in burqas in dramatic Kabul escape"
        cdr.sourceUri shouldBe "https://www.express.co.uk/news/world/1486491/sas-troops-afghanistan-taliban-escape-burqa-british-special-forces-kabul-ont"
    }

    "Pulse Json Format" should "should convert pulse telegram data " in {
        val cdr = pulseFormat.convertPulseToCdr( "1", VALID_PULSE_TELEGRAM_SAMPLE ).get
        cdr.extractedText shouldBe "telegram sample 1 body"
        cdr.captureSource shouldBe "pulse_telegram_stream"
        cdr.extractedMetadata.title shouldBe ""
        cdr.sourceUri shouldBe ""

    }

    "Pulse Json Format" should "should convert pulse twitter data " in {
        val cdr = pulseFormat.convertPulseToCdr( "1", VALID_PULSE_TWITTER_SAMPLE ).get
        cdr.extractedText shouldBe "twitter sample 1 body"
        cdr.captureSource shouldBe "pulse_tweet_traptor"
        cdr.extractedMetadata.title shouldBe ""
        cdr.sourceUri shouldBe "https://twitter.com/NicoH715/status/1434886844093771779"

    }
}
