package com.twosixlabs.dart.service.annotator

import com.twosixlabs.dart.test.base.StandardTestBase3x
import com.twosixlabs.dart.test.utils.TestObjectMother.TEST_CONFIG
import com.typesafe.config.Config

class AnnotatorDefTestSuite extends StandardTestBase3x {

    "annotator def parsing" should "successfully parse annotator configuration" in {
        val annotatorConfig : Config = TEST_CONFIG.getConfig( "annotators" )

        val expected : Set[ AnnotatorDef ] = Set( AnnotatorDef( "qntfy-ner", "localhost", 45000, true, "api/v1/annotate/cdr", 5, 750 ),
                                                  AnnotatorDef( "qntfy-categories", "localhost", 45001, false, "api/v1/annotate/cdr", 5, 750 ),
                                                  AnnotatorDef( "custom", "my-custom-host", 45002, false, "api/v1/annotate/cdr", 5, 750 ) )

        AnnotatorDef.parseConfig( annotatorConfig ) shouldBe expected
    }

}
