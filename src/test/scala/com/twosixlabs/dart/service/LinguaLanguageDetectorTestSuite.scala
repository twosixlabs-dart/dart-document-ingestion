package com.twosixlabs.dart.service

import com.twosixlabs.dart.test.base.StandardTestBase3x

class LinguaLanguageDetectorTestSuite extends StandardTestBase3x {

    private val detector : LanguageDetector = new LinguaLanguageDetector

    private val russianShort : String = "Бумеранг . земля стекловатой"
    private val russianLong : String = "Ai, жители все равно не прочитают. Во многих деревнях просто нет связи, не то что интернета("

    private val spanishShort : String = "Buen programa completo, claro"
    private val spanishLong : String = "¡¡Que belleza!! Gracias Luis por mostrar las bellezas de nuestro país!!! Nosotros hacemos viajes a este mágico y relampaguante lugar, el hermoso Catatumbo, a la orden para quienes quieran ir!!!!"
    private val portugueseShort : String = "BELÍSSIMO VÍDEO E BELÍSSIMA PAISAGEM,JUNTANDO OS DOIS DA ESTE MARAVILHOSO VÍDEO FORTE ABRAÇOS E OBRIGADO AMIGO"
    private val arabicLong : String = "مشروع عواصم المحافظات يستهدف إنشاء 500 ألف وحدة سكنية\\nقال الدكتور مصطفى مدبولي، رئيس مجلس الوزراء، إن مشروع عواصم المدن والمحافظات يستهدف إنشاء نحو 500 ألف وحدة سكنية.\\nوأضاف « مدبولي» خلال كلمته اليوم الأربعاء، بملتقى بناة مصر بمشاركة وفود عربية وإفريقية: «سعداء بعودة إصدار تقرير التنمية البشرية بمصر بعد توقف طويل».\\nوأوضح رئيس الوزراء، أن هذا العام سيتم الانتهاء من تغطية جيمع المدن بالصرف الصحي، مؤكدا أن الدولة بذلت جهدا هائلا بالقطاع على مدار 3 سنوات ومازال تطوير مستمر"
    private val arabicShort : String = "رئيس الوزراء: مشروع عواصم المحافظات يستهدف إنشاء 500 ألف وحدة سكنية - موقع اليوم الإخباري"

    "Lingua Language Detector" should "predict languages that use the extended Latin alphabet (Romance languages & English)" in {
        detector.detectLanguage( portugueseShort ) shouldBe Languages.PORTUGUESE
        detector.detectLanguage( spanishShort ) shouldBe Languages.SPANISH
        detector.detectLanguage( spanishLong ) shouldBe Languages.SPANISH
    }

    "Lingua Language Detector" should "predict languages that use the Russian character set" in {
        detector.detectLanguage( russianShort ) shouldBe Languages.RUSSIAN
        detector.detectLanguage( russianLong ) shouldBe Languages.RUSSIAN
    }

    "Lingua Language Detector" should "predict languages that use the Arabic character set" in {
        detector.detectLanguage( arabicShort ) shouldBe Languages.ARABIC
        detector.detectLanguage( arabicLong ) shouldBe Languages.ARABIC
    }

}
