package com.twosixlabs.dart.service

import com.github.pemistahl.lingua.api.{Language, LanguageDetectorBuilder, LanguageDetector => Lingua}

object Languages {
    val ENGLISH : String = Language.ENGLISH.getIsoCode639_3.toString
    val SPANISH = Language.SPANISH.getIsoCode639_3.toString
    val PORTUGUESE = Language.PORTUGUESE.getIsoCode639_3.toString
    val ARABIC = Language.ARABIC.getIsoCode639_3.toString
    val RUSSIAN = Language.RUSSIAN.getIsoCode639_3.toString
}

trait LanguageDetector {

    def detectLanguage( text : String ) : String

}

class LinguaLanguageDetector extends LanguageDetector {

    private val detector : Lingua = LanguageDetectorBuilder.fromAllBuiltInSpokenLanguages().build()

    override def detectLanguage( text : String ) : String = {
        detector.detectLanguageOf( text ).getIsoCode639_3.toString
    }
}

class NoOpLanguageDetector extends LanguageDetector {
    override def detectLanguage( text : String ) : String = Languages.ENGLISH
}
