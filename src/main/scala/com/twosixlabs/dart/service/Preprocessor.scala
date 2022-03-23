package com.twosixlabs.dart.service

import com.twosixlabs.cdr4s.core.CdrDocument
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.model.util.ModelBuilder
import org.eclipse.rdf4j.model.vocabulary.{RDF, XMLSchema}
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.time.format.DateTimeFormatter

trait Preprocessor {

    def preprocess( cdr : CdrDocument ) : CdrDocument

}
