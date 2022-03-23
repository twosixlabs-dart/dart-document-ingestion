package com.twosixlabs.dart.serialization.json

import com.twosixlabs.cdr4s.core.CdrFormat
import com.twosixlabs.cdr4s.json.dart.{DartJsonFormat, LadleJsonFormat}

object JsonDataFormats {
    val DART_JSON : CdrFormat = new DartJsonFormat
    val LADLE_JSON : CdrFormat = new LadleJsonFormat
    val FACTIVA_JSON : FactivaJsonFormat = new FactivaJsonFormat
    val INGEST_PROXY_JSON : IngestProxyEventJsonFormat = new IngestProxyEventJsonFormat
    val DEDUPLICATION_JSON : DeduplicationJsonFormat = new DeduplicationJsonFormat
    val PULSE_JSON : PulseJsonFormat = new PulseJsonFormat

}
