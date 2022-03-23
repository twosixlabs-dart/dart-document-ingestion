package com.twosixlabs.dart

import com.twosixlabs.dart.service.annotator.{Annotator, AnnotatorDef, RestAnnotator}
import okhttp3.OkHttpClient

class AnnotatorFactory( httpClient : OkHttpClient ) {

    def newAnnotator( conf : AnnotatorDef ) : Annotator = new RestAnnotator( conf, httpClient )

}
