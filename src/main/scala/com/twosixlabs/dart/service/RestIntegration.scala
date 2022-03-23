package com.twosixlabs.dart.service

import okhttp3.MediaType

trait RestIntegration {

    protected val JSON_MEDIA_TYPE : MediaType = MediaType.get( "application/json; charset=utf-8" )
    val systemName : String

}
