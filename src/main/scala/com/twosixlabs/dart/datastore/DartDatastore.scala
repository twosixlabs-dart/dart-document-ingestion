package com.twosixlabs.dart.datastore

import com.twosixlabs.cdr4s.core.{CdrAnnotation, CdrDocument}

import scala.concurrent.Future

object DartDatastore {
    final val GET_BY_ID_OP : String = "DartDatastore.getByDocId"
    final val SAVE_DOCUMENT_OP : String = "DartDatastore.saveDocument"
    final val ADD_ANNOTATION_OP : String = "DartDatastore.addAnnotation"
    final val EXISTS_OP : String = "DartDatastore.exists"
    final val TENANT_MEMBERSHIPS_OP : String = "DartDatastore.tenantMembershipsFor"
    final val UPDATE_TENANT_OP : String = "DartDatastore.updateTenant"
}

trait DartDatastore {

    val timeoutMs : Int = 2000

    def getByDocId( docId : String ) : Future[ Option[ CdrDocument ] ]

    def saveDocument( doc : CdrDocument ) : Future[ Unit ]

    def addAnnotation( docId : CdrDocument, annotation : CdrAnnotation[ _ ] ) : Future[ Unit ]

    def exists( docId : String ) : Future[ Boolean ]

    def tenantMembershipsFor( docId : String ) : Future[ Set[ String ] ]

    def updateTenant( docId : String, tenant : String ) : Future[ String ]

}
