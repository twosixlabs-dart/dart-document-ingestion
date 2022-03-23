package com.twosixlabs.dart.pipeline.stage

import com.twosixlabs.dart.pipeline.stage.Overrides.DEFAULT_GENRE
import com.twosixlabs.dart.serialization.json.IngestProxyEvent

case class Overrides( labels : Set[ String ] = Set(),
                      sourceUri : Option[ String ] = None,
                      tenants : Set[ String ] = Set(),
                      genre : String = DEFAULT_GENRE,
                      runAnnotators : Boolean = false )

object Overrides {

    val DEFAULT_GENRE : String = "unspecified"

    def fromIngestMetadata( event : IngestProxyEvent ) : Overrides = {
        if ( event.metadata == null ) Overrides()
        else {
            val labelsWorkaround : Set[ String ] = event.metadata.labels match {
                case labels : Set[ String ] => labels
                case null => Set()
            }

            val sourceUri : Option[ String ] = event.metadata.sourceUri

            val tenantsWorkaround : Set[ String ] = event.metadata.tenants match {
                case tenants : Set[ String ] => tenants
                case null => Set()
            }

            val genre : String = {
                if ( event.metadata.genre != null && event.metadata.genre.nonEmpty && event.metadata.genre != DEFAULT_GENRE ) {
                    event.metadata.genre
                } else DEFAULT_GENRE
            }

            val runAnnotators : Boolean = event.getMetadata.reannotate

            Overrides( labels = labelsWorkaround, sourceUri = sourceUri, tenants = tenantsWorkaround, genre = genre, runAnnotators = runAnnotators )
        }
    }
}
