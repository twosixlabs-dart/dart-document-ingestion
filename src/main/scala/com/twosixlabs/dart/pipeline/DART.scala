package com.twosixlabs.dart.pipeline

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, Routers}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import com.twosixlabs.dart.AppContext
import com.twosixlabs.dart.pipeline.command.{DocumentProcessing, IngestProxy}
import com.twosixlabs.dart.pipeline.stage.{DartPipelineStage, NotificationPublisher, OperationalStatusWriter}
import com.twosixlabs.dart.utils.Utils.actorUuid
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext

object DART {
    def apply( appContext : AppContext ) : Behavior[ DocumentProcessing ] = Behaviors.setup[ DocumentProcessing ]( akkaSystemContext => new DART( akkaSystemContext, appContext ) )
}

class DART( context : ActorContext[ DocumentProcessing ], appContext : AppContext ) extends AbstractBehavior[ DocumentProcessing ]( context ) {

    implicit val executionContext : ExecutionContext = ExecutionContext.global

    private val notifications : ActorRef[ DocumentProcessing ] = {
        val notificationConfig : Config = appContext.config.getConfig( "notifications" )
        val pool = Routers.pool( notificationConfig.getInt( "pool.size" ) )( Behaviors.supervise( NotificationPublisher( appContext, notificationConfig ) ).onFailure[ Exception ]( SupervisorStrategy.restart ) )
        context.spawn( pool, "notifications-pool" )
    }

    private val operations : ActorRef[ DocumentProcessing ] = {
        val operationalStatusConfig : Config = appContext.config.getConfig( "operations" )
        val pool = Routers.pool( operationalStatusConfig.getInt( "pool.size" ) )( Behaviors.supervise( OperationalStatusWriter( appContext ) ).onFailure[ Exception ]( SupervisorStrategy.restart ) )
        context.spawn( pool, "operational-status-pool" )
    }

    // this is the main pipeline router
    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case event@IngestProxy( docId, _ ) => {
                context.log.debug( s"${docId} - launching document processing actor" )
                val dart = context.spawn( DartPipelineStage( docId, appContext, notifications, operations ), s"${docId}-dart-pipeline-${actorUuid}" )
                dart ! event

                Behaviors.same
            }
            case _ => Behaviors.unhandled
        }
    }

}
