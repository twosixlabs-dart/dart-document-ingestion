package com.twosixlabs.dart.pipeline.stage

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import com.twosixlabs.dart.AppContext
import com.twosixlabs.dart.operations.status.PipelineStatus
import com.twosixlabs.dart.operations.status.PipelineStatus.Status
import com.twosixlabs.dart.operations.status.client.PipelineStatusUpdateClient
import com.twosixlabs.dart.pipeline.command.{DocumentProcessing, OperationalStatusFailure, OperationalStatusSuccess}

object OperationalStatusWriter {
    def apply( appContext : AppContext ) : Behavior[ DocumentProcessing ] = {
        OperationalStatusWriter( appContext.opsDatastore )
    }

    def apply( opsClient : PipelineStatusUpdateClient ) : Behavior[ DocumentProcessing ] = {
        Behaviors.setup( context => new OperationalStatusWriter( context, opsClient ) )
    }
}

class OperationalStatusWriter( context : ActorContext[ DocumentProcessing ], opsClient : PipelineStatusUpdateClient ) extends AbstractBehavior[ DocumentProcessing ]( context ) {
    override def onMessage( message : DocumentProcessing ) : Behavior[ DocumentProcessing ] = {
        message match {
            case OperationalStatusSuccess( docId, stageName, processorType, _ ) => {
                opsClient.fireAndForget( new PipelineStatus( docId, stageName, processorType, Status.SUCCESS, "DART", System.currentTimeMillis(), System.currentTimeMillis(), "" ) )
                Behaviors.same
            }
            case OperationalStatusFailure( docId, stageName, processorType, message, _ ) => {
                opsClient.fireAndForget( new PipelineStatus( docId, stageName, processorType, Status.FAILURE, "DART", System.currentTimeMillis(), System.currentTimeMillis(), message ) )
                Behaviors.same
            }
            case _ => Behaviors.unhandled
        }
    }
}
