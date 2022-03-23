//package com.twosixlabs.dart.pipeline.stage
//
//import akka.actor.typed.ActorRef
//import com.twosixlabs.dart.operations.status.PipelineStatus
//import com.twosixlabs.dart.operations.status.PipelineStatus.{ProcessorType, Status}
//import com.twosixlabs.dart.operations.status.client.PipelineStatusUpdateClient
//import com.twosixlabs.dart.pipeline.command.{DocumentProcessing, OperationalStatusFailure, OperationalStatusSuccess}
//import com.twosixlabs.dart.test.base.AkkaTestBase
//import com.twosixlabs.dart.test.utils.TestObjectMother.CDR_TEMPLATE
//import org.mockito.captor.ArgCaptor
//
//import java.lang.Thread.sleep
//
//class OperationalStatusWriterTestSuite extends AkkaTestBase {
//    private val statusClient = mock[ PipelineStatusUpdateClient ]
//
//    override def beforeEach( ) = reset( statusClient )
//
//    override def afterEach( ) = reset( statusClient )
//
//    "Operational Status Writer" should "post a successful operation status" in {
//
//        val pipelineStatusCaptor = ArgCaptor[ PipelineStatus ]
//        val doc = CDR_TEMPLATE.copy( documentId = "1a" )
//
//        val statusWriter : ActorRef[ DocumentProcessing ] = testKit.spawn( OperationalStatusWriter( statusClient ), "test-success" )
//
//        when( statusClient.fireAndForget( * ) ).thenAnswer()
//
//        statusWriter ! OperationalStatusSuccess( doc.documentId, "test", ProcessorType.CORE )
//        sleep( 500 )
//
//        verify( statusClient ).fireAndForget( pipelineStatusCaptor )
//
//        val pipelineStatus : PipelineStatus = pipelineStatusCaptor.value
//        pipelineStatus.getDocumentId shouldBe "1a"
//        pipelineStatus.getApplicationId shouldBe "test"
//        pipelineStatus.getProcessorType shouldBe ProcessorType.CORE
//        pipelineStatus.getStatus shouldBe Status.SUCCESS
//        pipelineStatus.getScope shouldBe "DART"
//        pipelineStatus.getMessage shouldBe ""
//    }
//
//    "Operational Status Writer" should "post a failure operation status" in {
//
//        val doc = CDR_TEMPLATE.copy( documentId = "1a" )
//
//        val statusWriter : ActorRef[ DocumentProcessing ] = testKit.spawn( OperationalStatusWriter( statusClient ), "test-failure" )
//        val pipelineStatusCaptor = ArgCaptor[ PipelineStatus ]
//
//
//        when( statusClient.fireAndForget( * ) ).thenAnswer()
//
//        statusWriter ! OperationalStatusFailure( doc.documentId, "test", ProcessorType.CORE, "ERROR" )
//        sleep( 500 )
//
//        verify( statusClient ).fireAndForget( pipelineStatusCaptor )
//
//        val pipelineStatus : PipelineStatus = pipelineStatusCaptor.value
//        pipelineStatus.getDocumentId shouldBe "1a"
//        pipelineStatus.getApplicationId shouldBe "test"
//        pipelineStatus.getProcessorType shouldBe ProcessorType.CORE
//        pipelineStatus.getStatus shouldBe Status.FAILURE
//        pipelineStatus.getScope shouldBe "DART"
//        pipelineStatus.getMessage shouldBe "ERROR"
//
//    }
//}
