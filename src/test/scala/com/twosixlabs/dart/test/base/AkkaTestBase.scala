package com.twosixlabs.dart.test.base

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.BeforeAndAfterEach

abstract class AkkaTestBase extends ScalaTestWithActorTestKit with StandardTestBase3x with BeforeAndAfterEach
