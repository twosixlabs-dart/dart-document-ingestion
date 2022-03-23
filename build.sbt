import Dependencies._
import sbt.Tests.Argument
import sbt._

organization in ThisBuild := "com.twosixlabs.dart"
name := "dart-document-ingestion"
scalaVersion in ThisBuild := "2.12.7"

resolvers in ThisBuild ++= Seq( "Maven Central" at "https://repo1.maven.org/maven2/",
                                "Local Ivy Repository" at s"file://${System.getProperty( "user.home" )}/.ivy2/local/default" )

lazy val root = ( project in file( "." ) )
  .settings( libraryDependencies ++= akka
                                     ++ rdf4j
                                     ++ lingua
                                     ++ dedup
                                     ++ cdr4s
                                     ++ dartTextUtils
                                     ++ arangoDatastoreRepo
                                     ++ ontologyRegistry
                                     ++ elastic4s
                                     ++ operations
                                     ++ kafka
                                     ++ okhttp
                                     ++ dartCommons
                                     ++ scopt
                                     ++ betterFiles
                                     ++ logging
                                     ++ java8Compat
                                     ++ unitTesting
                                     ++ embeddedKafka,
             excludeDependencies ++= Seq( ExclusionRule( "org.slf4j", "slf4j-log4j12" ),
                                          ExclusionRule( "org.slf4j", "log4j-over-slf4j" ),
                                          ExclusionRule( "log4j", "log4j" ),
                                          ExclusionRule( "org.apache.logging.log4j", "log4j-core" ) ),
             dependencyOverrides ++= Seq( "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.5",
                                          "com.arangodb" %% "velocypack-module-scala" % "1.2.0",
                                          "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.10.5",
                                          "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.5" ) )

mainClass in(Compile, run) := Some( "Main" )

enablePlugins( JavaAppPackaging )
enablePlugins( PackPlugin )


// don't run tests when build the fat jar, use sbt test instead for that (takes too long when building the image)
test in assembly := {}

coverageExcludedPackages := ".*Main.*;.*Context.*;.*DART.*;.*exception.*;.*Provider.*;.*serialization.*;.*NoOp.*;.*InMemory.*"

assemblyMergeStrategy in assembly := {
    case PathList( "META-INF", "MANIFEST.MF" ) => MergeStrategy.discard
    case PathList( "META-INF", "*.SF" ) => MergeStrategy.discard
    case PathList( "META-INF", "*.DSA" ) => MergeStrategy.discard
    case PathList( "META-INF", "*.RSA" ) => MergeStrategy.discard
    case PathList( "META-INF", "*.DEF" ) => MergeStrategy.discard
    case PathList( "*.SF" ) => MergeStrategy.discard
    case PathList( "*.DSA" ) => MergeStrategy.discard
    case PathList( "*.RSA" ) => MergeStrategy.discard
    case PathList( "*.DEF" ) => MergeStrategy.discard
    case PathList( "reference.conf" ) => MergeStrategy.concat
    case _ => MergeStrategy.last
}

parallelExecution in Test := false

testOptions in Test := Seq( Argument( "-oI" ) )
