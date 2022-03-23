logLevel := Level.Warn

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

addSbtPlugin( "com.eed3si9n" % "sbt-assembly" % "0.15.0" )
addSbtPlugin( "com.typesafe.sbt" % "sbt-native-packager" % "1.7.6" )
addSbtPlugin( "org.scoverage" % "sbt-scoverage" % "1.6.1" )
addSbtPlugin( "org.xerial.sbt" %% "sbt-pack" % "0.13" )
