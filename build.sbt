name := "Hoodie"

version := "0.0"

scalaVersion := "2.9.1"

organization := "be.bolder"

resolvers += ScalaToolsSnapshots

// resolvers += "Tinkerpop" at "http://tinkerpop.com/maven2"

resolvers += "Fusesource Snapshots" at "http://repo.fusesource.com/nexus/content/repositories/snapshots"

libraryDependencies += "org.fusesource.hawtdispatch" % "hawtdispatch" % "1.5-SNAPSHOT"

// libraryDependencies += "org.fusesource.hawtdispatch" %% "hawtdispatch" % "1.5" from "http://repo.fusesource.com/nexus/content/repositories/snapshots/org/fusesource/hawtdispatch/hawtdispatch/1.5-SNAPSHOT/hawtdispatch-1.5-20110719.020420-2.jar"

libraryDependencies += "org.fusesource.hawtdispatch" % "hawtdispatch-scala" % "1.5-SNAPSHOT"
// libraryDependencies += "org.fusesource.hawtdispatch" %% "hawtdispatch-scala" % "1.5" from "http://repo.fusesource.com/nexus/content/repositories/snapshots/org/fusesource/hawtdispatch/hawtdispatch-scala/1.5-SNAPSHOT/hawtdispatch-scala-1.5-20110719.020420-2.jar"

// libraryDependencies += "org.scala-tools.testing" %% "scalacheck" % "2.8" % "test"

// libraryDependencies += "com.tinkerpop.blueprints" %% "blueprints-core" % "0.9" // from "http://tinkerpop.com/maven2/com/tinkerpop/blueprints/blueprints-core/0.9/blueprints-core-0.9.jar"

// libraryDependencies += "com.tinkerpop.blueprints" %% "blueprints-neo4j-graph" % "0.9" // from "http://tinkerpop.com/maven2/com/tinkerpop/blueprints/blueprints-neo4j-graph/0.9/blueprints-neo4j-graph-0.9.jar"

maxErrors := 20

pollInterval := 1000

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

scalacOptions += "-deprecation"

initialCommands in console := "import be.bolder.hoodie._"

// mainClass in (Compile, packageBin) := Some("be.bolder.hoodie.Hoodie")
