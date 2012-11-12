import AssemblyKeys._ // put this at the top of the file


assemblySettings


name := "GraphLab"

version := "1.0-spark"

scalaVersion := "2.9.2"


resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Spray Repository" at "http://repo.spray.cc/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
    )


libraryDependencies  ++= Seq(
            // other dependencies here
            // pick and choose:
            //"org.scalanlp" %% "breeze-math" % "0.1",
            //"org.scalanlp" %% "breeze-learn" % "0.1",
            //"org.scalanlp" %% "breeze-process" % "0.1",
            //"org.scalanlp" %% "breeze-viz" % "0.1"
            "org.spark-project" % "spark-core_2.9.2" % "0.7.0"
)

libraryDependencies += "com.yammer.metrics" % "metrics-core" % "3.0.0-SNAPSHOT"


libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.3"


libraryDependencies += "com.typesafe.akka" % "akka-remote" % "2.0.3"


libraryDependencies += "com.typesafe.akka" % "akka-slf4j" % "2.0.3"



libraryDependencies += "io.netty" % "netty" % "3.5.9.Final"


libraryDependencies += "asm" % "asm-all" % "3.3.1"

libraryDependencies += "org.javassist" % "javassist" % "3.15.0-GA"
