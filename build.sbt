import AssemblyKeys._ // put this at the top of the file


name := "GraphLab"

version := "1.0"

scalaVersion := "2.9.2"



libraryDependencies  ++= Seq(
            // other dependencies here
            // pick and choose:
            "org.scalanlp" %% "breeze-math" % "0.1",
            "org.scalanlp" %% "breeze-learn" % "0.1",
            "org.scalanlp" %% "breeze-process" % "0.1",
            "org.scalanlp" %% "breeze-viz" % "0.1"
)


// Add Akka actor framework for communication
libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.3"


assemblySettings



