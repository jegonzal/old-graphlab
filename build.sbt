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

resolvers ++= Seq(
            // other resolvers here
            // if you want to use snapshot builds (currently 0.2-SNAPSHOT), use this.
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)


