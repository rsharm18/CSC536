//1import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
//2import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

val akkaVersion = "2.6.4"

lazy val `RAFT_Imlepementation` = project
  .in(file("."))
  //3  .settings(multiJvmSettings: _*)
  .settings(
    organization := "com.typesafe.akka.samples",
    scalaVersion := "2.13.1",
    scalacOptions in Compile ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    javacOptions in Compile ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    javaOptions in run ++= Seq("-Xms128m", "-Xmx1024m", "-Djava.library.path=./target/native"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      //      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion, // needed for Factorial
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion, // needed for Stats
      //      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
      //      "org.scalatest" %% "scalatest" % "3.1.1",
      //      "org.scalatest" %% "scalatest" % "3.1.1" % Test,
      //8      "io.kamon" % "sigar-loader" % "1.6.6-rev002"
    )
  )
//7  .configs (MultiJvm)

