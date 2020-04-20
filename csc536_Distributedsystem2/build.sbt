lazy val root = (project in file(".")).
  settings (
    name := "hw_tokenRing_Snapshot",
    version := "1.0",
    scalaVersion := "2.13.1",
    scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation"),
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.4"
  )