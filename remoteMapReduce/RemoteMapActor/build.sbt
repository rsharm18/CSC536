lazy val root = (project in file(".")).
  settings (
    name := "RemoteMapActor",
    version := "1.0",
    scalaVersion := "2.13.1",
    scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation"),
    resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/",
    libraryDependencies +="org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.4",
    libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.6.4"
  )
