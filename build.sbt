
val akkaVersion = "2.6.3"

lazy val `dNSG` = project
  .in(file("."))
  .settings(
    scalaVersion := "2.13.1",
    Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    run / javaOptions ++= Seq("-Xms128m", "-Xmx1024m", "-Djava.library.path=./target/native"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed"           % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-typed"         % akkaVersion,
      "ch.qos.logback"    %  "logback-classic"             % "1.2.3",
      "com.typesafe.akka" %% "akka-multi-node-testkit"    % akkaVersion % Test,
      "org.scalatest"     %% "scalatest"                  % "3.0.8"     % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed"   % akkaVersion % Test),

    licenses := Seq(("CC0", url("http://creativecommons.org/publicdomain/zero/1.0")))
  )

// set main class for assembly
mainClass in assembly := Some("com.github.julkw.dnsg.Main")

// skip tests during assembly
test in assembly := {}

// don't include logging configuration file
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  //case x => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
