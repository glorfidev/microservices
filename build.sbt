ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "3.8.4"

val pekkoVersion = "2.0.0"
val pekkoHttpVersion = "2.0.0"
val arteryJavaOption = "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"

lazy val root: Project = project.in(file(".")).aggregate(LocalProject("toolkit")).settings(
  name := "microservices",
  Compile / scalacOptions ++= Seq("-encoding", "UTF-8", "-release", "17", "-deprecation", "-feature", "-unchecked"),
  Compile / javacOptions ++= Seq("--release", "17", "-Xlint:unchecked", "-Xlint:deprecation"),
  libraryDependencies ++= Seq(
    "org.apache.pekko" %% "pekko-actor" % pekkoVersion,
    "org.apache.pekko" %% "pekko-remote" % pekkoVersion,
    "org.apache.pekko" %% "pekko-cluster" % pekkoVersion,
    "org.apache.pekko" %% "pekko-distributed-data" % pekkoVersion,
    "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
    "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoVersion,
    "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
    "org.apache.pekko" %% "pekko-persistence" % pekkoVersion,
    "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
    "org.iq80.leveldb" % "leveldb" % "0.12",
    "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
    "ch.qos.logback" % "logback-classic" % "1.5.29",
    "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % Test,
    "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpVersion % Test,
    "org.scalatest" %% "scalatest" % "3.2.20" % Test
  ),
  Compile / run / fork := true,
  Compile / run / javaOptions += arteryJavaOption,
  Test / fork := true,
  Test / javaOptions ++= Seq(arteryJavaOption, "--add-opens=java.base/java.nio=ALL-UNNAMED")
)

lazy val toolkit = project.in(file("toolkit")).settings(
  name := "job-cluster-toolkit",
  Compile / scalacOptions ++= Seq("-release", "17", "-deprecation", "-unchecked"),
  Compile / run / fork := true,
  Compile / run / javaOptions ++= {
    val converter = (LocalProject("root") / fileConverter).value
    val engineClasspath = (LocalProject("root") / Runtime / fullClasspath).value
      .map(entry => converter.toPath(entry.data).toString)
      .mkString(java.io.File.pathSeparator)
    Seq(
      s"-Dtoolkit.engine.classpath=$engineClasspath",
      s"-Dtoolkit.data-dir=${(LocalProject("root") / baseDirectory).value / ".toolkit-data"}"
    )
  },
  libraryDependencies ++= Seq(
    "org.playframework" %% "play-pekko-http-server" % "3.0.12",
    "ch.qos.logback" % "logback-classic" % "1.5.29",
    "org.scalatest" %% "scalatest" % "3.2.20" % Test
  )
)
