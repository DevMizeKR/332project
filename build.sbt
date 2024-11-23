ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "332project"
  )

libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
libraryDependencies += "io.grpc" % "grpc-netty" % "1.65.1"
libraryDependencies += "io.grpc" % "grpc-protobuf" % "1.65.1"
libraryDependencies += "io.grpc" % "grpc-stub" % "1.64.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.6"

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)