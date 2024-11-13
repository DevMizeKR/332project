ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "332project"
  )

libraryDependencies += "io.grpc" % "grpc-netty" % "1.65.1"
libraryDependencies += "io.grpc" % "grpc-protobuf" % "1.65.1"
libraryDependencies += "io.grpc" % "grpc-stub" % "1.64.0"

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.0")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.15"