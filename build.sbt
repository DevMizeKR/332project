ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.5.2"

lazy val root = (project in file("."))
  .settings(
    name := "332project"
  )

libraryDependencies += "io.grpc" % "grpc-netty" % "1.65.1"
libraryDependencies += "io.grpc" % "grpc-protobuf" % "1.65.1"
libraryDependencies += "io.grpc" % "grpc-stub" % "1.64.0"

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)