addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.0")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.11")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.15"