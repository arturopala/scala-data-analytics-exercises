name := """scala-data-misc"""

version := "1.0-SNAPSHOT"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

startYear := Some(2015)

description := "Large data set analytics exercises using Scala"

scalaVersion := "2.11.7"

developers := List(Developer("arturopala","Artur Opala","opala.artur@gmail.com",url("https://pl.linkedin.com/in/arturopala")))

resolvers += Resolver.sonatypeRepo("snapshots")

resolvers += Resolver.jcenterRepo

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.5" % Test,
  "org.scalacheck" %% "scalacheck" % "1.12.2" % Test
)

fork := true

javaOptions in run ++= Seq ( "-Xms1024M", "-Xmx4G" )

scalariformSettings

coverageEnabled := true
