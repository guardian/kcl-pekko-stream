configs(IntegrationTest)
Defaults.itSettings
val TestAndIntegrationTest = "test,it"

ThisBuild / organization := "com.gu"
ThisBuild / scalaVersion  := "2.13.15"
scalacOptions ++= Seq("-deprecation", "-feature")
ThisBuild / licenses  += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

name := "kcl-pekko-stream"

val AkkaVersion = "2.6.16"

val slf4j = "org.slf4j" % "slf4j-api" % "1.7.32"
val logback = "ch.qos.logback" % "logback-classic" % "1.2.5"
val amazonKinesisClient = "software.amazon.kinesis" % "amazon-kinesis-client" % "2.3.6"
val amazonKinesisProducer = "com.amazonaws" % "amazon-kinesis-producer" % "0.12.11"
val scalaKinesisProducer = "io.github.streetcontxt" %% "kpl-scala" % "2.0.0"
val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.5.0"
val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
val scalaMock = "org.scalamock" %% "scalamock" % "5.1.0"
val akkaStream = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion

libraryDependencies ++= Seq(
  akkaStream,
  amazonKinesisClient,
  slf4j,
  scalaCollectionCompat,
  scalaTest % TestAndIntegrationTest,
  akkaStreamTestkit % TestAndIntegrationTest,
  logback % TestAndIntegrationTest,
  amazonKinesisProducer % TestAndIntegrationTest,
  scalaMock % Test
)
