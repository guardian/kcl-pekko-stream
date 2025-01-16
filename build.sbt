import ReleaseTransformations.*
import sbtversionpolicy.withsbtrelease.ReleaseVersion

name := "kcl-pekko-stream"
organization := "com.gu"
scalaVersion  := "2.13.15"
scalacOptions ++= Seq("-deprecation", "-feature", "-release:11")
licenses := Seq(License.Apache2)

releaseCrossBuild := false // currently not cross-building, remember to change to true if we start
releaseVersion := ReleaseVersion.fromAggregatedAssessedCompatibilityWithLatestRelease().value
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  setNextVersion,
  commitNextVersion
)

val PekkoVersion = "1.1.2"

val slf4j = "org.slf4j" % "slf4j-api" % "1.7.32"
val logback = "ch.qos.logback" % "logback-classic" % "1.2.5"
val amazonKinesisClient = "software.amazon.kinesis" % "amazon-kinesis-client" % "2.3.6"
val amazonKinesisProducer = "com.amazonaws" % "amazon-kinesis-producer" % "0.12.11"
val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.5.0"
val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
val scalaMock = "org.scalamock" %% "scalamock" % "5.1.0"
val pekkoStream = "org.apache.pekko" %% "pekko-stream" % PekkoVersion
val pekkoStreamTestkit = "org.apache.pekko" %% "pekko-stream-testkit" % PekkoVersion

configs(IntegrationTest)
Defaults.itSettings
val TestAndIntegrationTest = "test,it"

libraryDependencies ++= Seq(
  pekkoStream,
  amazonKinesisClient,
  slf4j,
  scalaCollectionCompat,
  scalaTest % TestAndIntegrationTest,
  pekkoStreamTestkit % TestAndIntegrationTest,
  logback % TestAndIntegrationTest,
  amazonKinesisProducer % TestAndIntegrationTest,
  scalaMock % Test
)
