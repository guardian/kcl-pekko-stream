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
val amazonSdkV2 = "2.44.13"

val slf4j = "org.slf4j" % "slf4j-api" % "1.7.32"
val logback = "ch.qos.logback" % "logback-classic" % "1.5.19"
val amazonKinesisClient = "software.amazon.kinesis" % "amazon-kinesis-client" % "3.4.3"
val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.5.0"
val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
val scalaMock = "org.scalamock" %% "scalamock" % "5.1.0"
val pekkoStream = "org.apache.pekko" %% "pekko-stream" % PekkoVersion
val pekkoStreamTestkit = "org.apache.pekko" %% "pekko-stream-testkit" % PekkoVersion

// Bringing the amazon packages in directly so we can specify a version that includes
// a patched netty version
val amazonKinesis = "software.amazon.awssdk" % "kinesis" % amazonSdkV2
val amazonDynamo = "software.amazon.awssdk" % "dynamodb" % amazonSdkV2
val amazonDynamoEnhanced = "software.amazon.awssdk" % "dynamodb-enhanced" % amazonSdkV2
val amazonCloudwatch = "software.amazon.awssdk" % "cloudwatch" % amazonSdkV2
val amazonNetty = "software.amazon.awssdk" % "netty-nio-client" % amazonSdkV2
val amazonSdkCore = "software.amazon.awssdk" % "sdk-core" % amazonSdkV2
val amazonAwsCore = "software.amazon.awssdk" % "aws-core" % amazonSdkV2
val amazonArn = "software.amazon.awssdk" % "arns" % amazonSdkV2
val amazonRegions = "software.amazon.awssdk" % "regions" % amazonSdkV2
val amazonUtils = "software.amazon.awssdk" % "utils" % amazonSdkV2
val amazonHttp = "software.amazon.awssdk" % "http-client-spi" % amazonSdkV2
val amazonAnnotations = "software.amazon.awssdk" % "annotations" % amazonSdkV2



libraryDependencies ++= Seq(
  pekkoStream,
  amazonKinesisClient,
  amazonKinesis,
  amazonDynamo,
  amazonDynamoEnhanced,
  amazonCloudwatch,
  amazonNetty,
  amazonSdkCore,
  amazonAwsCore,
  amazonArn,
  amazonRegions,
  amazonUtils,
  amazonHttp,
  amazonAnnotations,
  slf4j,
  scalaCollectionCompat,
  scalaTest % Test,
  pekkoStreamTestkit % Test,
  logback % Test,
  scalaMock % Test
)

dependencyOverrides ++= Seq(
  "org.apache.avro" % "avro" % "1.11.4",
  "org.json" % "json" % "20231013",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.19.1",
)
