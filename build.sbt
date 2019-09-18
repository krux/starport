val awsSdkVersion = "1.11.618"
val slickVersion = "3.2.3"

val scalaTestArtifact      = "org.scalatest"          %% "scalatest"            % "3.0.5" % "test"
val slickArtifact          = "com.typesafe.slick"     %% "slick"                % slickVersion
val slickHikaricpArtifact  = "com.typesafe.slick"     %% "slick-hikaricp"       % slickVersion
val scoptArtifact          = "com.github.scopt"       %% "scopt"                % "3.7.0"
val configArtifact         = "com.typesafe"           %  "config"               % "1.3.3"
val nscalaTimeArtifact     = "com.github.nscala-time" %% "nscala-time"          % "2.18.0"
val hyperionArtifact       = "com.krux"               %% "hyperion"             % "5.4.1"
val slf4jApiArtifact       = "org.slf4j"              %  "slf4j-api"            % "1.7.12"
val logbackClassicArtifact = "ch.qos.logback"         %  "logback-classic"      % "1.1.7"
val awsSdkS3               = "com.amazonaws"          %  "aws-java-sdk-s3"      % awsSdkVersion
val awsSdkSES              = "com.amazonaws"          %  "aws-java-sdk-ses"     % awsSdkVersion
val awsSdkSSM              = "com.amazonaws"          %  "aws-java-sdk-ssm"     % awsSdkVersion
val awsSdkSNS              = "com.amazonaws"          %  "aws-java-sdk-sns"     % awsSdkVersion
val stubbornArtifact       = "com.krux"               %% "stubborn"             % "1.3.0"
val metricsGraphite        = "io.dropwizard.metrics"  %  "metrics-graphite"     % "4.0.2"
val postgreSqlJdbc         = "org.postgresql"         %  "postgresql"           % "42.2.4"
val awsLambdaEvents        = "com.amazonaws"          %  "aws-lambda-java-events" % "2.2.1"
val awsLambdaCore          = "com.amazonaws"          %  "aws-lambda-java-core"   % "1.2.0"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint", "-Xfatal-warnings"),
  scalaVersion := "2.12.8",
  libraryDependencies += scalaTestArtifact,
  organization := "com.krux",
  test in assembly := {},  // skip test during assembly
  publishMavenStyle := true
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(name := "starport").
  aggregate(core,lambda)

lazy val core = (project in file("starport-core")).
  settings(commonSettings: _*).
  enablePlugins(BuildInfoPlugin).
  settings(
    name := "starport-core",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.krux.starport",
    assemblyJarName in assembly := "starport-core.jar",
    libraryDependencies ++= Seq(
      scalaTestArtifact,
      slickArtifact,
      slickHikaricpArtifact,
      scoptArtifact,
      nscalaTimeArtifact,
      configArtifact,
      slf4jApiArtifact,
      logbackClassicArtifact,
      hyperionArtifact,
      awsSdkS3,
      awsSdkSES,
      awsSdkSSM,
      awsSdkSNS,
      stubbornArtifact,
      metricsGraphite,
      postgreSqlJdbc
    ),
    fork := true
  )

lazy val lambda = (project in file("starport-lambda")).
  settings(commonSettings: _*).
  settings(
    name := "starport-lambda",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.krux.starport",
    assemblyJarName in assembly := "starport-lambda.jar",
    libraryDependencies ++= Seq(
      awsLambdaCore,
      awsLambdaEvents,
    ),
    fork := true
  ).dependsOn(core)
