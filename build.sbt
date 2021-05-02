name := "SimpleData"

version := "0.1"

scalaVersion := "2.11.12"

idePackagePrefix := Some("org.rojosam")

organization := "rojosam"

scalacOptions ++= Seq("-deprecation", "-feature")

resolvers += "aws-glue-etl-artifacts Maven2 Repository" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/"

val log4jVersion      = "2.12.1"

libraryDependencies ++=  Seq(
  "com.amazonaws"                 % "AWSGlueETL"              % "1.0.0"               % "provided",
  "com.amazonaws"                 % "aws-java-sdk"            % "1.11.754"            % "provided",
  "com.holdenkarau"              %% "spark-testing-base"      % "2.4.3_0.14.0"        % "test",
  "org.scalactic"                %% "scalactic"               % "3.0.8"               % "test",
  "org.scalatest"                %% "scalatest"               % "3.0.8"               % "test",
  "org.apache.logging.log4j"      % "log4j-core"              % log4jVersion          % "provided",
  "org.apache.logging.log4j"      % "log4j-api"               % log4jVersion          % "provided",
  "org.apache.logging.log4j"      % "log4j-slf4j-impl"        % log4jVersion          % "provided",
  "org.apache.logging.log4j"      % "log4j-1.2-api"           % log4jVersion          % "provided",
  "com.github.scopt"             %% "scopt"                   % "3.7.1",
  "org.scala-lang"                % "scala-reflect"           % "2.11.12"
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12"),
  ExclusionRule("javax.servlet", "servlet-api")
)

Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

dependencyOverrides += "com.fasterxml.jackson.core"  % "jackson-databind" % "2.6.7.2" % "test"


artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" +module.revision + "." + artifact.extension
}

assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)

val assemblyJar = s"simpledata_2.11-0.1.jar"

assembly / assemblyJarName  := assemblyJar

assembly / test := {}

Test / parallelExecution := false

Test / coverageEnabled := true
coverageMinimum := 80
coverageFailOnMinimum := true
coverageHighlighting := true



