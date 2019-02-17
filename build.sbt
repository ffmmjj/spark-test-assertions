name := "spark-test-assertions"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.2" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.2" % Provided

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.2" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.2" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test


// POM settings for Sonatype
organization := "com.github.ffmmjj"
homepage := Some(url("https://github.com/ffmmjj/spark-test-assertions"))
scmInfo := Some(ScmInfo(url("https://github.com/username/spark-test-assertions"),
                            "git@github.com:ffmmjj/spark-test-assertions.git"))
developers := List(Developer("ffmmjj",
                             "Felipe",
                             "ffmmjj@gmail.com",
                             url("https://github.com/ffmmjj")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true
publishTo := sonatypePublishTo.value

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)
useGpg := true
updateOptions := updateOptions.value.withGigahorse(false)
