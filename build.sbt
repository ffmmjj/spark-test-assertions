name := "spark-test-assertions"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.2" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.2" % Provided

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.2" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.2" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
