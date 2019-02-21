import sbt._

object dependencies {
  val sparkDependencies: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % "2.1.2",
    "org.apache.spark" %% "spark-sql" % "2.1.2"
  )

  val testDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % "3.0.5"
  )
}
