// ~/Documents/Repositories/RPi-Cluster-Spark/tpch-scala/build.sbt
scalaVersion := "2.13.18"

name := "tpch-benchmark"
version := "0.1.0"

// "provided" means: don't bundle Spark in the jar, it'll be on the cluster already
// libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.2.0-SNAPSHOT" % "provided"

// Tell sbt where your custom Spark jars are, so it can compile against them
unmanagedJars in Compile ++= {
  val sparkJarsDir = file(sys.env.getOrElse("SPARK_HOME", 
    sys.props("user.home") + "/Documents/Repositories/spark/assembly/target/scala-2.13/jars"))
  (sparkJarsDir ** "*.jar").classpath
}