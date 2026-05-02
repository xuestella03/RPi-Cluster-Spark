error id: file://<WORKSPACE>/tpch-scala/src/main/scala/TpchBenchmark.scala:scala/collection/MapOps#getOrElse().
file://<WORKSPACE>/tpch-scala/src/main/scala/TpchBenchmark.scala
empty definition using pc, found symbol in pc: scala/collection/MapOps#getOrElse().
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -sys/env/getOrElse.
	 -sys/env/getOrElse#
	 -sys/env/getOrElse().
	 -scala/Predef.sys.env.getOrElse.
	 -scala/Predef.sys.env.getOrElse#
	 -scala/Predef.sys.env.getOrElse().
offset: 662
uri: file://<WORKSPACE>/tpch-scala/src/main/scala/TpchBenchmark.scala
text:
```scala
// ~/Documents/Repositories/RPi-Cluster-Spark/tpch-scala/src/main/scala/TpchBenchmark.scala
package tpch

import org.apache.spark.sql.SparkSession
import java.io.{FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TpchBenchmark {

  // Mirror your config.py values here
  val executorMemory   = sys.env.getOrElse("EXECUTOR_MEMORY", "768m")
  val masterUrl        = sys.env.getOrElse("SPARK_MASTER_URL", "spark://192.168.50.65:7077")
  val dataPath         = sys.env.getOrElse("DATA_PATH", 
    "/home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf1.5")
  val resultsDir       = sys.env.getOrEl@@se("RESULTS_DIR",
    "<WORKSPACE>/tpch/results/scala")
  val activeConfig     = sys.env.getOrElse("ACTIVE_CONFIG", "default")
  val sf               = sys.env.getOrElse("SF", "1.5")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TPC-H Benchmark Scala")
      .master(masterUrl)
      .config("spark.executor.memory", executorMemory)
      .config("spark.executor.cores", "4")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.memory", "2g")
      .config("spark.memory.fraction", "0.45")
      .config("spark.memory.storageFraction", "0.5")
      .getOrCreate()

    println(s"Spark UI: ${spark.sparkContext.uiWebUrl.getOrElse("unavailable")}")

    loadTables(spark)

    // Warmup
    println("Running warmup...")
    runQuery(spark, "warmup", getQuery5)
    spark.catalog.clearCache()

    // Results setup
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val csvPath = s"$resultsDir/$timestamp-$activeConfig-sf$sf.csv"
    new java.io.File(resultsDir).mkdirs()

    val writer = new PrintWriter(new FileWriter(csvPath, true))
    writer.println("timestamp,query,elapsed_s,executor_memory,active_config")

    // Run queries - just 2 iterations like your current setup
    for (i <- 0 until 2) {
      println(s"\n=== Iteration $i ===")
      val start = System.currentTimeMillis()
      runQuery(spark, s"Q5-iter$i", getQuery5)
      val elapsed = (System.currentTimeMillis() - start) / 1000.0

      writer.println(s"$timestamp,5,$elapsed,$executorMemory,$activeConfig")
      writer.flush()

      spark.catalog.clearCache()
      Thread.sleep(3000)
    }

    writer.close()
    println(s"Results written to $csvPath")
    spark.stop()
  }

  def loadTables(spark: SparkSession): Unit = {
    println("Loading TPC-H tables...")
    val tables = Seq("customer", "lineitem", "nation", "orders",
                     "part", "partsupp", "region", "supplier")
    for (table <- tables) {
      spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(s"$dataPath/$table.tbl")
        .createOrReplaceTempView(table)
      println(s"  Loaded $table")
    }
  }

  def runQuery(spark: SparkSession, label: String, queryFn: SparkSession => Unit): Unit = {
    println(s"Running $label")
    val start = System.currentTimeMillis()
    queryFn(spark)
    val elapsed = (System.currentTimeMillis() - start) / 1000.0
    println(s"$label finished in ${elapsed}s")
  }

  def getQuery5(spark: SparkSession): Unit = {
    spark.sql("""
      SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
      FROM customer
      JOIN orders   ON c_custkey = o_custkey
      JOIN lineitem ON l_orderkey = o_orderkey
      JOIN supplier ON l_suppkey = s_suppkey
      JOIN nation   ON c_nationkey = n_nationkey AND s_nationkey = n_nationkey
      JOIN region   ON n_regionkey = r_regionkey
      WHERE r_name = 'ASIA'
        AND o_orderdate >= '1994-01-01'
        AND o_orderdate < '1995-01-01'
      GROUP BY n_name
      ORDER BY revenue DESC
    """).show(10)
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: scala/collection/MapOps#getOrElse().