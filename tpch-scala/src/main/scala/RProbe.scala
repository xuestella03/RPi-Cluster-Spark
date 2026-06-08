// ~/Documents/Repositories/RPi-Cluster-Spark/tpch-scala/src/main/scala/RProbe.scala
//
// Diagnostic harness for measuring the per-operator-type slowdown ratio r = t_slow / t_fast.
// Each invocation runs ONE probe query (selected by the PROBE env var) that isolates a single
// operator class, several times (PROBE_ITERS, default 3) so the parser can read a warmed stage.
//
// It reuses TpchBenchmark.loadTables / schemas / DATA_PATH so the scan, parsing, and table setup
// are byte-for-byte identical to the real TPC-H benchmark — only the query body differs.
//
// Run via find-r.yml, which sets PROBE, PROBE_ITERS, DATA_PATH and all the --conf flags.
// Build with `sbt package` (produces the same jar as TpchBenchmark; launch with --class tpch.RProbe).

package tpch

import org.apache.spark.sql.SparkSession

object RProbe {

  // Each probe is written to force exactly one operator class onto the heavy stage:
  //   scan  -> low-cardinality GROUP BY: the work is the lineitem file scan (map stage, no shuffle read)
  //   agg   -> high-cardinality GROUP BY: real hash-aggregate work on the reduce side
  //   sort  -> global ORDER BY: range-partitioned reduce that sorts within partition
  //   join  -> sort-merge join (broadcast disabled): reduce reads both shuffles and merges
  private val queries: Map[String, String] = Map(
    "scan" ->
      """SELECT l_returnflag, COUNT(*)
         FROM lineitem
         WHERE l_shipdate <= DATE '1998-09-02'
         GROUP BY l_returnflag""",
    "agg" ->
      """SELECT l_partkey, SUM(l_quantity)
         FROM lineitem
         GROUP BY l_partkey""",
    "sort" ->
      """SELECT l_orderkey, l_shipdate
         FROM lineitem
         ORDER BY l_shipdate""",
    "join" ->
      """SELECT COUNT(*)
         FROM lineitem l
         JOIN orders o ON l.l_orderkey = o.o_orderkey"""
  )

  def main(args: Array[String]): Unit = {
    val probe = sys.env.getOrElse("PROBE", "scan").trim.toLowerCase
    val iters = sys.env.getOrElse("PROBE_ITERS", "3").toInt

    val sql = queries.getOrElse(
      probe,
      sys.error(s"Unknown PROBE='$probe'. Use one of: ${queries.keys.mkString(", ")}"))

    // master, memory, weights, eventLog.dir, shuffle.partitions, maxPartitionBytes, GC, etc.
    // all arrive from spark-submit --conf in find-r.yml.
    val spark = SparkSession.builder()
      .appName(s"R-Probe [$probe]")
      .getOrCreate()

    // Keep the plan fixed so the measured stage matches what you deploy:
    //  - AQE off  => spark.sql.shuffle.partitions is honored, no coalescing of the reduce.
    //  - for join, disable broadcast so it becomes a sort-merge join (the JoinReduce class),
    //    not a broadcast hash join.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    if (probe == "join") {
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    }

    // Identical table setup to the real benchmark.
    TpchBenchmark.loadTables(spark)

    println(s"[R-Probe] probe=$probe iters=$iters dataPath=${TpchBenchmark.dataPath}")
    for (i <- 0 until iters) {
      val start = System.currentTimeMillis()
      spark.sql(sql).write.format("noop").mode("overwrite").save()
      val secs = (System.currentTimeMillis() - start) / 1000.0
      println(s"[R-Probe] $probe iter=$i elapsed=${secs}s")
      spark.catalog.clearCache()
      System.gc()
      Thread.sleep(2000)
    }

    spark.stop()
  }
}