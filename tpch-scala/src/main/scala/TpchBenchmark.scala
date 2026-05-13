// ~/Documents/Repositories/RPi-Cluster-Spark/tpch-scala/src/main/scala/TpchBenchmark.scala
package tpch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.{FileWriter, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

object TpchBenchmark {

    // TPC-H Schemas
    val customerSchema = StructType(Array(
        StructField("c_custkey", IntegerType),
        StructField("c_name", StringType),
        StructField("c_address", StringType),
        StructField("c_nationkey", IntegerType),
        StructField("c_phone", StringType),
        StructField("c_acctbal", DecimalType(15,2)),
        StructField("c_mktsegment", StringType),
        StructField("c_comment", StringType)
    ))

    val lineitemSchema = StructType(Array(
        StructField("l_orderkey", IntegerType),
        StructField("l_partkey", IntegerType),
        StructField("l_suppkey", IntegerType),
        StructField("l_linenumber", IntegerType),
        StructField("l_quantity", DecimalType(15,2)),
        StructField("l_extendedprice", DecimalType(15,2)),
        StructField("l_discount", DecimalType(15,2)),
        StructField("l_tax", DecimalType(15,2)),
        StructField("l_returnflag", StringType),
        StructField("l_linestatus", StringType),
        StructField("l_shipdate", DateType),
        StructField("l_commitdate", DateType),
        StructField("l_receiptdate", DateType),
        StructField("l_shipinstruct", StringType),
        StructField("l_shipmode", StringType),
        StructField("l_comment", StringType)
    ))

    val nationSchema = StructType(Array(
        StructField("n_nationkey", IntegerType),
        StructField("n_name", StringType),
        StructField("n_regionkey", IntegerType),
        StructField("n_comment", StringType)
    ))

    val ordersSchema = StructType(Array(
        StructField("o_orderkey", IntegerType),
        StructField("o_custkey", IntegerType),
        StructField("o_orderstatus", StringType),
        StructField("o_totalprice", DecimalType(15,2)),
        StructField("o_orderdate", DateType),
        StructField("o_orderpriority", StringType),
        StructField("o_clerk", StringType),
        StructField("o_shippriority", IntegerType),
        StructField("o_comment", StringType)
    ))

    val partSchema = StructType(Array(
        StructField("p_partkey", IntegerType),
        StructField("p_name", StringType),
        StructField("p_mfgr", StringType),
        StructField("p_brand", StringType),
        StructField("p_type", StringType),
        StructField("p_size", IntegerType),
        StructField("p_container", StringType),
        StructField("p_retailprice", DecimalType(15,2)),
        StructField("p_comment", StringType)
    ))

    val partsuppSchema = StructType(Array(
        StructField("ps_partkey", IntegerType),
        StructField("ps_suppkey", IntegerType),
        StructField("ps_availqty", IntegerType),
        StructField("ps_supplycost", DecimalType(15,2)),
        StructField("ps_comment", StringType)
    ))

    val regionSchema = StructType(Array(
        StructField("r_regionkey", IntegerType),
        StructField("r_name", StringType),
        StructField("r_comment", StringType)
    ))

    val supplierSchema = StructType(Array(
        StructField("s_suppkey", IntegerType),
        StructField("s_name", StringType),
        StructField("s_address", StringType),
        StructField("s_nationkey", IntegerType),
        StructField("s_phone", StringType),
        StructField("s_acctbal", DecimalType(15,2)),
        StructField("s_comment", StringType)
    ))

    // Config values
    val executorMemory   = sys.env.getOrElse("EXECUTOR_MEMORY", "768m")
    val masterUrl        = sys.env.getOrElse("SPARK_MASTER_URL", "spark://192.168.50.65:7077")
    // val dataPath         = sys.env.getOrElse("DATA_PATH", 
    //     "/home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf0.3")
    val dataPath         = sys.env.getOrElse("DATA_PATH", 
        "/mnt/tpch/sf1")
    val resultsDir       = sys.env.getOrElse("RESULTS_DIR",
        "/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/scala")
    val activeConfig     = sys.env.getOrElse("ACTIVE_CONFIG", "default")
    val sf               = sys.env.getOrElse("SF", "0.3")

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
        .appName("TPC-H Benchmark Scala")
        .master(masterUrl)
        .config("spark.executor.memory", executorMemory)
        .config("spark.executor.cores", "4")
        .config("spark.scheduler.minRegisteredResourcesRatio", "1.0")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.memory.fraction", "0.45")
        .config("spark.memory.storageFraction", "0.5")
        .config("spark.task.maxFailures", "1") 
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/event-logs")
        .config("spark.eventLog.compress", "false")
        .getOrCreate()

        println(s"Spark UI: ${spark.sparkContext.uiWebUrl.getOrElse("unavailable")}")

        loadTables(spark)

        // Warmup
        println("Running warmup...")
        runQuery(spark, "warmup", getQuery6)
        spark.catalog.clearCache()
        spark.sparkContext.getPersistentRDDs.foreach { case (_, rdd) => rdd.unpersist() }
        System.gc()
        Thread.sleep(5000)  

        // Results setup
        val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
        val csvPath = s"$resultsDir/$timestamp-$activeConfig-sf$sf.csv"
        new java.io.File(resultsDir).mkdirs()

        val writer = new PrintWriter(new FileWriter(csvPath, true))
        writer.println("timestamp,query,elapsed_s,executor_memory,active_config")

        // Map query number -> function
        val queries: Map[Int, SparkSession => Unit] = Map(
            1 -> getQuery1,
            3 -> getQuery3,
            5 -> getQuery5,
            6 -> getQuery6
        )

        // Run queries for x iterations
        for (i <- 0 until 3) {
            println(s"\n=== Iteration $i ===")

            // Shuffle queries each iteration
            val shuffledQueries = Random.shuffle(queries.toSeq)

            for ((qNum, qFunc) <- shuffledQueries) {
                println(s"Running Q$qNum (iteration $i)")

                val start = System.currentTimeMillis()
                runQuery(spark, s"Q${qNum}-iter$i", qFunc)
                val elapsed = (System.currentTimeMillis() - start) / 1000.0

                writer.println(s"$timestamp,$qNum,$elapsed,$executorMemory,$activeConfig")
                writer.flush()

                spark.catalog.clearCache()
                System.gc()
                Thread.sleep(3000)
            }
        }

        writer.close()
        println(s"Results written to $csvPath")
        spark.stop()
    }

    def loadTables(spark: SparkSession): Unit = {
        println("Loading TPC-H tables...")

        val tableSchemas = Map(
        "customer" -> customerSchema,
        "lineitem" -> lineitemSchema,
        "nation"   -> nationSchema,
        "orders"   -> ordersSchema,
        "part"     -> partSchema,
        "partsupp" -> partsuppSchema,
        "region"   -> regionSchema,
        "supplier" -> supplierSchema
        )

        for ((table, schema) <- tableSchemas) {
        spark.read
            .option("delimiter", "|")
            .option("header", "false")
            .schema(schema)
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

    def getQuery1(spark: SparkSession): Unit = {
        spark.sql("""SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) as sum_qty,
            SUM(l_extendedprice) as sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            AVG(l_quantity) as avg_qty,
            AVG(l_extendedprice) as avg_price,
            AVG(l_discount) as avg_disc,
            COUNT(*) as count_order
        FROM lineitem
        WHERE l_shipdate <= date '1998-12-01' - interval '90' day
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
        """).show(10)
    }

    def getQuery3(spark: SparkSession): Unit = {
        spark.sql("""
        SELECT
            l_orderkey,
            SUM(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING'
            AND c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND o_orderdate < date '1995-03-15'
            AND l_shipdate > date '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
        """).show(10)
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

    def getQuery6(spark: SparkSession): Unit = {
        def df = spark.sql("""
        SELECT
            SUM(l_extendedprice * l_discount) as revenue
        FROM lineitem
        WHERE l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07
            AND l_quantity < 24
        """)
        df.explain("formatted")

        df.show(10)

    }
}