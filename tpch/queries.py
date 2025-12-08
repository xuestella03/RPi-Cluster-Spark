# TPC-H queries for PySpark
# Queries 1, 3, 5, and 6
# We assume that we've loaded data at this point

def get_query_1(spark):
    lineitem_df = spark.table("lineitem")
    query = """
        SELECT
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
    """
    lineitem_df.createOrReplaceTempView("lineitem")
    return spark.sql(query)

def get_query_3(spark):
    customer_df = spark.table("customer")
    orders_df = spark.table("orders")
    lineitem_df = spark.table("lineitem")
    
    query = """
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
    """
    customer_df.createOrReplaceTempView("customer")
    orders_df.createOrReplaceTempView("orders")
    lineitem_df.createOrReplaceTempView("lineitem")
    return spark.sql(query)

def get_query_5(spark):
    customer_df = spark.table("customer")
    orders_df = spark.table("orders")
    lineitem_df = spark.table("lineitem")
    supplier_df = spark.table("supplier")
    nation_df = spark.table("nation")
    region_df = spark.table("region")
    
    query = """
        SELECT
            n_name,
            SUM(l_extendedprice * (1 - l_discount)) as revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey
            AND l_orderkey = o_orderkey
            AND l_suppkey = s_suppkey
            AND c_nationkey = s_nationkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'ASIA'
            AND o_orderdate >= date '1994-01-01'
            AND o_orderdate < date '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC
    """
    customer_df.createOrReplaceTempView("customer")
    orders_df.createOrReplaceTempView("orders")
    lineitem_df.createOrReplaceTempView("lineitem")
    supplier_df.createOrReplaceTempView("supplier")
    nation_df.createOrReplaceTempView("nation")
    region_df.createOrReplaceTempView("region")
    return spark.sql(query)

def get_query_6(spark): 
    lineitem_df = spark.table("lineitem")
    
    query = """
        SELECT
            SUM(l_extendedprice * l_discount) as revenue
        FROM lineitem
        WHERE l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1995-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07
            AND l_quantity < 24
    """
    lineitem_df.createOrReplaceTempView("lineitem")
    return spark.sql(query)

QUERIES = {
    1: get_query_1,
    3: get_query_3,
    5: get_query_5,
    6: get_query_6
}