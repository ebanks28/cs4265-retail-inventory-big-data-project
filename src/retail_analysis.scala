import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Move dataset into Spark
val retail = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/retail/retail.csv")
retail.printSchema()
retail.count()

// Clean data to remove returns (negative quantities), zero prices, and null values
val cleanRetail = retail.filter(col("Quantity") > 0).filter(col("UnitPrice") > 0).na.drop()
cleanRetail.count()

// Scale dataset
val scaled10 = cleanRetail.crossJoin(spark.range(10).toDF("replication_id"))
scaled10.cache()
scaled10.count()

// Measure query execution time
val start = system.nanoTime()

// Perform query
val result = scaled10.withColumn("Revenue", col("Quantity") * col("UnitPrice")).groupBy("StockCode").agg(sum("Revenue").alias("TotalRevenue")).orderBy(desc("TotalRevenue"))
result.show()

val end = system.nanoTime()

val runtimeSeconds = (endTime - startTime) / 1e9
println(s"Query runtime: $runtimeSeconds seconds")

spark.stop()
