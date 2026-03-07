import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val retail = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/retail/retail.csv")
retail.printSchema()
retail.count()

val cleanRetail = retail.filter(!col("InvoiceNo").startsWith("C"))
cleanRetail.count()

val scaled10 = cleanRetail.crossJoin(spark.range(10).toDF("replication_id"))
scaled10.count()

val result = scaled10.withColumn("Revenue", col("Quantity") * col("UnitPrice")).groupBy("StockCode").agg(sum("Revenue").alias("TotalRevenue")).orderBy(desc("TotalRevenue"))
result.show()

spark.stop()
