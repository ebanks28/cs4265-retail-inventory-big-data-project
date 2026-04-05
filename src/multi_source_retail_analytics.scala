/**
 * multi_source_analytics.scala
 * --------------------------
 * CS 4265 - Big Data Analytics
 * Milestone 3: Complete Implementation
 *
 * Ingests two distinct data sources from HDFS:
 *   1. UCI Online Retail Dataset       -> /retail/retail.csv
 *   2. World Bank GDP Data (2011)      -> /retail/worldbank/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv
 *
 * Joins on Country and runs combined analytical queries.
 *
 * Download on WSL with:
 *   curl -o ~/multi_source_retail_analytics.scala "https://raw.githubusercontent.com/ebanks28/cs4265-retail-inventory-big-data-project/main/src/multi_source_retail_analytics.scala"
 * Verify with:
 *   cat ~/multi_source_retail_analytics.scala | head -20 (Should show the first 20 lines of this file)
 *
 * Run with:
 *   spark-shell --master local[*] -i multi_source_retail_analytics.scala
 * Or:
 *   spark-submit --class MultiSourceAnalytics multi_source_analytics.jar
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
val RETAIL_PATH = "hdfs://localhost:9000/retail/retail.csv"
val GDP_PATH    = "hdfs://localhost:9000/retail/worldbank/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv"
val OUTPUT_BASE = "hdfs://localhost:9000/retail/output"

// ---------------------------------------------------------------------------
// SparkSession
// ---------------------------------------------------------------------------
val spark = SparkSession.builder().appName("RetailInventoryAnalytics_M3_MultiSource").config("spark.sql.shuffle.partitions", "8").getOrCreate()

import spark.implicits._

println("[INFO] ========== Starting Multi-Source Analytics Pipeline ==========")

// ===========================================================================
// SECTION 1: Load & Clean UCI Retail Data
// ===========================================================================
println("[INFO] Loading UCI Online Retail dataset...")

val retailRaw = spark.read.option("header", "true").option("inferSchema", "true").csv(RETAIL_PATH)

println(s"[INFO] Raw retail row count: ${retailRaw.count()}")

val retailClean = retailRaw.filter(col("CustomerID").isNotNull).filter(col("InvoiceNo").isNotNull).filter(!col("InvoiceNo").startsWith("C")).filter(col("Quantity") > 0).filter(col("UnitPrice") > 0).withColumn("UnitPrice", col("UnitPrice").cast(DoubleType)).withColumn("Revenue", col("Quantity") * col("UnitPrice")).withColumn("InvoiceDateParsed", coalesce(to_timestamp(col("InvoiceDate"), "yyyy/MM/dd HH:mm:ss"), to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm"))).withColumn("Year",  year(col("InvoiceDateParsed"))).withColumn("Month", month(col("InvoiceDateParsed")))

println(s"[INFO] Clean retail row count: ${retailClean.count()}")
retailClean.printSchema()

// ===========================================================================
// SECTION 2: Load & Clean World Bank GDP Data
// ===========================================================================
println("[INFO] Loading World Bank GDP dataset...")

// The World Bank CSV has 4 metadata rows at the top before the real header.
// Skip them by reading with header=true and then selecting only what we need.
// The file is wide-format: each year is its own column.
// We only need "Country Name" and the year 2011 (most of the UCI data is 2010-2011).
val gdpRaw = spark.read.option("header", "true").option("inferSchema", "false").csv(GDP_PATH)

// Preview the column names to confirm structure
println("[INFO] World Bank raw columns:")
gdpRaw.columns.take(10).foreach(println)

val gdpClean = {
  val rawLines = spark.sparkContext.textFile(GDP_PATH)
  val headerLine = rawLines.filter(_.contains("Country Name")).first()
  val headers = headerLine.split(",").map(_.replaceAll("\"", "").trim)

  val dataLines = rawLines
    .zipWithIndex()
    .filter { case (line: String, idx: Long) => idx >= 5 }
    .map { case (line: String, idx: Long) => line }

  val rowRDD = dataLines.map(line => {
    val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      .map(_.replaceAll("\"", "").trim)
    org.apache.spark.sql.Row(cols: _*)
  })

  val schema = org.apache.spark.sql.types.StructType(
    headers.map(h => org.apache.spark.sql.types.StructField(h, StringType, nullable = true))
  )

  val gdpWithHeader = spark.createDataFrame(rowRDD, schema)
  val headerRow = gdpWithHeader.first()

  gdpWithHeader
    .filter(row => row != headerRow)
    .toDF(headers: _*)
    .select(
      col("Country Name").alias("CountryName"),
      col("2011").alias("GDP_2011_Raw")
    )
    .filter(col("GDP_2011_Raw").isNotNull)
    .withColumn("GDP_2011", col("GDP_2011_Raw").cast(DoubleType))
    .filter(col("GDP_2011").isNotNull)
    .drop("GDP_2011_Raw")
}

println(s"[INFO] GDP records loaded: ${gdpClean.count()}")
gdpClean.show(10)

// ===========================================================================
// SECTION 3: Country Name Mapping
// ===========================================================================
// The UCI dataset uses short country names that don't always match World Bank names.
// This mapping handles the most common mismatches in the UCI dataset.

println("[INFO] Applying country name mapping...")

val countryMapping = Seq(
  ("United Kingdom",            "United Kingdom"),
  ("Germany",                   "Germany"),
  ("France",                    "France"),
  ("EIRE",                      "Ireland"),
  ("Spain",                     "Spain"),
  ("Netherlands",               "Netherlands"),
  ("Belgium",                   "Belgium"),
  ("Switzerland",               "Switzerland"),
  ("Portugal",                  "Portugal"),
  ("Australia",                 "Australia"),
  ("Norway",                    "Norway"),
  ("Italy",                     "Italy"),
  ("Cyprus",                    "Cyprus"),
  ("Sweden",                    "Sweden"),
  ("Finland",                   "Finland"),
  ("Austria",                   "Austria"),
  ("Denmark",                   "Denmark"),
  ("Japan",                     "Japan"),
  ("Poland",                    "Poland"),
  ("Israel",                    "Israel"),
  ("USA",                       "United States"),
  ("Canada",                    "Canada"),
  ("Singapore",                 "Singapore"),
  ("Bahrain",                   "Bahrain"),
  ("Czech Republic",            "Czechia"),
  ("Greece",                    "Greece"),
  ("Iceland",                   "Iceland"),
  ("Malta",                     "Malta"),
  ("Hong Kong",                 "Hong Kong SAR, China"),
  ("United Arab Emirates",      "United Arab Emirates"),
  ("Saudi Arabia",              "Saudi Arabia"),
  ("Brazil",                    "Brazil"),
  ("Lithuania",                 "Lithuania"),
  ("RSA",                       "South Africa"),
  ("Lebanon",                   "Lebanon"),
  ("Channel Islands",           null),   // no direct mapping — will be excluded from join
  ("European Community",        null),   // no mapping
  ("Unspecified",               null)    // no mapping
).toDF("UCI_Country", "WB_Country")

// Join the mapping onto the retail data to produce a normalized country name
val retailMapped = retailClean.join(countryMapping, retailClean("Country") === countryMapping("UCI_Country"), "left").drop("UCI_Country")

// ===========================================================================
// SECTION 4: Join Retail Data with GDP Data
// ===========================================================================
println("[INFO] Joining retail data with World Bank GDP data...")

val retailWithGDP = retailMapped.join(gdpClean, retailMapped("WB_Country") === gdpClean("CountryName"), "left").drop("CountryName")

val matchedCount   = retailWithGDP.filter(col("GDP_2011").isNotNull).count()
val unmatchedCount = retailWithGDP.filter(col("GDP_2011").isNull).count()
println(s"[INFO] Rows with GDP match:    $matchedCount")
println(s"[INFO] Rows without GDP match: $unmatchedCount")

// Register as a temp view for Spark SQL queries
retailWithGDP.createOrReplaceTempView("retail_gdp")

// ===========================================================================
// SECTION 5: Multi-Source Analytical Queries
// ===========================================================================

// ---------------------------------------------------------------------------
// Query 1: Revenue by Country with GDP Context
// Answers: How does sales revenue relate to a country's economic size?
// ---------------------------------------------------------------------------
println("[INFO] --- Query 1: Revenue by Country with GDP Context ---")

val revenueByCountryGDP = spark.sql("""
  SELECT
    Country,
    WB_Country,
    ROUND(GDP_2011 / 1e9, 2)          AS GDP_Billions_USD,
    ROUND(SUM(Revenue), 2)            AS TotalRevenue_GBP,
    COUNT(DISTINCT InvoiceNo)         AS NumInvoices,
    COUNT(DISTINCT CustomerID)        AS NumCustomers,
    ROUND(SUM(Revenue)
          / COUNT(DISTINCT CustomerID), 2) AS RevenuePerCustomer
  FROM retail_gdp
  WHERE GDP_2011 IS NOT NULL
  GROUP BY Country, WB_Country, GDP_2011
  ORDER BY TotalRevenue_GBP DESC
""")

println("Revenue by Country with GDP Context:")
revenueByCountryGDP.show(20, truncate = false)
revenueByCountryGDP.write.mode("overwrite").parquet(s"$OUTPUT_BASE/revenue_by_country_gdp")

// ---------------------------------------------------------------------------
// Query 2: Revenue Per Capita Proxy
// Normalizes revenue by GDP to identify over- or under-performing markets
// relative to economic size.
// ---------------------------------------------------------------------------
println("[INFO] --- Query 2: Revenue Relative to GDP (Market Penetration Proxy) ---")

val revenuePerGDP = spark.sql("""
  SELECT
    Country,
    ROUND(GDP_2011 / 1e9, 2)                        AS GDP_Billions_USD,
    ROUND(SUM(Revenue), 2)                           AS TotalRevenue_GBP,
    ROUND(SUM(Revenue) / (GDP_2011 / 1e9), 4)        AS RevenuePerBillionGDP
  FROM retail_gdp
  WHERE GDP_2011 IS NOT NULL
  GROUP BY Country, GDP_2011
  ORDER BY RevenuePerBillionGDP DESC
""")

println("Revenue relative to GDP (top 15):")
revenuePerGDP.show(15, truncate = false)
revenuePerGDP.write.mode("overwrite").parquet(s"$OUTPUT_BASE/revenue_per_gdp")

// ---------------------------------------------------------------------------
// Query 3: GDP Tier Analysis
// Buckets countries into High / Medium / Low GDP tiers and compares
// purchasing behavior across tiers.
// ---------------------------------------------------------------------------
println("[INFO] --- Query 3: Purchasing Behavior by GDP Tier ---")

val gdpTierAnalysis = spark.sql("""
  SELECT
    CASE
      WHEN GDP_2011 >= 1e12 THEN 'High GDP (>1T USD)'
      WHEN GDP_2011 >= 1e11 THEN 'Medium GDP (100B-1T USD)'
      ELSE                       'Low GDP (<100B USD)'
    END                                          AS GDP_Tier,
    COUNT(DISTINCT Country)                      AS NumCountries,
    ROUND(SUM(Revenue), 2)                       AS TotalRevenue_GBP,
    COUNT(DISTINCT CustomerID)                   AS TotalCustomers,
    ROUND(AVG(Revenue), 4)                       AS AvgTransactionValue,
    ROUND(SUM(Revenue)
          / COUNT(DISTINCT CustomerID), 2)       AS AvgRevenuePerCustomer
  FROM retail_gdp
  WHERE GDP_2011 IS NOT NULL
  GROUP BY GDP_Tier
  ORDER BY TotalRevenue_GBP DESC
""")

println("Purchasing behavior by GDP tier:")
gdpTierAnalysis.show(truncate = false)
gdpTierAnalysis.write.mode("overwrite").parquet(s"$OUTPUT_BASE/gdp_tier_analysis")

// ---------------------------------------------------------------------------
// Query 4: Monthly Trend by GDP Tier
// Shows how purchasing volume evolves month-over-month across economic tiers.
// ---------------------------------------------------------------------------
println("[INFO] --- Query 4: Monthly Revenue Trend by GDP Tier ---")

val monthlyByTier = spark.sql("""
  SELECT
    Year,
    Month,
    CASE
      WHEN GDP_2011 >= 1e12 THEN 'High GDP'
      WHEN GDP_2011 >= 1e11 THEN 'Medium GDP'
      ELSE                       'Low GDP'
    END                                   AS GDP_Tier,
    ROUND(SUM(Revenue), 2)                AS MonthlyRevenue,
    COUNT(DISTINCT InvoiceNo)             AS MonthlyInvoices
  FROM retail_gdp
  WHERE GDP_2011 IS NOT NULL
    AND Year IS NOT NULL
  GROUP BY Year, Month, GDP_Tier
  ORDER BY Year, Month, GDP_Tier
""")

println("Monthly revenue trend by GDP tier:")
monthlyByTier.show(36, truncate = false)
monthlyByTier.write.mode("overwrite").parquet(s"$OUTPUT_BASE/monthly_trend_by_gdp_tier")

// ---------------------------------------------------------------------------
// Query 5: Single-Source vs Multi-Source Row Reconciliation
// Diagnostic query — useful to include in your M3 report to show
// that both sources were genuinely integrated.
// ---------------------------------------------------------------------------
println("[INFO] --- Query 5: Source Integration Diagnostics ---")

val diagnostics = spark.sql("""
  SELECT
    COUNT(*)                                    AS TotalRetailRows,
    COUNT(DISTINCT Country)                     AS UniqueCountriesInRetail,
    SUM(CASE WHEN GDP_2011 IS NOT NULL
             THEN 1 ELSE 0 END)                 AS RowsMatchedToGDP,
    COUNT(DISTINCT
      CASE WHEN GDP_2011 IS NOT NULL
           THEN Country END)                    AS CountriesMatchedToGDP,
    COUNT(DISTINCT
      CASE WHEN GDP_2011 IS NULL
           THEN Country END)                    AS CountriesUnmatched
  FROM retail_gdp
""")

println("Source integration diagnostics:")
diagnostics.show(truncate = false)

println("[INFO] ========== Multi-Source Analytics Pipeline Complete ==========")

spark.stop()
