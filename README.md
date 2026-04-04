# Scalable Retail Inventory Analytics Using Big Data Technologies

## Project Overview
This project demonstrates scalable retail data analytics using Apache Hadoop and Apache Spark.

The pipeline ingests two distinct data sources, performs multi-source integration, and executes
distributed analytical queries to produce insights on revenue, customer behavior, product performance,
and temporal trends enriched with country-level economic context.

The system is built on a layered Big Data architecture:
 
1. **Storage** — Both source datasets are stored in HDFS
2. **Processing** — Apache Spark cleans, transforms, and joins the data
3. **Querying** — Spark SQL executes distributed analytical queries on the unified dataset
4. **Output** — Results are written to HDFS as Parquet files for efficient downstream access
 
This project is developed as part of the CS 4265 - Big Data Analytics course.

---

## Technologies

| Component | Technology | Version |
|-----------|-----------|---------|
| Distributed Storage | Apache Hadoop HDFS | 3.3.6 |
| Parallel Processing | Apache Spark | 3.5.1 |
| Query Interface | Spark SQL | 3.5.1 |
| Data Formats | CSV (input), Parquet (output) | — |
| Runtime | Scala via spark-shell | 2.12 |

---

## Data Sources
 
### 1. UCI Online Retail Dataset
Retail transaction data covering 541,909 records from a UK-based online retailer
(December 2010 – December 2011).
 
**Download:** https://archive.ics.uci.edu/dataset/352/online+retail
 
Download the file and rename it to `retail.csv` if necessary.
 
### 2. World Bank GDP Dataset
Country-level GDP data (current USD) used to enrich retail analytics with economic context.
 
**Download:** https://data.worldbank.org/indicator/NY.GDP.MKTP.CD
 
Click **Download → CSV**. Extract the zip and locate the file named:
`API_NY.GDP.MKTP.CD_DS2_en_csv_v2_*.csv`
 
> **Note:** Neither dataset is stored in this repository due to file size limits.
> Both must be downloaded and uploaded to HDFS before running the pipeline (see Setup below).

---

## Repository Structure
 
```
├── src/
│   ├── multi_source_retail_analytics.scala   # Main M3 analytics pipeline (multi-source)
│   ├── retail_analysis.scala                 # M2 single-source analytics pipeline
│   └── hdfs_setup.sh                         # HDFS directory creation and data upload script
├── docs/
|   ├── CS4265_Evan_Banks_M1.pdf              # Milestone 1 initial report
│   ├── CS4265_Evan_Banks_M2.pdf              # Milestone 2 progress report
│   ├── CS4265_Evan_Banks_M3.pdf              # Milestone 3 progress report
|   └── Data_Dictionary.md                    # Data dictionary explaining final schema
└── screenshots/
    └── ...                                   # Proof of execution and query outputs
```
 
---

## Environment Setup
 
### Prerequisites
 
Ensure the following are installed and configured in your WSL (Ubuntu) environment:
 
- Java 8 or 11
- Hadoop 3.3.6 (with HDFS configured in pseudo-distributed mode)
- Apache Spark 3.5.1
- SSH service running (`sudo service ssh start`)
 
### Start HDFS
 
```bash
sudo service ssh start
start-dfs.sh
```
 
Verify all services are running:
 
```bash
hdfs dfsadmin -report
```
 
You should see **Live datanodes: 1** in the output.
 
---

## Data Upload to HDFS
 
### Option A: Using the setup script
 
```bash
bash src/hdfs_setup.sh
```
 
### Option B: Manual upload
 
Create the required HDFS directories:
 
```bash
hdfs dfs -mkdir -p /retail
hdfs dfs -mkdir -p /retail/worldbank
```
 
Upload both datasets:
 
```bash
hdfs dfs -put /path/to/retail.csv /retail/retail.csv
hdfs dfs -put /path/to/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_*.csv /retail/worldbank/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv
```
 
Verify both files are in HDFS:
 
```bash
hdfs dfs -ls /retail
hdfs dfs -ls /retail/worldbank
```
 
---

## Running the Pipeline
 
### M3 Pipeline — Multi-Source Analytics (Current)
 
Download the script from the repository:
 
```bash
curl -o ~/multi_source_retail_analytics.scala \
  "https://raw.githubusercontent.com/ebanks28/cs4265-retail-inventory-big-data-project/main/src/multi_source_retail_analytics.scala"
```
 
Run the full end-to-end pipeline:
 
```bash
spark-shell --master local[*] -i ~/multi_source_retail_analytics.scala
```

---

## Pipeline Description
 
The M3 pipeline (`multi_source_retail_analytics.scala`) executes the following stages:
 
### Stage 1 — Data Ingestion
- Reads `retail.csv` from HDFS using Spark's CSV reader
- Reads the World Bank GDP file using `sparkContext.textFile()` with custom header parsing
  to handle the four metadata rows present before the real column header
 
### Stage 2 — Cleaning and Transformation
- Removes null `CustomerID` and `InvoiceNo` values
- Filters cancellation records (InvoiceNo prefixed with `C`)
- Removes rows with non-positive `Quantity` or `UnitPrice`
- Derives `Revenue` (Quantity × UnitPrice)
- Parses `InvoiceDate` (format: `yyyy/MM/dd HH:mm:ss`) to extract `Year` and `Month`
- Selects and casts the 2011 GDP column from the World Bank dataset
 
### Stage 3 — Multi-Source Integration
- Applies a country name mapping table to normalize UCI informal names
  (e.g., `EIRE` → `Ireland`, `USA` → `United States`, `Czech Republic` → `Czechia`)
  to match World Bank formal names
- Joins retail data to GDP data on normalized country name (left join)
- Registers the unified result as a Spark SQL temporary view: `retail_gdp`
- Match rate: 396,807 / 397,884 rows (99.7%) across 34 of 37 countries
 
### Stage 4 — Analytical Queries
Five Spark SQL queries execute on the `retail_gdp` view:
 
| Query | Description |
|-------|-------------|
| 1 | Revenue by country with GDP context |
| 2 | Revenue relative to GDP (market penetration proxy) |
| 3 | Purchasing behavior bucketed by GDP tier |
| 4 | Monthly revenue trend by GDP tier |
| 5 | Source integration diagnostics |
 
### Stage 5 — Output
Results are written to HDFS as Parquet files under `/retail/output/`:
 
```
/retail/output/revenue_by_country_gdp
/retail/output/revenue_per_gdp
/retail/output/gdp_tier_analysis
/retail/output/monthly_trend_by_gdp_tier
```
 
---

## Expected Output
 
As the pipeline runs, you will see `[INFO]` log lines indicating progress through each stage:
 
```
[INFO] ========== Starting Multi-Source Analytics Pipeline ==========
[INFO] Loading UCI Online Retail dataset...
[INFO] Raw retail row count: 541909
[INFO] Clean retail row count: 397884
[INFO] Loading World Bank GDP dataset...
[INFO] GDP records loaded: 217
[INFO] Applying country name mapping...
[INFO] Joining retail data with World Bank GDP data...
[INFO] Rows with GDP match:    396807
[INFO] Rows without GDP match: 1077
[INFO] --- Query 1: Revenue by Country with GDP Context ---
...
[INFO] ========== Multi-Source Analytics Pipeline Complete ==========
```
 
---

## Current Status (Milestone 3)
 
### What Works
- HDFS single-node pseudo-distributed cluster fully operational
- Two distinct data sources ingested and stored in HDFS
- Full cleaning and transformation pipeline including date parsing and revenue derivation
- Country name normalization resolving mismatches between UCI and World Bank naming conventions
- Multi-source join with 99.7% row match rate
- Five Spark SQL analytical queries executing successfully
- All outputs written to HDFS as Parquet files
 
### Known Limitations
- Single-node deployment is memory-bound at approximately 100 million rows
  (demonstrated in M2 scalability experiments)
- Channel Islands, European Community, and Unspecified are unmappable to World Bank
  country entries and are excluded from GDP-dependent queries
- Pipeline is batch-oriented; real-time streaming ingestion is outside project scope
 
### What's Next (Milestone 4)
- Final validation and portfolio-ready documentation
- Data dictionary for the `retail_gdp` schema
- Complete bottleneck analysis and scalability conclusions
