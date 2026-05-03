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
Retail transaction data covering approximately 541,909 records from a UK-based online
retailer (December 2010 – December 2011).

**Download:** https://archive.ics.uci.edu/dataset/352/online+retail

The dataset downloads as a `.zip` file. After unzipping, open the Excel file and export
it as CSV by going to **File → Save As** and selecting
**CSV UTF-8 (Comma delimited) (*.csv)** from the file type dropdown. Save the file as
`retail.csv`.

> **Note:** The date format in `retail.csv` may vary depending on how the file is
> exported from Excel. The pipeline handles both `yyyy/MM/dd HH:mm:ss` and
> `MM/dd/yyyy HH:mm` automatically.
 
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
cs4265-retail-inventory-big-data-project/
├── README.md                                    # Comprehensive project documentation
├── LICENSE                                      # Open source license
├── .gitattributes                               # Uses UNIX line endings when cloned regardless of OS
├── .gitignore                                   # Excludes data, credentials, artifacts
├── config/
│   └── settings.yaml                            # Pipeline configuration parameters
├── docs/
|   ├── CS4265_Evan_Banks_M1.pdf                 # Milestone 1 initial report
│   ├── CS4265_Evan_Banks_M2.pdf                 # Milestone 2 progress report
│   ├── CS4265_Evan_Banks_M3.pdf                 # Milestone 3 progress report
|   ├── CS4265_Evan_Banks_M4.pdf                 # Milestone 4 final report
|   ├── Data_Dictionary.md                       # Schema documentation for the retail_gdp unified dataset
|   └── validation.md                            # Data validation report for the M4 pipeline
├── src/
|   ├── multi_source_retail_analytics.scala      # M4 refined multi-source analytics pipeline
│   ├── multi_source_retail_analytics_old.scala  # M3 multi-source analytics pipeline
│   ├── retail_analysis.scala                    # M2 single-source analytics pipeline
│   └── hdfs_setup.sh                            # HDFS directory creation and data upload script
└── screenshots/
    ├── Data_Acquisition_Screenshot.png          # Evidence of retail.csv being loaded from HDFS into Spark (M2)
    ├── Data_Storage_Screenshot.png              # Evidence of retail.csv being stored in HDFS (M2)
    ├── Query_1_Screenshot.png                   # Sample output of revenue by country with GDP context query (M3)
    ├── Query_2_Screenshot.png                   # Sample output of revenue relative to GDP query (M3)
    ├── Query_3_Screenshot.png                   # Output of purchasing behavior by GDP tier query (M3)
    ├── Query_4_Screenshot.png                   # Output of monthly revenue trend by GDP tier query (M3)
    ├── Query_5_Screenshot.png                   # Output of source integration diagnostics query (M3)
    ├── Query_Results_Screenshot.png             # Output of total revenue per product query on retail.csv (M2)
    ├── stack_architecture_final.png             # Final big data technology stack (M4)
    ├── data_flow_diagram.pdf.png                # Proposed data flow pipeline (M1)
    └── stack_architecture_diagram.pdf           # Proposed big data technology stack (M1)
```

---

## Environment Setup

## Prerequisites

Ensure the following are installed in your WSL (Ubuntu) environment. If any are missing,
install them using the commands below.

**Java 11**
```bash
sudo apt update
sudo apt install openjdk-11-jdk -y
java -version
```

**Hadoop 3.3.6**
```bash
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
```

**Apache Spark 3.5.1**
```bash
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
```

**SSH**
```bash
sudo apt install openssh-server -y
sudo service ssh start
```

> **Note:** A `.gitattributes` file is included in the repository to ensure Unix line
> endings on all scripts. If you encounter `\r` related errors when running any `.sh`
> or `.scala` file, fix them with:
> ```bash
> sed -i 's/\r//' path/to/file
> ```
 
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

Place both downloaded data files in the root of the cloned repository before running
the setup script:
```
cs4265-retail-inventory-big-data-project/
├── retail.csv
├── API_NY.GDP.MKTP.CD_DS2_en_csv_v2_*.csv
├── src/
├── docs/
└── ...
```

Then run the setup script from the repo root:
```bash
bash src/hdfs_setup.sh
```

This will start HDFS, create the required directories, and upload both files. If you
prefer to upload manually:
```bash
hdfs dfs -mkdir -p /retail
hdfs dfs -mkdir -p /retail/worldbank
hdfs dfs -put retail.csv /retail/retail.csv
hdfs dfs -put API_NY.GDP.MKTP.CD_DS2_en_csv_v2_*.csv /retail/worldbank/
```
 
---

## Running the Pipeline

### M4 Pipeline — Multi-Source Analytics with Error Handling (Current)

Download the script from the repository:

```bash
curl -o ~/multi_source_retail_analytics.scala \
  "https://raw.githubusercontent.com/ebanks28/cs4265-retail-inventory-big-data-project/main/src/multi_source_retail_analytics.scala"
```

> **Alternative to curl:** If your WSL environment cannot reach the internet, clone
> the repository on Windows via GitHub Desktop and copy the script into WSL:
> ```bash
> cp /mnt/c/path/to/repo/src/multi_source_retail_analytics.scala ~/
> ```

Fix line endings if needed, then run:

```bash
sed -i 's/\r//' ~/multi_source_retail_analytics.scala
spark-shell --master local[*] -i ~/multi_source_retail_analytics.scala
```

### Previous Pipeline Versions (Reference Only)
- `src/multi_source_retail_analytics_old.scala` — M3 multi-source pipeline
- `src/retail_analysis.scala` — M2 single-source pipeline

## Pipeline Description
 
The M4 pipeline (`multi_source_retail_analytics.scala`) executes the following stages:
 
### Stage 1 — Data Ingestion
- Reads `retail.csv` from HDFS using Spark's CSV reader
- Reads the World Bank GDP file using `sparkContext.textFile()` with custom header parsing
  to handle the four metadata rows present before the real column header
 
### Stage 2 — Cleaning and Transformation
- Removes null `CustomerID` and `InvoiceNo` values
- Filters cancellation records (InvoiceNo prefixed with `C`)
- Removes rows with non-positive `Quantity` or `UnitPrice`
- Derives `Revenue` (Quantity × UnitPrice)
- Parses `InvoiceDate` using `coalesce()` to handle two format variants
  (`yyyy/MM/dd HH:mm:ss` and `MM/dd/yyyy HH:mm`) to extract `Year` and `Month`
- Selects and casts the 2011 GDP column from the World Bank dataset
 
### Stage 3 — Multi-Source Integration
- Applies a country name mapping table to normalize UCI informal names
  (e.g., `EIRE` → `Ireland`, `USA` → `United States`, `Czech Republic` → `Czechia`)
  to match World Bank formal names
- Joins retail data to GDP data on normalized country name (left join)
- Registers the unified result as a Spark SQL temporary view: `retail_gdp`
- Match rate: 396,832 / 397,884 rows (99.7%) across 34 of 37 countries
 
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
[INFO] GDP records loaded: 261
[INFO] Applying country name mapping...
[INFO] Joining retail data with World Bank GDP data...
[INFO] Rows with GDP match:    396832
[INFO] Rows without GDP match: 1052
[INFO] --- Query 1: Revenue by Country with GDP Context ---
...
[INFO] ========== Multi-Source Analytics Pipeline Complete ==========
```
 
---

## Current Status (Milestone 4 — Complete)

### What Works
- HDFS single-node pseudo-distributed cluster fully operational
- Two distinct data sources ingested and stored in HDFS
- Full cleaning and transformation pipeline including dual-format date parsing and revenue derivation
- Country name normalization resolving mismatches between UCI and World Bank naming conventions
- Multi-source join with 99.7% row match rate (396,832 / 397,884 rows)
- Five Spark SQL analytical queries executing successfully
- All outputs written to HDFS as Parquet files
- Runtime error handling: retry logic, graceful GDP fallback, HDFS file existence checks
- Fresh clone test completed successfully
- Data dictionary, validation report, and architecture documentation complete

### Known Limitations
- Single-node deployment is memory-bound at approximately 100 million rows
  (demonstrated in M2 scalability experiments)
- Channel Islands, European Community, and Unspecified are unmappable to World Bank
  country entries and are excluded from GDP-dependent queries
- Pipeline is batch-oriented; real-time streaming ingestion is outside project scope
- `settings.yaml` configuration values are not yet read at runtime; parameters
  remain in the script's configuration block
