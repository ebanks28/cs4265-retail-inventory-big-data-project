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
Retail transaction data covering approximately 541,909 records from a UK-based online retailer
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
│   ├── CS4265_Evan_Banks_M2.pdf              # Milestone 2 progress report
│   └── CS4265_Evan_Banks_M3.pdf              # Milestone 3 progress report
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

## Current Status
### What Works
The core system for the Retail Inventory Big Data Project is operational. The following components have been successfully implemented and tested:
- HDFS Data Ingestion
  The retail dataset can be uploaded to Hadoop Distributed File System (HDFS) using the provided setup script (src/hdfs_setup.sh). The script creates the required   directory and uploads the dataset to /retail in HDFS.

- Spark Data Processing Pipeline
  A working analytics pipeline has been implemented in src/retail_analysis.scala using Apache Spark. The pipeline performs the following steps:
  -  Reads the retail dataset from HDFS
  -  Prints the dataset schema and record counts
  -  Cleans the data by removing invalid rows (negative quantities, zero prices, and null values)
  -  Scales the dataset synthetically using a crossJoin to simulate larger data volumes
  -  Executes an aggregation query to compute total revenue by product (StockCode)

- Distributed Query Execution
  The Spark job successfully executes the revenue aggregation query and displays the highest-revenue products.

- Reproducibility
  The repository includes scripts and instructions that allow the full pipeline to be reproduced:

  1. Upload dataset to HDFS

  2. Launch Spark

  3. Execute the analytics script

### What's In-Progress
The following components are currently in progress or planned for future development:

- Performance Analysis
  Additional experimentation will be performed to analyze how query runtime changes as the dataset is scaled. Runtime measurements will be used to evaluate the scalability of the Spark processing pipeline.

- Expanded Scalability Testing
  Further dataset scaling factors will be tested to better demonstrate distributed processing performance. A second dataset, the World Bank Country Economic Data dataset, will be included to strengthen the multi-source justification.

- Additional Documentation and Results
  Future updates will include expanded documentation of experiment results and performance observations.

Overall, the core infrastructure for data ingestion, distributed processing, and analytics is complete. The project is currently focused on evaluating system performance and documenting results.
