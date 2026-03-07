# Retail Inventory Management Using Big Data

## Project Overview
This project demonstrates scalable retail data analytics using Apache Hadoop and Apache Spark.

The system:
1. Uploads retail transaction data to HDFS
2. Cleans and filters transaction records
3. Scales the dataset synthetically
4. Runs distributed Spark analytics to compute product revenue
5. Measures performance as data size increases

The system will be implemented using a layered Big Data architecture that incorporates
distributed storage, parallel processing, and SQL-based querying. This project is
being developed as part of the CS 4265 - Big Data Analytics course.

## Technologies
- Hadoop Distributed File System (HDFS)
- Apache Spark
- Apache Hive / Spark SQL
- Parquet and CSV data formats

## Dataset
The dataset used in this project is the UCI Online Retail dataset. Due to GitHub file size 
limits, the dataset is not stored in this repository.

Download it from:
https://archive.ics.uci.edu/ml/datasets/online+retail

After downloading, place it in the project directory and upload it to HDFS using:

bash src/hdfs_setup.sh

## Repository Structure
src/           - Spark scripts and implementation  
screenshots/   - code outputs and architecture diagrams  
docs/          - project report and documentation  
screenshots/   - proof of execution and outputs

## Running the Project

1. Start Hadoop (Version 3.3.6):
   start-dfs.sh

2. Upload dataset:
   bash src/hdfs_setup.sh

3. Run Spark job (Spark version 3.5.1):
   spark-shell -i src/retail_analysis.scala

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
  Further dataset scaling factors will be tested to better demonstrate distributed processing performance.

- Additional Documentation and Results
  Future updates will include expanded documentation of experiment results and performance observations.

Overall, the core infrastructure for data ingestion, distributed processing, and analytics is complete. The project is currently focused on evaluating system performance and documenting results.
