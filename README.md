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
src/        - Spark scripts and implementation  
data/       - dataset references  
docs/       - project report and documentation  
screenshots/ - proof of execution and outputs

## Running the Project

1. Start Hadoop (Version 3.3.6):
   start-dfs.sh

2. Upload dataset:
   bash src/hdfs_setup.sh

3. Run Spark job (Spark version 3.5.1):
   spark-shell -i src/retail_analysis.scala
