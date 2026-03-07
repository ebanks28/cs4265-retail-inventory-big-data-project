# Retail Inventory Management Using Big Data

## Project Overview
The purpose of this project is to design a Big Data analytics system for retail inventory
management. The goal is to analyze large-scale sales and inventory datasets in order to
support batch-oriented analytical queries such as inventory aggregation, demand trends,
and stock level summaries.

The system will be implemented using a layered Big Data architecture that incorporates
distributed storage, parallel processing, and SQL-based querying. This project is
being developed as part of the CS 4265 - Big Data Analytics course.

## Technologies
- Hadoop Distributed File System (HDFS)
- Apache Spark
- Apache Hive / Spark SQL
- Parquet and CSV data formats

## Repository Structure
src/        - Spark scripts and implementation  
data/       - dataset references  
docs/       - project report and documentation  
screenshots/ - proof of execution and outputs

## Running the Project

1. Start Hadoop (Version 3.3.6)
   start-dfs.sh

2. Upload dataset
   bash src/hdfs_setup.sh

3. Run Spark job (Spark version 3.5.1)
   spark-shell -i src/retail_analysis.scala
