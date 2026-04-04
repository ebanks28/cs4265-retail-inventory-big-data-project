#!/bin/bash
# hdfs_setup.sh
# -------------
# Sets up HDFS directories and uploads both data sources required for the M3 pipeline.
#
# Usage: bash src/hdfs_setup.sh
#
# Prerequisites:
#   - Hadoop 3.3.6 installed and configured in pseudo-distributed mode
#   - SSH service running (sudo service ssh start)
#   - Java 8 or 11 installed (required by Hadoop, verify with: java -version)
#   - Both source files downloaded and placed in the same directory as this script:
#       retail.csv
#       API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv
 
set -e  # Exit immediately if any command fails
 
# ---------------------------------------------------------------------------
# Configuration — update these paths if your files are stored elsewhere
# ---------------------------------------------------------------------------
RETAIL_FILE="retail.csv"
GDP_FILE="API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv"
 
HDFS_RETAIL_DIR="/retail"
HDFS_WORLDBANK_DIR="/retail/worldbank"
 
# ---------------------------------------------------------------------------
# Step 1: Start HDFS
# ---------------------------------------------------------------------------
echo "[INFO] Starting HDFS..."
start-dfs.sh
 
echo "[INFO] Waiting for HDFS to initialize..."
sleep 5
 
# ---------------------------------------------------------------------------
# Step 2: Verify HDFS is running
# ---------------------------------------------------------------------------
echo "[INFO] Verifying running Java processes..."
jps
 
echo "[INFO] Checking HDFS health..."
hdfs dfsadmin -report | grep "Live datanodes"
 
# ---------------------------------------------------------------------------
# Step 3: Create HDFS directories
# ---------------------------------------------------------------------------
echo "[INFO] Creating HDFS directories..."
hdfs dfs -mkdir -p "$HDFS_RETAIL_DIR"
hdfs dfs -mkdir -p "$HDFS_WORLDBANK_DIR"
 
# ---------------------------------------------------------------------------
# Step 4: Upload data sources
# ---------------------------------------------------------------------------
# Upload UCI Online Retail Dataset
if [ ! -f "$RETAIL_FILE" ]; then
    echo "[ERROR] $RETAIL_FILE not found. Download it from:"
    echo "        https://archive.ics.uci.edu/dataset/352/online+retail"
    exit 1
fi
echo "[INFO] Uploading UCI Online Retail Dataset..."
hdfs dfs -put -f "$RETAIL_FILE" "$HDFS_RETAIL_DIR/retail.csv"
 
# Upload World Bank GDP Dataset
if [ ! -f "$GDP_FILE" ]; then
    echo "[ERROR] $GDP_FILE not found. Download it from:"
    echo "        https://data.worldbank.org/indicator/NY.GDP.MKTP.CD"
    echo "        Click Download -> CSV, then extract the zip file."
    exit 1
fi
echo "[INFO] Uploading World Bank GDP Dataset..."
hdfs dfs -put -f "$GDP_FILE" "$HDFS_WORLDBANK_DIR/$GDP_FILE"
 
# ---------------------------------------------------------------------------
# Step 5: Verify uploads
# ---------------------------------------------------------------------------
echo "[INFO] Verifying HDFS contents..."
hdfs dfs -ls "$HDFS_RETAIL_DIR"
hdfs dfs -ls "$HDFS_WORLDBANK_DIR"
 
echo "[INFO] Setup complete. You can now run the analytics pipeline with:"
echo "       spark-shell --master local[*] -i src/multi_source_retail_analytics.scala"
