#!/bin/bash

#Start HDFS
start-dfs.sh

# Create directory
hdfs dfs -mkdir -p /retail

# Upload dataset
hdfs dfs -put retail.csv /retail/

# Verify upload
hdfs dfs -ls /retail
