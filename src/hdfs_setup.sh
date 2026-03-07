#!/bin/bash

#Start HDFS
start-dfs.sh

# Verify (HDFS is running if NameNode, DataNode, SecondaryNameNode, and JPS are all visible)
jps

# Create directory
hdfs dfs -mkdir -p /retail

# Upload dataset
hdfs dfs -put retail.csv /retail/

# Verify upload
hdfs dfs -ls /retail
