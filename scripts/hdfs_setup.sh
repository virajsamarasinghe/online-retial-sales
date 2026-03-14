#!/usr/bin/env bash
# hdfs_setup.sh - Re-configured for Industry Structure
set -euo pipefail

# --- Configuration ---
RAW_DATA_PATH="data/online_retail_raw.csv"
HDFS_RAW_DIR="/user/retail/raw"

echo "=== HDFS Setup (Starting Hadoop Daemons) ==="
hdfs --daemon stop namenode || true
hdfs --daemon stop datanode || true
sleep 2
hdfs --daemon start namenode
hdfs --daemon start datanode
sleep 5

echo "Waiting for HDFS to leave Safe Mode..."
hdfs dfsadmin -safemode wait

# Clean up and recreate HDFS directories
hdfs dfs -rm -r -f /user/retail/raw /user/retail/cleaned /user/retail/output2 || true
hdfs dfs -mkdir -p "$HDFS_RAW_DIR"

if [ -f "$RAW_DATA_PATH" ]; then
    echo "Uploading raw data from $RAW_DATA_PATH to HDFS..."
    hdfs dfs -put "$RAW_DATA_PATH" "$HDFS_RAW_DIR/"
else
    echo "ERROR: Raw data not found at $RAW_DATA_PATH"
    exit 1
fi

echo "=== HDFS Setup Complete ==="
