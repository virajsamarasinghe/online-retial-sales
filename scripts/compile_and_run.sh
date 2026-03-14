#!/usr/bin/env bash
# compile_and_run.sh - Simplified Structure
set -euo pipefail

# --- Configuration ---
SRC_DIR="src/main/java"
BUILD_DIR="target/classes"
JAR_PATH="target/sales-mapreduce.jar"
HADOOP_CP=$(hadoop classpath)

echo "=== Step 1: Compiling Java Source Files ==="
mkdir -p "$BUILD_DIR"
javac -Xlint:-options --release 8 -classpath "$HADOOP_CP" -d "$BUILD_DIR" $(find "$SRC_DIR" -name "*.java")
jar -cvf "$JAR_PATH" -C "$BUILD_DIR" .

echo "=== Step 2: Cleaning Previous HDFS Outputs ==="
hdfs dfs -rm -r -f /user/retail/cleaned /user/retail/output2 || true

# --- Execution and Log Capture ---
LOG_FILE="docs/EXECUTION_LOG.txt"
echo "EE7222/EC7204 - FULL PROJECT EXECUTION LOG" > "$LOG_FILE"
echo "Generated on: $(date)" >> "$LOG_FILE"
echo "------------------------------------------" >> "$LOG_FILE"

echo "=== Step 3: Running Pipeline Stage 1 (Cleaning) ===" | tee -a "$LOG_FILE"
hadoop jar "$JAR_PATH" Cleaning.DataCleaningDriver /user/retail/raw /user/retail/cleaned 2>&1 | tee -a "$LOG_FILE"

echo "=== Step 4: Running Pipeline Stage 2 (Country Total Aggregation) ===" | tee -a "$LOG_FILE"
hadoop jar "$JAR_PATH" Task1_CountryRevenue.SalesDriver /user/retail/cleaned /user/retail/output2 2>&1 | tee -a "$LOG_FILE"

echo "=== Step 5: Running Pipeline Stage 3 (Global Monthly Trends) ===" | tee -a "$LOG_FILE"
hdfs dfs -rm -r -f /user/retail/monthly || true
hadoop jar "$JAR_PATH" Task2_MonthlyTrends.MonthlySalesDriver /user/retail/cleaned /user/retail/monthly 2>&1 | tee -a "$LOG_FILE"

echo "=== Step 6: Running Pipeline Stage 4 (Country Monthly granular) ===" | tee -a "$LOG_FILE"
hdfs dfs -rm -r -f /user/retail/country_monthly || true
hadoop jar "$JAR_PATH" Task3_CountryMonthly.CountryMonthlyDriver /user/retail/cleaned /user/retail/country_monthly 2>&1 | tee -a "$LOG_FILE"

echo "=== Step 7: Running Pipeline Stage 5 (Top Country per Month) ===" | tee -a "$LOG_FILE"
hdfs dfs -rm -r -f /user/retail/top_country || true
hadoop jar "$JAR_PATH" Task4_MarketLeaders.TopCountryDriver /user/retail/country_monthly /user/retail/top_country 2>&1 | tee -a "$LOG_FILE"

echo "\n--- FINAL AGGREGATED RESULTS SUMMARY ---" >> "$LOG_FILE"
echo "=== Top 10 Countries by Revenue ===" >> "$LOG_FILE"
hdfs dfs -cat /user/retail/output2/part-r-00000 2>/dev/null | sort -t$'\t' -k2 -rn | head -10 >> "$LOG_FILE"

echo "=== Global Monthly Sales Trends ===" >> "$LOG_FILE"
hdfs dfs -cat /user/retail/monthly/part-r-00000 2>/dev/null >> "$LOG_FILE"

echo "=== Absolute Top Country per Month ===" >> "$LOG_FILE"
hdfs dfs -cat /user/retail/top_country/part-r-00000 2>/dev/null | sort >> "$LOG_FILE"

# Individual task files for convenience
hdfs dfs -cat /user/retail/cleaned/part-m-00000 2>/dev/null | head -n 100 > docs/Cleaning_Sample.txt || true
hdfs dfs -cat /user/retail/output2/part-r-00000 > docs/Task1_CountryRevenue.txt 2>/dev/null || true
hdfs dfs -cat /user/retail/monthly/part-r-00000 > docs/Task2_MonthlyTrends.txt 2>/dev/null || true
hdfs dfs -cat /user/retail/country_monthly/part-r-00000 > docs/Task3_CountryMonthly.txt 2>/dev/null || true
hdfs dfs -cat /user/retail/top_country/part-r-00000 > docs/Task4_MarketLeaders.txt 2>/dev/null || true

echo "Comprehensive logs saved to $LOG_FILE"
