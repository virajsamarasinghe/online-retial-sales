# Online Retail Sales Analysis - Hadoop MapReduce Pipeline

This project implements a multi-stage data processing pipeline using Apache Hadoop to analyze retail transaction data. The system is designed to process large-scale datasets, performed data cleaning, and generate aggregated financial insights.

## Project Structure
The project is organized into logical components following standard development practices:

* **data/**: Contains the raw transactional dataset.
* **src/main/java/**: Java source code for the MapReduce stages.
* **scripts/**: Shell scripts and the Python Analytical Dashboard.
* **docs/**: Technical reports, analysis results (Hadoop output), and performance insights.
* **target/**: build artifacts.
* **requirements.txt**: Python dependencies for the dashboard.

## Execution Guide
To run the analysis pipeline, follow these steps from the root directory:

1. **Environment Setup and Data Ingestion**
   Execute the setup script to start Hadoop daemons and upload the raw data to HDFS:
   ```bash
   bash scripts/hdfs_setup.sh
   ```

2. **Pipeline Execution**
   Compile the source code and run the 5-stage MapReduce pipeline:
   ```bash
   bash scripts/compile_and_run.sh
   ```

3. **Analytical Dashboard**
   Launch the industry-standard visualization tool to explore the results:
   ```bash
   pip install -r requirements.txt
   streamlit run scripts/dashboard.py
   ```

## Analytical Dashboard Features
The dashboard provides high-end business intelligence insights:
- **Executive Metrics:** Summary of global revenue and market reach.
- **Geographic Analysis:** Distribution of performance across international markets.
- **Temporal Trends:** Monthly growth trajectory and cross-country comparisons.
- **Market Share:** Visualization of dominance by monthly leaders.

### Dashboard Preview
![Executive Metrics](images/Screenshot%202026-03-15%20at%2001.10.29.png)
![Geographic Analysis](images/Screenshot%202026-03-15%20at%2001.10.57.png)
![Temporal Trends](images/Screenshot%202026-03-15%20at%2001.11.12.png)
![Market Share Leaders](images/Screenshot%202026-03-15%20at%2001.11.34.png)

## Project Overview
* **Dataset Size:** 541,909 records
* **Processing Model:** 5-Stage Distributed MapReduce Pipeline
* **Analytics Engine:** Streamlit-based Business Intelligence Dashboard
* **Objective:** Determination of global revenue, temporal trends, and market dominance.

Detailed technical information and analysis results are available in the docs/REPORT.md file.
# online-retial-sales
