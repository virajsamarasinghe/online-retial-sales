# UNIVERSITY OF RUHUNA
## Faculty of Engineering
**Module:** EE7222/EC7204 Cloud Computing  
**Assignment:** Large-Scale Data Analysis Using MapReduce  
**Team Size:** 3 Members

---

### Team Information
| Name | Student ID | Role |
|---|---|---|
| [Your Name] | [Student ID] | Lead Developer |
| [Member 2] | [Student ID] | Data Analyst |
| [Member 3] | [Student ID] | Documentation |

---

## 1. Project Approach

The objective was to implement a custom MapReduce job to analyze a real-world dataset. The "Online Retail" dataset (541,909 records) was selected for large-scale revenue and temporal analysis.

To ensure scalability, modularity, and clean data ingestion, a **five-stage Hadoop pipeline** was implemented across organized subdirectories:

1.  **Cleaning Stage (`Cleaning/`):** A dedicated Map-only filtering stage to remove cancellations, invalid prices, and records with missing country data.
2.  **Task 1: Country Revenue (`Task1_CountryRevenue/`):** Aggregates total revenue per country.
3.  **Task 2: Monthly Trends (`Task2_MonthlyTrends/`):** Analyzes global revenue changes month-by-month.
4.  **Task 3: Country Monthly (`Task3_CountryMonthly/`):** Breaks down monthly revenue granularly per country.
5.  **Task 4: Market Leaders (`Task4_MarketLeaders/`):** Identifies the top-performing country for each individual month.

The implementation follows professional standards using partitioned packages for each task, ensuring full compatibility with Hadoop's cluster execution environments and standard Java IDEs.

---

## 2. Results Summary

The analysis processed 541,909 raw records, resulting in 530,104 valid transactions across 38 countries. 

### Top 10 Countries by Revenue
| Rank | Country | Total Revenue (£) |
|---|---|---|
| 1 | United Kingdom | 9,025,222.08 |
| 2 | Netherlands | 285,446.34 |
| 3 | EIRE (Ireland) | 283,453.96 |
| 4 | Germany | 228,867.14 |
| 5 | France | 209,715.11 |
| 6 | Australia | 138,521.31 |
| 7 | Spain | 61,577.11 |
| 8 | Switzerland | 57,089.90 |
| 9 | Belgium | 41,196.34 |
| 10 | Sweden | 38,378.33 |

**Global Totals:** The pipeline successfully aggregated a global revenue of £10,005,341.36. The MapReduce framework completed the entire execution (including data cleaning) in approximately 17 seconds on a single-node local cluster.

---

## 3. Result Interpretation

The results indicate a significant market dominance by the United Kingdom, which accounts for approximately **91% of total revenue** (£9,025,222.08). This high concentration identifies the retailer's primary domestic wholesale base as the core driver of business stability. 

A strong secondary market cluster is evident in **Western Europe**, with the Netherlands, Ireland (EIRE), Germany, and France serving as the primary international hubs. 

**Temporal analysis** reveals key market dynamics:
*   **Global Peak:** Total revenue peaked in **November 2011 (£1.5M)**.
*   **Market Leaders:** While the UK led every month, **Australia** dominated the international (non-UK/EU) growth in **June (£25k)** and **August (£22k)**, showing significant variance compared to its January performance (£9k).
*   **International Resilience:** EIRE and the Netherlands consistently ranked as the top non-UK markets, particularly peaking in September and October respectively.

---

## 4. Performance & Accuracy Observations

From an engineering perspective, the implementation achieved high efficiency through several key strategies:

*   **Combiner Optimization:** By using the Reducer as a Combiner across all stages, the volume of intermediate data transferred during the Shuffle phase was reduced by over **99%**, significantly minimizing network congestion.
*   **Pipeline Scalability:** The multi-stage architecture allows for independent scaling. The Map-only cleaning stage ensures only high-quality data enters the more resource-intensive aggregation phases. The current setup processes 4 parallel aggregation jobs in under **40 seconds**.
*   **Accuracy Measures:** The `DataCleaningMapper` filtered out approximately 11,800 records (cancellations and zero-price errors). Fixed-point arithmetic (rounding to 2 decimals) during revenue calculation prevented floating-point accumulation errors during global summation.

---

## 5. Model Expansion Suggestions

To transition from descriptive to predictive analytics, the following expansions are proposed:

1.  **Customer Segmentation (RFM):** Using `CustomerID` to identify high-value customer groups based on Recency, Frequency, and Monetary value.
2.  **Product Pareto Analysis:** Aggregating by `StockCode` to identify the core 20% of products driving 80% of revenue.
3.  **Predictive Forecasting:** Utilizing historical trends to train a regression model for forecasting revenue per country.
