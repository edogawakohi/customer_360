# CUSTOMER 360 BEHAVIORAL ANALYTICS
Processing telecom logs (JSON/PARQUET) using Pyspark and Gemini. This project focuses about behavioral and interaction data.
## OVERALL PIPELINE FLOW
![](docs/etl.png)

## 1.Overview

**Customer 360** is a data engineering project that builds a unified and comprehensive view of customers by integrating data from multiple touchpoints. The project processes customer viewing and search logs through an end-to-end ETL pipeline using **PySpark**, stores the transformed data in **MySQL**, and visualizes business insights with **Power BI**.

### Key Objectives

* **Unify Customer Data:** Integrate **Content Logs** and **Search Logs** to create a single customer profile.
* **Understand Customer Behavior:** Analyze customer activity levels (High/Low) and identify content preferences based on viewing and search history.
* **Track Preference Trends:** Monitor changes in customer search interests over time to uncover behavioral patterns.
* **Generate Business Insights:** Transform raw log data into meaningful analytics that support customer segmentation and decision-making.

## 2.Detailed process
**Pipeline 1: Log Content Processing(Viewing Data - April)** 

This pipeline processes customer viewing logs to generate customer behavior metrics and preference profiles.
* **Content Classification:** Map raw `AppName` values into standardized content categories including: Truyền hình, Phim ảnh, Giải trí, Thiếu nhi, Thể thao.
* **`Active` User Classification:** Customers are categorized based on the number of active days in a month:
    * **High:** More than **20** active days.
    * **Medium:** Between **10 and 20** active days (inclusive).
    * **Low:** Fewer than **10** active days.
* **Preference Profiling:** Identify each customer's most frequently watched content category (`MostWatch`) and generate an overall content preference profile (`Taste`) based on viewing behavior.

****
