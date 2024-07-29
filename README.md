## System Overview (Batch Layer)
<div align="center">
  <img src="https://github.com/user-attachments/assets/f7165453-ee9a-4779-81c5-b29ac5b5eb36" alt="Batch Layer Overview"/>
</div>

## System Overview (Streaming Layer)
<div align="center">
  <img src="https://github.com/user-attachments/assets/841f280f-39bf-43a2-b5a7-da0260ebc86a" alt="Streaming Layer Overview"/>
  <p>Big Data Solution Overview for Q-Retail Company</p>
</div>

## Objectives
1. Develop a data ingestion pipeline for batch files and streaming data from Kafka.
2. Create a scalable data storage solution using a data lake architecture.
3. Implement data processing workflows for transforming raw data into structured formats.
4. Establish a data warehouse using Hive tables for centralized business intelligence.
5. Provide actionable insights and reports to business teams.





Our data pipeline follows the typical stages of a data engineering pipeline. It consists of the following five stages:

1. **Data Sources:** Data comes from two sources:
   1. CSV files (batch)
   2. Mobile application (streaming)

2. **Storage:** HDFS
3. **Transformation:** Data processing using Spark and Spark Streaming
4. **Serving:** DWH Modeling in Hive and Reporting
5. **NoSQL:** HBase

## Tools and Technologies

- **HDFS:** Scalable data storage
- **Apache Spark:** Data processing, transformation, and analysis
- **Apache Hive:** Data warehousing and SQL-based querying
- **Apache Kafka:** Real-time data streaming
- **Python:** Scripting and moving data to HDFS
- **AirFlow:** Automation and creating DAGs for data flow
- **HBase:** NoSQL database for storing and retrieving large amounts of sparse data, addressing the small file (logs) problem
- **Docker:** Containerization for consistent and isolated environments

## Streaming Data Ingestion

- **Kafka producer** sends app logs to Kafka
- **Spark** streaming job reads, processes, and stores data in **HBase**

## Handling Small File Problem with HBase

HBase was used to manage the system's data efficiently. To handle the problem of small files and improve performance by avoiding row-by-row writes, we implemented a batching mechanism:

- Every 100 records are grouped into a batch and sent together
- This approach minimizes the number of write operations

## Implementing SCD Type 2 with Spark

We implemented Slowly Changing Dimensions Type 2 (SCD Type 2) using Apache Spark. This approach:

- Manages historical data by maintaining the history of changes in data over time
- Enables effective tracking and querying of historical records
