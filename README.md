# Real-Time Ride-Sharing Analytics with Apache Spark

## Overview
This Hands-on demonstrates real-time data processing and analytics for a ride-sharing platform using Apache Spark Structured Streaming. The workflow includes ingesting live ride data, performing aggregations, and analyzing trends over time, with results stored in CSV format.

## Tasks
The Hands-on is divided into three main tasks:

### Task 1: Ingest and Parse Real-Time Ride Data
- Ingest streaming data from a socket source (e.g., `localhost:9999`).
- Parse JSON messages into a structured DataFrame with fields:
  - `trip_id`
  - `driver_id`
  - `distance_km`
  - `fare_amount`
  - `timestamp`
- Save parsed data to CSV files for further processing.

### Task 2: Real-Time Aggregations (Driver-Level)
- Compute real-time aggregations to answer:
  - Total fare per driver (`SUM(fare_amount)`).
  - Average trip distance per driver (`AVG(distance_km)`).
- Store the aggregated results in CSV files.

### Task 3: Windowed Time-Based Analytics
- Convert `timestamp` to `TimestampType` for time-based analysis.
- Perform a 5-minute windowed aggregation of `fare_amount`, sliding every 1 minute.
- Store windowed aggregations in CSV files.

## Setup Instructions

### Prerequisites
- Apache Spark installed
- Python 3.x installed
- `pyspark` package installed

### Running the Tasks
1. **Start data generator** to simulate real-time data:
   ```bash
   python data_generator.py
   ```
2. **Run Task 1** (ingests and parses data):
   ```bash
   python task1.py
   ```
3. **Run Task 2** (computes real-time aggregations):
   ```bash
   python task2.py
   ```
4. **Run Task 3** (performs windowed aggregations):
   ```bash
   python task3.py
   ```

## Troubleshooting
- If Spark fails to start, check your Python and Java installations.
- Ensure that port `9999` is available before starting the socket server.
- If CSV files are not generated, verify that data is being streamed correctly.

## Learning Outcomes
By completing this Hands-on, you will:
- Gain hands-on experience with Apache Spark Structured Streaming.
- Learn how to process, aggregate, and analyze streaming data in real-time.
- Understand time-windowed aggregations for trend analysis.

---

