
# 📋 Project Overview

This project implements a complete **Big Data Lambda Architecture** to analyze music streaming data. It solves the challenge of balancing low-latency reporting with comprehensive historical analysis by using two parallel processing layers.

The pipeline simulates a high-traffic streaming platform (similar to Spotify) and processes user events to detect trends in real-time while archiving data for deep analytics.

## Architecture Highlights
* **Hot Layer (Real-Time):** Processes live streams using **Spark Structured Streaming** to calculate "Trending Songs" in 10-minute sliding windows.
* **Cold Layer (Batch):** Performs complex aggregations on historical data stored in a **Parquet Data Lake** using **Spark SQL**.
* **Ingestion:** A Python-based producer generates realistic events (weighted popularity, geolocation, device types) sent to **Apache Kafka**.

## 🏗️ Technical Architecture

The entire infrastructure is containerized using **Docker Compose** to ensure reproducibility.

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Generator** | Python (Kafka-Python) | Simulates thousands of user listening events/sec. |
| **Message Broker** | Confluent Kafka (v7.5) | Buffers and distributes events to consumers. |
| **Stream Engine** | Spark Structured Streaming | Consumes Kafka topics, aggregates metrics, and writes to Data Lake. |
| **Batch Engine** | Spark SQL | Reads Parquet files for weekly charts & geographic analysis. |
| **Storage** | Parquet Files | Columnar storage format for optimized querying (Snappy compression). |


## 🚀 Quick Start

1.  **Launch Infrastructure:**
    ```bash
    docker compose up -d
    ```

2.  **Start Producer (Windows):**
    ```bash
    pip install kafka-python
    python producer.py --broker 127.0.0.1:9092
    ```

3.  **Run Spark Jobs (Docker):**
    ```bash
    # Real-time Streaming
    docker exec -it spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark/work-dir/stream_job.py

    # Batch Analysis (after a few mins)
    docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/batch_job.py
    ```