
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

## 📂 Data Lake Structure & Management

Our architecture implements a **medallion-style storage logic** where raw events are persisted for long-term durability.

* **`data/datalake/song_plays/`**: This is the root of our Cold Layer. Data is stored in **Parquet format**, a columnar storage optimized for Big Data.
* **`date=YYYY-MM-DD/hour=X/`**: We use **Time-based Partitioning**. This allows Spark to perform "Partition Pruning", skipping irrelevant folders when calculating a monthly top chart, which drastically improves performance.
* **`_checkpoint/`**: Vital for the **Hot Layer**. It stores the current offset of the Kafka stream. If the system crashes, Spark uses this metadata to resume exactly where it left off without losing data (Resilience).
* **`_spark_metadata/`**: A transaction log that ensures "Exactly-once" processing semantics when writing Parquet files.

## 🛠️ Challenges Encountered & Solutions

During the implementation, we handled several real-world Big Data integration issues:

* **Kafka Listener Misconfiguration:** We initially struggled with `NoBrokersAvailable` errors. We resolved this by configuring dual listeners: `29092` for internal Docker communication (Spark-to-Kafka) and `9092` for external host access (Python Producer).
* **Spark PATH Resolution:** Running `spark-submit` inside the container required using the absolute path `/opt/spark/bin/spark-submit` as it wasn't present in the default OCI runtime PATH.
* **Schema Enforcement:** Since Kafka handles data as raw bytes, we implemented a strict `StructType` schema in Spark to ensure the incoming JSON events are valid before processing.
* **Data Persistence:** To achieve a relevant "Monthly Top Chart", we modified the `producer.py` to generate randomized timestamps over a 30-day period, simulating historical depth in the Data Lake.

## 📈 Key Learnings

This project demonstrated the power of the **Lambda Architecture**. While the **Stream Job** provides immediate visibility into viral songs (Trending Now), the **Batch Job** ensures that we can always recalculate high-accuracy business metrics (Top of the Month) even if the real-time windowing parameters change.
