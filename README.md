# Project Overview

This project implements a complete **Big Data Lambda Architecture** to analyze music streaming data. It solves the challenge of balancing low-latency reporting with comprehensive historical analysis by using two parallel processing layers.

The pipeline simulates a high-traffic streaming platform (similar to Spotify) and processes user events to detect trends in real-time while archiving data for deep analytics.

##  Why We Selected These Tools

* **Apache Kafka:** We chose Kafka as our message broker because it is the industry standard for high-throughput, fault-tolerant data ingestion. It acts as a buffer between our data generator and our processing engine.
* **Apache Spark:** We selected Spark (Structured Streaming & SQL) because it allows us to use the same engine for both real-time windowed aggregations (Hot Layer) and complex historical SQL queries (Cold Layer).
* **Parquet:** For storage, we used Parquet files to build our Data Lake. Its columnar format and Snappy compression are optimized for Big Data analytical queries.

## Technical Architecture

The entire infrastructure is containerized using **Docker Compose** to ensure reproducibility.

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Generator** | Python (Kafka-Python) | Simulates thousands of user listening events/sec. |
| **Message Broker** | Confluent Kafka (v7.5) | Buffers and distributes events to consumers. |
| **Stream Engine** | Spark Structured Streaming | Consumes Kafka topics, aggregates metrics, and writes to Data Lake. |
| **Batch Engine** | Spark SQL | Reads Parquet files for weekly charts & geographic analysis. |
| **Storage** | Parquet Files | Columnar storage format for optimized querying (Snappy compression). |


## Quick Start

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

## Data Lake Structure & Management

Our architecture implements a **medallion-style storage logic** where raw events are persisted for long-term durability.

* **`data/datalake/song_plays/`**: This is the root of our Cold Layer. Data is stored in **Parquet format**, a columnar storage optimized for Big Data.
* **`date=YYYY-MM-DD/hour=X/`**: We use **Time-based Partitioning**. This allows Spark to perform "Partition Pruning", skipping irrelevant folders when calculating a monthly top chart, which drastically improves performance.
* **`_checkpoint/`**: Vital for the **Hot Layer**. It stores the current offset of the Kafka stream. If the system crashes, Spark uses this metadata to resume exactly where it left off without losing data (Resilience).
* **`_spark_metadata/`**: A transaction log that ensures "Exactly-once" processing semantics when writing Parquet files.

## Challenges Encountered & Solutions

During the implementation, we handled several real-world Big Data integration issues:

* **Kafka Listener & Network Isolation:** We initially faced `NoBrokersAvailable` errors because the Python Producer (running on the Host) and Spark (running in Docker) required different access points. We resolved this by configuring dual listeners: `29092` for internal Docker traffic and `9092` for external host access.
* **Data Persistence & Volume Mapping:** A major challenge was the "Path does not exist" error during batch processing. We solved this by synchronizing Docker volumes in the `docker-compose.yml` to ensure the Stream Job writes to the same physical `./data` directory that the Batch Job reads from.
* **Spark PATH Resolution:** Since `spark-submit` was not in the default OCI runtime PATH of the Spark image, we had to use the absolute path `/opt/spark/bin/spark-submit` to execute our jobs within the container.
* **Schema Enforcement & Data Integrity:** Because Kafka treats data as raw bytes, we implemented a strict `StructType` schema in Spark. This ensures all incoming JSON events are correctly typed and validated before being stored in the Data Lake.


## Our Setup Notes (Problem Solving Process)

This project was a significant learning experience in managing distributed Big Data systems. Below is a detailed breakdown of the technical hurdles I encountered and how we solved them using log analysis.

### 1. Resolving the Spark PATH & Environment Recognition
Initially, attempting to run Spark jobs via a simple `spark-submit` command failed with the following error:
> `OCI runtime exec failed: exec failed: unable to start container process: exec: "spark-submit": executable file not found in $PATH`

**The fix:** We identified that in the official `apache/spark` image, binaries are not in the global PATH. We resolved this by using the absolute path `/opt/spark/bin/spark-submit` to reach the engine within the container.

### 2. Debugging Kafka Connectivity (The "No Brokers" Trap)
When launching the Producer, We encountered persistent connectivity issues:
> `WARNING - Connection attempt 1/10 failed: NoBrokersAvailable`

Even after manually updating the code to `localhost:9092`, the logs showed the script was still trying to connect to port `29092`.
**The fix:** By inspecting the code, We found that `argparse` was overriding my manual changes. We learned to use the command line argument `--broker localhost:9092` to successfully bypass the internal defaults and connect from my host machine.

### 3. Handling Data Loss & Offset Synchronization
During the first stable stream, Spark issued a warning regarding offset mismatches:
> `WARN KafkaMicroBatchStream: Partition song-plays-0's offset was changed from 26701 to 3497, some data may have been missed.`

**The fix:** This taught us how Spark tracks progress in Kafka. We ensured the implementation of a `checkpointLocation` in the `stream_job.py`. This metadata allows Spark to save its state, ensuring that if the container restarts, it resumes exactly where it left off without re-processing the entire stream.

Once the environment was stabilized, the system achieved a steady ingestion rate:
> `INFO - 📈 Sent 1000 events | [cite_start]Rate: 9.8/sec`



The **Lambda Architecture** successfully synchronized both layers: the **Hot Layer** (Spark Streaming) provided 10-minute windowed results, while the **Cold Layer** (Parquet files) ensured data was safely persisted in the Data Lake for deep batch analysis.


## Key Learnings

This project demonstrated the power of the **Lambda Architecture**. While the **Stream Job** provides immediate visibility into viral songs (Trending Now), the **Batch Job** ensures that we can always recalculate high-accuracy business metrics (Top of the Month) even if the real-time windowing parameters change.
