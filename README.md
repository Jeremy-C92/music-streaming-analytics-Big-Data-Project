# music-streaming-analytics-Big-Data-Project
Lambda Architecture for Real-Time Music Streaming Analytics with Kafka &amp; Spark

Description :

"An end-to-end Big Data pipeline implementing the Lambda Architecture to analyze music streaming trends. This project solves the latency vs. accuracy trade-off by combining a Hot Layer (Spark Structured Streaming on Kafka) for real-time metrics and a Cold Layer (Spark SQL on Parquet Data Lake) for deep historical analysis. Containerized with Docker for reproducibility."

Key Technical Highlights :

- Real-time Ingestion: Handling high-velocity event streams via Apache Kafka (v7.5).

- Hybrid Processing: Simultaneous stream processing (Windowed Aggregation) and batch persistence.

- Infrastructure as Code: Full Docker Compose orchestration with custom networking and volume management.

- Robustness: Implemented retry logic for Kafka producers and checkpointing for Spark Streaming.