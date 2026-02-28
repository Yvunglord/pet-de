# Pet Data Engineering Project

A comprehensive data ingestion and processing pipeline for real-time and batch data, featuring:

- Real-time cryptocurrency trades via Binance WebSocket
- FakeStore API REST data ingestion
- Change Data Capture (CDC) with Debezium and Kafka
- Stream processing with Apache Spark (Structured Streaming)
- Data warehousing with ClickHouse
- Orchestration with Apache Airflow (future)
- Monitoring with Kafdrop, Flower, and pgAdmin

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone git@github.com:Yvunglord/pet-de.git
cd pet-de
```

### 2. Environment configuration

Copy the example environment file and adjust if needed:
```bash
cp .env.example .env
```

Edit .env to set your own passwords and settings.

### 3. Start the infrastructure

The first start will automatically download required JAR files (PostgreSQL JDBC driver for Debezium, ClickHouse JDBC driver for Spark, etc.) via the jar-downloader service.
```bash
docker compose up -d
```
Wait a few minutes for all services to become healthy. You can monitor progress with:
```bash
docker compose logs -f
```

### 4. Access web UIs


| Service  | URL |
| ------------- | ------------- |
| Airflow  | http://localhost:8080  |
| pgAdmin  | http://localhost:5050  |
| Kafdrop  | http://localhost:9000  |
| Spark Master  | http://localhost:8081  |
| Flower  | http://localhost:5555  |
| ClickHouse  |  http://localhost:8123  |

## 🛑 Stopping and Cleaning Up
```bash
docker-compose down         # stop containers
docker-compose down -v      # stop and remove volumes (WARNING: deletes all data)
```