# Cars Streaming Anomaly Detection Platform

## 🚗 Overview

This platform provides real-time monitoring, enrichment, and anomaly detection for vehicle telemetry data using a modern data lakehouse architecture. It integrates Apache Spark, Iceberg, Kafka, Airflow, Trino, Superset, and a Telegram bot with AI assistant capabilities for end-to-end data pipeline management and analytics.

---

## 📦 Project Structure

- **airflow/**: DAGs for orchestrating streaming jobs and maintenance tasks.
- **jobs/**: Spark jobs for data generation, enrichment, alerting, aggregation, and compaction.
- **configs/**: Table schemas, data generation scripts, compaction utilities, and constants.
- **telegram_bot/**: Telegram bot handlers for Airflow, Trino, Spark, Superset, and AI agent.
- **docker/**: Docker scripts for environment setup.
directories.

---

## ⚙️ Main Components

### 1. Data Generation & Ingestion

- [`jobs/4_data_generator.py`](jobs/4_data_generator.py): Streams random car sensor events to Kafka for testing.
- [`airflow/dags/4_job_dag.py`](airflow/dags/4_job_dag.py): Orchestrates the data generator job.

### 2. Data Enrichment

- [`jobs/5_enriching.py`](jobs/5_enriching.py): Enriches raw sensor data with car, model, and color dimensions, calculates expected gear, and writes to Kafka and Iceberg.
- [`airflow/dags/5_job_dag.py`](airflow/dags/5_job_dag.py): Orchestrates the enrichment job.

### 3. Real-Time Anomaly Detection

- [`jobs/6_alerting.py`](jobs/6_alerting.py): Detects anomalies (speed > 120, gear mismatch, RPM > 6000), flags alerts, writes to Iceberg, and streams alerts to Kafka.
- [`airflow/dags/6_job_dag.py`](airflow/dags/6_job_dag.py): Orchestrates the alerting job.

### 4. Aggregation & Notification

- [`jobs/7_print_aggregations.py`](jobs/7_print_aggregations.py): Aggregates alerts in 1-minute windows, computes stats by car color, and sends summaries to Telegram.
- [`jobs/tg_functions_job_7.py`](jobs/tg_functions_job_7.py): Formats and sends Telegram notifications.
- [`airflow/dags/7_job_dag.py`](airflow/dags/7_job_dag.py): Orchestrates the aggregation job.

### 5. Table Maintenance

- [`jobs/8_table_compaction.py`](jobs/8_table_compaction.py): Compacts Iceberg tables, rewrites manifests, expires old snapshots, and removes orphan files.
- [`configs/spark/jobs/compaction_funcs.py`](configs/spark/jobs/compaction_funcs.py): Utility functions for table maintenance.
- [`airflow/dags/8_job_dag.py`](airflow/dags/8_job_dag.py): Orchestrates compaction jobs.

---

## 🗄️ Data Lakehouse Tables

- **Dimension Tables**: Car models, car colors, cars ([`jobs/1_car_models.py`](jobs/1_car_models.py), [`jobs/2_car_colors.py`](jobs/2_car_colors.py), [`jobs/3_cars.py`](jobs/3_cars.py))
- **Raw Table**: `cars_raw` for audit and reprocessing ([`jobs/3_1_cars_raw.py`](jobs/3_1_cars_raw.py))
- **Enriched Table**: `cars_enriched` with full event context and alert flags
- **Alert Table**: Aggregated alerts for analytics

All tables use Apache Iceberg format with S3 storage.

---

## 🤖 Telegram Bot Features

- **Airflow Control**: List, trigger, monitor, and kill DAGs ([`telegram_bot/handlers/airflow.py`](telegram_bot/handlers/airflow.py))
- **Trino Queries**: Browse schemas/tables, run SELECT queries, count rows ([`telegram_bot/handlers/trino_queries.py`](telegram_bot/handlers/trino_queries.py))
- **Spark Monitoring**: Check running streaming jobs ([`telegram_bot/handlers/spark.py`](telegram_bot/handlers/spark.py))
- **Superset Dashboards**: List and access dashboards ([`telegram_bot/handlers/superset.py`](telegram_bot/handlers/superset.py))
- **AI Assistant**: Natural language control via GPT ([`telegram_bot/handlers/ai_agent.py`](telegram_bot/handlers/ai_agent.py))

---

## 🚀 Quick Start

1. **Build and Start Services**  
   Use `docker-compose up` to launch all required services.

2. **Initialize Tables**  
   Run setup DAGs or submit jobs:
   ```sh
   docker exec spark spark-submit jobs/1_car_models.py
   docker exec spark spark-submit jobs/2_car_colors.py
   docker exec spark spark-submit jobs/3_cars.py
   docker exec spark spark-submit jobs/3_1_cars_raw.py
   ```

3. **Start Streaming Jobs**  
   Trigger DAGs via Airflow UI or Telegram bot:
   - `/dags` — List DAGs
   - `/run <dag_id>` — Start a job (e.g., `4_data_generator`, `5_enriching`, `6_alerting`, `7_print_aggregations`)

4. **Monitor & Query**  
   - Use Telegram bot for pipeline control, queries, and dashboards.
   - AI assistant: `/ai` or "🤖 AI Assistant" button.

---

## 📊 Analytics & Dashboards

- **Superset**: Visualize aggregated alerts and car data.
- **Trino**: SQL queries on Iceberg tables.

---

## ⚡ Real-Time Alerts

- Alerts for speeding, gear mismatch, and high RPM.
- Telegram notifications every minute with aggregated stats.

---

## 🛠️ Maintenance

- Daily compaction and cleanup of Iceberg tables via Airflow DAGs and Spark jobs.

---

## 📝 Environment & Configuration

- Set credentials and endpoints in `.env`.
- See [`telegram_bot/config.py`](telegram_bot/config.py) and [`configs/constants.py`](configs/constants.py) for details.

---

## 📚 Documentation

- See [`docs/`](docs/) for architecture diagrams and usage guides.

---

## 🧑‍💻 Contributing

Pull requests and issues are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) if available.

---

## 📄 License

See [LICENSE](LICENSE) for details.

---

## 💬 Support

For help, use `/help` in the Telegram bot or contact the maintainer.
