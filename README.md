# streaming_dag 🚀

This project demonstrates a real-time data streaming pipeline using Apache Airflow, Kafka, and Cassandra. It fetches user data from a public API, processes it, and streams it to a Kafka topic. A separate Spark job consumes this data from Kafka and persists it into a Cassandra database.

## Table of Contents 📖

- [Project Title & Badges](#project-title--badges)
- [Description](#description)
- [Features](#features-✨)
- [Tech Stack](#tech-stack-🛠️)
- [Installation](#installation-⚙️)
- [Usage](#usage-💡)
- [Project Structure](#project-structure-📁)
- [Contributing](#contributing-🤝)
- [License](#license-📜)
- [Important Links](#important-links-🔗)
- [Footer](#footer-✨)

## Project Title & Badges 🏷️

![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.0.1-orange)
![Kafka](https://img.shields.io/badge/Kafka-2.8.1-brightgreen)
![Spark](https://img.shields.io/badge/Spark-3.5.1-red)
![Cassandra](https://img.shields.io/badge/Cassandra-4.0-informational)

## Description 📝

This project sets up a data streaming pipeline that leverages Apache Airflow for workflow orchestration. A core component is the ingestion of mock user data from the `randomuser.me` API. This data is then published to a Kafka topic, acting as a message broker. A PySpark application consumes these messages from Kafka and writes them into a Cassandra NoSQL database.

The primary goal is to showcase a scalable and resilient data ingestion and processing pattern suitable for real-time analytics and data warehousing.

## Features ✨

- **Real-time Data Ingestion**: Fetches data from an external API at regular intervals.
- **Kafka Integration**: Publishes streaming data to a Kafka topic for decoupling and buffering.
- **Spark Data Processing**: Utilizes PySpark for efficient stream processing.
- **Cassandra Persistence**: Stores processed streaming data in a NoSQL database.
- **Airflow Orchestration**: Manages the data pipeline using Airflow DAGs.
- **Dockerization**: Provides a Dockerfile for environment setup.

## Tech Stack 🛠️

| Category        | Technologies                                     |
|-----------------|--------------------------------------------------|
| Orchestration   | Apache Airflow                                   |
| Messaging       | Kafka (confluent-kafka)                          |
| Stream Processing | Apache Spark (PySpark)                           |
| Database        | Apache Cassandra (cassandra-driver)              |
| Language        | Python                                           |
| Containerization| Docker                                           |

## Installation ⚙️

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Kafka, ZooKeeper, and Cassandra instances (or use Docker Compose to set them up locally)

### Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/bibhutiregmi1/streaming_dag.git
    cd streaming_dag
    ```

2.  **Install Python dependencies:**
    The `requirements.txt` file lists the necessary Python packages. You can install them using pip:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Set up Airflow Environment:**
    The `Dockerfile` suggests using `apache/airflow:3.0.1`. Ensure your Airflow environment is configured to run the DAGs.

    *Note: The provided `docker-compose.yaml` and `airflow.cfg` files are substantial and require proper configuration for Airflow, Kafka, and Cassandra integration. It is recommended to set up these services using Docker Compose for a streamlined local development environment.*

4.  **Configure Kafka and Cassandra:**
    - Ensure Kafka is running and accessible at `localhost:9092` (or the configured address).
    - Ensure Cassandra is running and accessible at `localhost:9042` (or the configured address).

## Usage 💡

This project involves multiple components that need to be running concurrently:

1.  **Kafka Producer (Airflow DAG):**
    - The `dags/streaming_dags.py` file defines an Airflow DAG named `streaming_dag`.
    - This DAG is scheduled to run daily (`schedule_interval='@daily'`).
    - It contains a task `stream_data` that fetches user data from `https://randomuser.me/api/` and produces it to the `users_created` Kafka topic.
    - To run this, ensure your Airflow environment is set up and the DAG is enabled.

2.  **Spark Stream Consumer:**
    - The `spark_stream.py` script is responsible for consuming data from Kafka and writing it to Cassandra.
    - It establishes connections to Spark, Kafka, and Cassandra.
    - It defines the schema for the incoming JSON data from Kafka.
    - It creates the necessary keyspace (`spark_streams`) and table (`created_users`) in Cassandra if they don't exist.
    - It then starts a Spark streaming query to read from the `users_created` Kafka topic and write to the `created_users` Cassandra table.

**To run the pipeline:**

1.  **Start Kafka and Cassandra services.** (e.g., using Docker Compose).
2.  **Start the Airflow scheduler and webserver.**
3.  **Upload the `dags/streaming_dags.py` to your Airflow DAGs folder.**
4.  **Run the `spark_stream.py` script.** This can be done by executing it directly or integrating it into your Airflow DAGs as a PySpark task.

**Example of running the Spark script:**
```bash
python spark_stream.py
```

Once running, you should see user data being streamed from Kafka and populated into your Cassandra database.

### Real-world Use Cases 🌍

- **Real-time Analytics**: Process and analyze incoming user data for immediate insights.
- **User Profile Management**: Update user profiles in real-time as new data arrives.
- **Event-Driven Architectures**: Build systems where actions are triggered by incoming data events.
- **Data Warehousing**: Continuously populate a data warehouse with fresh data.

## Project Structure 📁

```
streaming_dag/
├── Dockerfile
├── .env
├── requirements.txt
├── docker-compose.yaml
├── config/
│   └── airflow.cfg
├── dags/
│   ├── dags.py
│   ├── streaming_dags.py
│   └── test.py
├── logs/
│   ├── ... (log files)
└── spark_stream.py
```
