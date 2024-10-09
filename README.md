# Brewery Data Pipeline

## Objective
This project is designed to consume data from the Open Brewery DB API, transform it according to the Medallion Architecture, and persist it in a data lake. The pipeline is orchestrated using Apache Airflow, and the code is containerized using Docker.

## Medallion Architecture
The data pipeline follows the Medallion Architecture, consisting of three layers:

### Bronze Layer (Raw Data)
Data is fetched directly from the Open Brewery DB API and stored in the raw format (JSON).

### Silver Layer (Curated Data)
Data is transformed, cleaned, and partitioned by country in Parquet format. This ensures faster queries and better structure for analytical tasks.

### Gold Layer (Aggregated Data)
Data is aggregated to create a view that provides insights into the number of breweries by location and type.

## Tools Used
- **Apache Airflow**: For orchestrating the data pipeline.
- **Python (with PySpark)**: For data collection and transformation.
- **Docker**: For containerization.
- **Parquet**: For efficient storage in the silver layer.
- **Airflow Email Alerting**: For notifications in case of pipeline failure.
- **Medallion Architecture**: Bronze (raw), Silver (curated), Gold (aggregated) data layers.

## Setup Instructions

### Requirements
- Docker
- Docker Compose
- Python 3.7+
- PySpark 3.0+
- Apache Airflow

### 1. Clone the Repository
```bash
git clone https://github.com/joaov-menezes/abinbev-openbrewery-test.git
cd abinbev-openbrewery-test
```

### 2. Create a .env file
The .env file should contain the following variables:
```
FILE_PATH_BRONZE=/opt/airflow/data/bronze
FILE_PATH_SILVER=/opt/airflow/data/silver
OUTPUT_VIEW = 'vw_brewery_by_location_by_type'
EMAIL_RECIPIENT=your.email@example.com  # Airflow alert email
SMTP_HOST=smtp.mailserver.com
SMTP_PORT=587
SMTP_USER=username
SMTP_PASSWORD=password
```

### 3. Build and Start Docker Services
Run the following command to build the Docker containers and start the services, including Airflow and Spark.
```bash
docker-compose up --build
```

### 4. Access the Airflow Web Interface
Once the services are up, access the Airflow interface by navigating to:
```
http://localhost:8080
```
Log in with the default credentials (airflow/airflow).

### 5. Trigger the DAG
In the Airflow interface, find the DAG named `brewery_data_pipeline`. You can manually trigger the pipeline to run the process of fetching data from the API, transforming it, and creating the gold view.

## Data Pipeline Steps
- **Bronze Layer**: The pipeline collects data from the API using the requests library and saves it in JSON format in the bronze layer.

- **Silver Layer**: The data is cleaned, transformed, and partitioned by country in Parquet format, optimizing for queries.

- **Gold Layer**: Aggregation is performed to generate insights on the number of breweries by location and type. This is saved as a temporary view in Spark SQL.

## Monitoring and Alerting
The pipeline is equipped with monitoring and alerting using Apache Airflow. If any task in the DAG fails, an email notification will be sent to the specified recipient.

## Decisions and Design Choices

- **Why Parquet?** Parquet is used in the silver layer due to its efficient columnar storage format, which is optimized for analytical workloads.

- **Partitioning by Country**: Partitioning by country ensures that querying data by location is faster and more efficient.

- **Airflow for Orchestration**: Airflow provides retry mechanisms and robust scheduling, which are crucial for managing data pipeline execution and failure handling.

## Airflow Email Alerting
In the case of task failure, the pipeline sends an email alert to the recipient specified in the .env file.
