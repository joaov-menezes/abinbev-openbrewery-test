FROM apache/airflow:latest

# Switch to root user to install system dependencies
USER root

# Install git, OpenJDK, and clean up apt cache
RUN apt-get update && \
    apt-get -y install git default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch to airflow user to install Python packages
USER airflow

# Copy requirements.txt to the container
COPY requirements.txt /requirements.txt

# Install necessary Python packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt