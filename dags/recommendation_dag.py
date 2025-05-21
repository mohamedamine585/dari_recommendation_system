FROM python:3.9-slim

# Install Java (required for spark-submit)
RUN apt-get update && apt-get install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy your flow and setup script
COPY recommendation_pipeline.py /opt/airflow/dags/recommendation_pipeline.py
COPY setup.sh /setup.sh

# Make setup script executable
RUN chmod +x /setup.sh

# Install Prefect
RUN pip install prefect

# Expose Prefect Orion UI port
EXPOSE 4200

# Run the setup script on container start
CMD ["/setup.sh"]
