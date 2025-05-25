FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./scheduler ./scheduler
COPY ./spark ./spark



RUN pip install requests
# Set environment variables with defaults
ENV RECEIVER_HOST=http://dari/spark
ENV MYSQL_HOST=mysql
ENV MYSQL_USER=spark_user
ENV MYSQL_PASS=spark_pass
ENV MYSQL_DB=spark_scheduler


EXPOSE 5001

# Run the scheduler
CMD ["python", "scheduler/scheduler.py"]