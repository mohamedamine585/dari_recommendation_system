#!/bin/bash
set -e

# Start Spark master in the background
/opt/bitnami/spark/sbin/start-master.sh

# Optional: wait for master to start
sleep 5

# Run your Python script in the foreground (this will block the container)
python /scheduler/receiver.py
