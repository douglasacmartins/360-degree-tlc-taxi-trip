#!/bin/bash
export MSYS_NO_PATHCONV=1

# Check if the container exists (running or stopped)
if [ "$(docker ps -a -q -f name=spark-runner)" ]; then
    echo "Spark runner already exists. Stopping and removing..."
    docker stop spark-runner
    docker rm spark-runner
    exit 0
fi
    
echo "Starting Spark runner..."

# Resolve the exact directory of this script, so you can execute it from anywhere
TARGET_DIR="$(cygpath -w $(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/src)"
echo "Target directory: $TARGET_DIR"

# MSYS_NO_PATHCONV=1 prevents Git Bash from corrupting the "/app" Linux path
docker run -d --name spark-runner -v "$TARGET_DIR:/app" apache/spark-py sleep infinity

echo "Waiting for container to initialize..."
sleep 5

echo "Copying requirements.txt to container..."
REQ_PATH="$(cygpath -w $(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/requirements-spark.txt)"
docker cp "$REQ_PATH" spark-runner:/tmp/requirements.txt
echo "Installing python packages..."
docker exec --user root spark-runner pip install --upgrade pip
docker exec --user root spark-runner pip install -r /tmp/requirements.txt

echo "Spark runner started and initialized with all dependencies!"

echo "Executing Spark ETL job..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sh "$SCRIPT_DIR/submit-spark.sh"