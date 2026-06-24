#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="quickstart"

cleanup() {
    echo "Cleaning up container..."
    docker rm -f "$CONTAINER_NAME" || true
    echo "Cleaning up decompressed test data..."
    rm -f test_data/72505394728.csv test_data/NYPD_Crash_Data.csv test_data/weatherdata.json test_data/crashdata.json || true
}

# Ensure cleanup runs on exit/error
trap cleanup EXIT

echo "Removing existing container if any..."
docker rm -f "$CONTAINER_NAME" || true

echo "Starting StarRocks 4.0 (allin1-ubuntu) container..."
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd --name "$CONTAINER_NAME" starrocks/allin1-ubuntu

echo "Waiting for StarRocks to start up and be healthy (up to 10 minutes)..."
READY=false
# 120 attempts of 5s sleep = 10 minutes max for slow pulls/starts
for i in {1..120}; do
    if docker exec "$CONTAINER_NAME" mysql -P 9030 -h 127.0.0.1 -u root -e "SELECT 1" >/dev/null 2>&1; then
        READY=true
        break
    fi
    echo "Waiting for StarRocks to respond... ($i/120)"
    sleep 5
done

if [ "$READY" = false ]; then
    echo "Error: StarRocks did not start in time."
    exit 1
fi

echo "StarRocks is up! Stabilizing for 10 seconds..."
sleep 10

echo "Initializing database and tables..."
docker exec -i "$CONTAINER_NAME" mysql -P 9030 -h 127.0.0.1 -u root < test_data/test_data.sql

echo "Running E2E tests..."
STARROCKS_E2E=1 cargo test --test e2e_tests -- --nocapture
