#!/bin/bash

echo "Starting producer"
gnome-terminal -- python3 ./api-producer_last.py antarctica 5 False
echo "Started!"

# Wait 2 minutes
sleep 60;

echo "Starting consumer"
gnome-terminal -- python3 ./consumer.py
echo "Started!"

# Wait 2 minutes
sleep 120;

echo "Starting consumer analysis"
bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0-beta --conf spark.cassandra.connection.host=127.0.0.1 new-streaming-processor.py
echo "Started!"


