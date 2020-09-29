#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4,com.hortonworks.shc:shc-core:1.1.0.3.1.5.90-1 \
	--repositories http://repo.hortonworks.com/content/groups/public/ \
	../batch_consumer.py

