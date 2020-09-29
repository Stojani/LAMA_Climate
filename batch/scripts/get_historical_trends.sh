#!/usr/bin/env bash

$SPARK_HOME/bin/spark-submit \
	--packages com.hortonworks.shc:shc-core:1.1.0.3.1.5.90-1 \
	--repositories http://repo.hortonworks.com/content/groups/public/ \
	../get_historical_trends.py

