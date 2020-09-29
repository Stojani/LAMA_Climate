#!/usr/bin/python3

from kafka import KafkaConsumer
import json
import pandas as pd
from pyspark.sql import SparkSession

# create sparksession
spark = SparkSession.builder \
    .master("local") \
    .appName("climate_batch_consumer") \
    .getOrCreate()

# define mapping dataframe - HBase table
catalog = "".join("""
    {
        "table": {
            "namespace": "default",
            "name": "batch_climate_streams"
        },
        "rowkey": "unix_date",
        "columns": {
            "dt": {"cf": "rowkey", "col": "unix_date", "type": "long"},
            "all": {"cf": "weather", "col": "clouds", "type": "double"},
            "humidity": {"cf": "weather", "col": "humidity", "type": "double"},
            "pressure": {"cf": "weather", "col": "pressure", "type": "double"},
            "temp": {"cf": "temperature", "col": "temp", "type": "double"},
            "temp_min": {"cf": "temperature", "col": "min_temp", "type": "double"},
            "temp_max": {"cf": "temperature", "col": "max_temp", "type": "double"},
            "deg": {"cf": "wind", "col": "deg", "type": "double"},
            "speed": {"cf": "wind", "col": "speed", "type": "double"}
        }
    }
""".split())

# function that writes given dataframe to Hbase 
def write_batch_to_hbase(df):
  df.write \
    .format("org.apache.spark.sql.execution.datasources.hbase") \
    .mode("append") \
    .options(catalog=catalog,newTable="5") \
    .save()

# conf kafka consumer
consumer = KafkaConsumer('batch-climate',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer = lambda v: json.loads(v.decode('utf-8')))

# scan messages in kafka topic and write coming dataframes to db
for message in consumer:
    data = json.loads(message.value)
    dicto = data["main"]
    dicto.update(data["wind"])
    dicto.update(data["clouds"])
    dicto["dt"]=data["dt"]
    utilDF = pd.DataFrame(dicto, index=[0])[['temp', 'temp_min', 'temp_max', 'pressure', 'humidity', 'speed', 'deg', 'all', 'dt']]
    finalDF = spark.createDataFrame(utilDF)
    write_batch_to_hbase(finalDF)

