#!/usr/bin/python3

from pyspark.sql import SparkSession

# create sparksession
spark = SparkSession.builder \
    .master("local") \
    .appName("climate_batch") \
    .getOrCreate()

# get data from json file
path = "./historical_climate.json"
climateDF = spark.read.json(path)

# clean date column
from pyspark.sql.functions import to_date, substring, unix_timestamp
climateDF = climateDF.withColumn('date', to_date(substring('dt_iso', 0, 10), 'yyyy-MM-dd'))
climateDF = climateDF.withColumn('unix_date', unix_timestamp('date',"yyyy-MM-dd"))

# aggregate util data by date
from pyspark.sql.functions import avg, min, max, round
aggrDF = climateDF.groupBy('unix_date').agg(round(avg('clouds.all'), 2).alias('avg_clouds'), min('clouds.all').alias('min_clouds'), max('clouds.all').alias('max_clouds'),
                                       round(avg('main.humidity'), 2).alias('avg_humidity'), min('main.humidity').alias('min_humidity'), max('main.humidity').alias('max_humidity'),
                                       round(avg('main.pressure'), 2).alias('avg_pressure'), min('main.pressure').alias('min_pressure'), max('main.pressure').alias('max_pressure'),
                                       round(avg('main.temp'), 2).alias('avg_temp'), min('main.temp_min').alias('min_temp'), max('main.temp_max').alias('max_temp'),
                                       round(avg('wind.deg'), 2).alias('avg_deg'), min('wind.deg').alias('min_deg'), max('wind.deg').alias('max_deg'),
                                       round(avg('wind.speed'), 2).alias('avg_speed'), min('wind.speed').alias('min_speed'), max('wind.speed').alias('max_speed')
                                      )

# prepare mapping dataframe - db table
catalog = "".join("""
    {
        "table": {
            "namespace": "default",
            "name": "climate"
        },
        "rowkey": "unix_date",
        "columns": {
            "unix_date": {"cf": "rowkey", "col": "unix_date", "type": "long"},
            "avg_clouds": {"cf": "weather", "col": "avg_clouds", "type": "double"},
            "min_clouds": {"cf": "weather", "col": "min_clouds", "type": "long"},
            "max_clouds": {"cf": "weather", "col": "max_clouds", "type": "long"},
            "avg_humidity": {"cf": "weather", "col": "avg_humidity", "type": "double"},
            "min_humidity": {"cf": "weather", "col": "min_humidity", "type": "long"},
            "max_humidity": {"cf": "weather", "col": "max_humidity", "type": "long"},
            "avg_pressure": {"cf": "weather", "col": "avg_pressure", "type": "double"},
            "min_pressure": {"cf": "weather", "col": "min_pressure", "type": "long"},
            "max_pressure": {"cf": "weather", "col": "max_pressure", "type": "long"},
            "avg_temp": {"cf": "temperature", "col": "avg_temp", "type": "double"},
            "min_temp": {"cf": "temperature", "col": "min_temp", "type": "double"},
            "max_temp": {"cf": "temperature", "col": "max_temp", "type": "double"},
            "avg_deg": {"cf": "wind", "col": "avg_deg", "type": "double"},
            "min_deg": {"cf": "wind", "col": "min_deg", "type": "long"},
            "max_deg": {"cf": "wind", "col": "max_deg", "type": "long"},
            "avg_speed": {"cf": "wind", "col": "avg_speed", "type": "double"},
            "min_speed": {"cf": "wind", "col": "min_speed", "type": "long"},
            "max_speed": {"cf": "wind", "col": "max_speed", "type": "long"}
        }
    }
""".split())

# write on hbase
aggrDF.write \
  .options(catalog=catalog,newTable="5") \
  .format("org.apache.spark.sql.execution.datasources.hbase") \
  .save()

spark.stop