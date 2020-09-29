#!/usr/bin/python3


from pyspark.sql import SparkSession

# create sparksession
spark = SparkSession.builder \
    .master("local") \
    .appName("climate_batch_stream_aggregation") \
    .getOrCreate()

#----------------------- READ FROM HBASE - Table: batch_climate_streams ------------------

stream_catalog = "".join("""
    {
        "table": {
            "namespace": "default",
            "name": "batch_climate_streams"
        },
        "rowkey": "unix_date",
        "columns": {
            "unix_date": {"cf": "rowkey", "col": "unix_date", "type": "long"},
            "clouds": {"cf": "weather", "col": "clouds", "type": "double"},
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

readDF = spark.read \
    .options(catalog=stream_catalog) \
    .format('org.apache.spark.sql.execution.datasources.hbase') \
    .load()

#----------------------- PREPROCESSING ------------------

from pyspark.sql.functions import from_unixtime, to_date, unix_timestamp
readDF = readDF.withColumn('string_date', from_unixtime('unix_date',"yyyy-MM-dd"))
readDF = readDF.withColumn('date', to_date('string_date', 'yyyy-MM-dd'))
readDF = readDF.withColumn('unix_date', unix_timestamp('date',"yyyy-MM-dd"))

#----------------------- AGGREGATION ------------------

from pyspark.sql.functions import avg, min, max, round
aggrDF = readDF.groupBy('unix_date').agg(round(avg('clouds'), 2).alias('avg_clouds'), min('clouds').alias('min_clouds'), max('clouds').alias('max_clouds'),
                                       round(avg('humidity'), 2).alias('avg_humidity'), min('humidity').alias('min_humidity'), max('humidity').alias('max_humidity'),
                                       round(avg('pressure'), 2).alias('avg_pressure'), min('pressure').alias('min_pressure'), max('pressure').alias('max_pressure'),
                                       round(avg('temp'), 2).alias('avg_temp'), min('temp_min').alias('min_temp'), max('temp_max').alias('max_temp'),
                                       round(avg('deg'), 2).alias('avg_deg'), min('deg').alias('min_deg'), max('deg').alias('max_deg'),
                                       round(avg('speed'), 2).alias('avg_speed'), min('speed').alias('min_speed'), max('speed').alias('max_speed'))


#----------------------- WRITE TO HBASE - Main Table: climate ----------------
main_catalog = "".join("""
    {
        "table": {
            "namespace": "default",
            "name": "climate"
        },
        "rowkey": "unix_date",
        "columns": {
            "unix_date": {"cf": "rowkey", "col": "unix_date", "type": "long"},
            "avg_clouds": {"cf": "weather", "col": "avg_clouds", "type": "double"},
            "min_clouds": {"cf": "weather", "col": "min_clouds", "type": "double"},
            "max_clouds": {"cf": "weather", "col": "max_clouds", "type": "double"},
            "avg_humidity": {"cf": "weather", "col": "avg_humidity", "type": "double"},
            "min_humidity": {"cf": "weather", "col": "min_humidity", "type": "double"},
            "max_humidity": {"cf": "weather", "col": "max_humidity", "type": "double"},
            "avg_pressure": {"cf": "weather", "col": "avg_pressure", "type": "double"},
            "min_pressure": {"cf": "weather", "col": "min_pressure", "type": "double"},
            "max_pressure": {"cf": "weather", "col": "max_pressure", "type": "double"},
            "avg_temp": {"cf": "temperature", "col": "avg_temp", "type": "double"},
            "min_temp": {"cf": "temperature", "col": "min_temp", "type": "double"},
            "max_temp": {"cf": "temperature", "col": "max_temp", "type": "double"},
            "avg_deg": {"cf": "wind", "col": "avg_deg", "type": "double"},
            "min_deg": {"cf": "wind", "col": "min_deg", "type": "double"},
            "max_deg": {"cf": "wind", "col": "max_deg", "type": "double"},
            "avg_speed": {"cf": "wind", "col": "avg_speed", "type": "double"},
            "min_speed": {"cf": "wind", "col": "min_speed", "type": "double"},
            "max_speed": {"cf": "wind", "col": "max_speed", "type": "double"}
        }
    }
""".split())


aggrDF.write \
  .options(catalog=main_catalog,newTable="5") \
  .format("org.apache.spark.sql.execution.datasources.hbase") \
  .mode("append") \
  .save()


spark.stop