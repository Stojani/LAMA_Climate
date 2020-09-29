#!/usr/bin/python3

# http://localhost:9870/webhdfs/v1/output/myresults.csv/part-*.csv?op=OPEN

from pyspark.sql import SparkSession

# create sparksession
spark = SparkSession.builder \
    .master("local") \
    .appName("climate_batch_historical_measures") \
    .getOrCreate()


#----------------------- DEFINE HBASE CATALOG ------------------

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


#----------------------- READ FROM HBASE ------------------

readDF = spark.read \
    .options(catalog=catalog) \
    .format('org.apache.spark.sql.execution.datasources.hbase') \
    .load()

#----------------------- PREPROCESSING ------------------

from pyspark.sql.functions import from_unixtime, to_date
readDF = readDF.withColumn('string_date', from_unixtime('unix_date',"yyyy-MM-dd"))
readDF = readDF.withColumn('date', to_date('string_date', 'yyyy-MM-dd'))

from pyspark.sql.functions import year, month
readDF = readDF.withColumn('year', year('date'))
readDF = readDF.withColumn('month', month('date'))

# Antarctica Seasons:
# winter: [March - October)
# summer: [October - March)
from pyspark.sql.functions import when, col
readDF = readDF.withColumn('season', when((col('month') >= 3) & (col('month') < 10), 'winter').otherwise('summer'))


#----------------------- AGGREGATION ------------------

from pyspark.sql.functions import avg, min, max, round
seasonDF = readDF.groupBy('year', 'season').agg(
    round(avg('avg_temp'), 2).alias('avg_temp'),max('max_temp').alias('max_temp'), min('min_temp').alias('min_temp'),
    round(avg('avg_speed'), 2).alias('avg_speed'),max('max_speed').alias('max_speed'), min('min_speed').alias('min_speed'),
    round(avg('avg_humidity'), 2).alias('avg_humidity'),max('max_humidity').alias('max_humidity'), min('min_humidity').alias('min_humidity'))


#----------------------- CREATE VIEW ------------------

seasonDF.createOrReplaceTempView("year_season")
#seasonDF.createGlobalTempView("year_season")

#----------------------- READ FROM VIEW ---------------

query_max_temp = spark.sql("SELECT season, year, 'Max Temperature' AS measure, max_temp AS value FROM year_season WHERE season = 'summer' ORDER BY max_temp LIMIT 1")
query_min_temp = spark.sql("SELECT season, year, 'Min Temperature' AS measure, min_temp AS value FROM year_season WHERE season = 'summer' ORDER BY min_temp LIMIT 1")

query_max_wind = spark.sql("SELECT season, year, 'Max Wind Speed' AS measure, max_speed AS value FROM year_season WHERE season = 'summer' ORDER BY max_speed LIMIT 1")
query_min_wind = spark.sql("SELECT season, year, 'Min Wind Speed' AS measure, min_speed AS value FROM year_season WHERE season = 'summer' ORDER BY min_speed LIMIT 1")

query_max_humidity = spark.sql("SELECT season, year, 'Max Humidity' AS measure, max_humidity AS value FROM year_season WHERE season = 'summer' ORDER BY max_humidity LIMIT 1")
query_min_humidity = spark.sql("SELECT season, year, 'Min Humidity' AS measure, min_humidity AS value FROM year_season WHERE season = 'summer' ORDER BY min_humidity LIMIT 1")

#test = spark.sql("SELECT year, season, avg_temp FROM global_temp.year_season WHERE season = 'summer'")


#----------------------- MEASURES UNION ---------------

tempDF = query_max_temp.union(query_min_temp)

windDF = query_max_wind.union(query_min_wind)

humidityDF = query_max_humidity.union(query_min_humidity)

finalDF = tempDF.union(windDF).union(humidityDF)

#----------------------- WRITE TO FILE ---------------

# Save file to HDFS
finalDF.coalesce(1).write.format('csv').mode('overwrite').option('sep',',').save("/output/measures", header='true')

spark.stop