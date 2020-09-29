# bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0-beta --conf spark.cassandra.connection.host=127.0.0.1 new-streaming-processor.py
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import sys
import json

MINUTE = 60
FIVE_MINUTES = 300
TEN_MINUTES = 600
THIRTY_MINUTES = 1800
HOUR = 3600
SIX_HOUR = 21600
TWELVE_HOURS = 43200
DAY = 86400

def process_rdd(time, rdd):
    print("--- %s --" % str(time))
    # try:
    rdd_to_cassandra_row = rdd.map(lambda x : (x[0], x[1][1], x[1][4], x[1][5], x[1][6], x[1][7]))
    df = rdd_to_cassandra_row.toDF(["location", "timestamp", "temperature", "wind", "humidity", "weather"])
    df.write.format("org.apache.spark.sql.cassandra").mode('append')\
        .options(table="location_data", keyspace="climate", ttl=DAY)\
        .save()
    print("Saved")
    # except:
    # 	e = sys.exc_info()[0]
    # 	print("Error: %s" % e)

def saveTrend(rdd):
    if rdd.isEmpty() == False:
        print("Saving trends:")
        print(rdd.take(10))
        # 0 - minutes, 1 - actual_temperature, 2 - temperature_trend, 3 - actual_wind, 4 - wind_trend, 5 - actual_humidity, 6 - humidity_trend, 7 - weather, 8 - location
        # 60 224.18 0.0 5.36 0.0 59.0 0.0 'Clouds' 'Antarctica'
        rdd_to_cassandra_trends_row = rdd.map(lambda x : (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8]))

        # Create DataFrame from RDD
        df = rdd_to_cassandra_trends_row.toDF(["interval", "actual_temperature", "temperature_trend", "actual_wind", "wind_trend", "actual_humidity", "humidity_trend", "weather", "location"])
        df.show(n=1)
        
        # Save DataFrame
        df.write.format("org.apache.spark.sql.cassandra").mode('append')\
            .options(table="trends_view", keyspace="climate", ttl=DAY)\
            .save()

# First interval_ends is the last interval
def calculate_percentage_variation(interval_ends, minutes):

    print("Interval:" + str(minutes))
    print(interval_ends[0])
    print("Location:" + interval_ends[0][0])
    print(interval_ends[1])

    # Calculate Temperature Trend
    temp_start = float(interval_ends[1][4])
    actual_temperature = float(interval_ends[0][4])
    temperature_trend = ((actual_temperature - temp_start) / temp_start ) * 100
    print(str(minutes) + '- Temp Trend:' + str(temperature_trend))
    print(str(minutes) + '- Temp now:' + str(actual_temperature))

    # Calculate Wind Trend
    wind_start = float(interval_ends[1][5])
    actual_wind = float(interval_ends[0][5])
    wind_trend = ((actual_wind - wind_start) / wind_start ) * 100
    print(str(minutes) + '- Wind trend:' + str(wind_trend))
    print(str(minutes) + '- Wind now:' + str(actual_wind))

    # Calculate Humidity Trend
    humidity_start = float(interval_ends[1][6])
    actual_humidity = float(interval_ends[0][6])
    humidity_trend = (actual_humidity - humidity_start)
    print(str(minutes) + '- humidity trend:' + str(humidity_trend))
    print(str(minutes) + '- humidity now:' + str(actual_humidity))

    location = interval_ends[0][0]
    weather = interval_ends[0][7]

    print("Saved: " + str([minutes, actual_temperature, temperature_trend,  actual_wind, wind_trend, actual_humidity, humidity_trend, weather, location]))
    return (minutes, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, weather, location)
    
def get_last_record(location, x, y):

    if x[0] != location and y[0] != location:
        return x

    if x[0] == location and y[0] != location:
        return x
    
    if x[0] != location and y[0] == location:
        return y

    if x[1] > y[1]:
        return x
    else :
        return y

def get_first_record(location, x, y):

    if x[0] != location and y[0] != location:
        return x

    if x[0] == location and y[0] != location:
        return x
    
    if x[0] != location and y[0] == location:
        return y

    if x[1] > y[1]:
        return x
    else :
        return y

    if x[1] < y[1]:
        # print(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])
        return x
    else :
        # print(y[0],y[1], y[2], y[3], y[4], y[5], y[6], y[7])
        return y

# main
sparkSess = SparkSession.builder \
    .appName("Locations Climate Streaming Processor") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()
sc = sparkSess.sparkContext
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)

# Check Table
sparkSess.read.format("org.apache.spark.sql.cassandra").options(table='location_data', keyspace='climate').load().show()

# Start DataStream
dataStream = ssc.socketTextStream("127.0.0.1",9999)

# Get Values
valuesRDD = dataStream.map(lambda line : line[1:len(line)-1]).map(lambda line : line.replace(" ", "")).map(lambda line : line.split(","))

valuesRDD.foreachRDD(process_rdd)

# Get Trend

def printRdd(rdd):
    print(rdd.take(10))

# TEST 1 minutes
rdd_antarctica_first_60 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Antarctica'", x,y), None, MINUTE, MINUTE)
rdd_antarctica_first_60_map = rdd_antarctica_first_60.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_first_60 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Arctic'", x,y), None, MINUTE, MINUTE)
rdd_arctic_first_60_map = rdd_arctic_first_60.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))


rdd_antarctica_last_60 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Antarctica'", x,y), None, MINUTE, MINUTE)
rdd_antarctica_last_60_map = rdd_antarctica_last_60.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_last_60 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Arctic'", x,y), None, MINUTE, MINUTE)
rdd_arctic_last_60_map = rdd_arctic_last_60.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_antarctica_60_trends = rdd_antarctica_last_60_map.join(rdd_antarctica_first_60_map).map(lambda x: (calculate_percentage_variation(x[1], MINUTE)))
rdd_antarctica_60_trends.foreachRDD(saveTrend)

rdd_arctic_60_trends = rdd_arctic_last_60_map.join(rdd_arctic_first_60_map).map(lambda x: (calculate_percentage_variation(x[1], MINUTE)))
rdd_arctic_60_trends.foreachRDD(saveTrend)

# 5 minutes
rdd_antarctica_first_300 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Antarctica'", x,y), None, FIVE_MINUTES, MINUTE)
rdd_antarctica_first_300_map = rdd_antarctica_first_300.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_first_300 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Arctic'", x,y), None, FIVE_MINUTES, MINUTE)
rdd_arctic_first_300_map = rdd_arctic_first_300.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))


rdd_antarctica_last_300 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Antarctica'", x,y), None, FIVE_MINUTES, MINUTE)
rdd_antarctica_last_300_map = rdd_antarctica_last_300.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_last_300 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Arctic'", x,y), None, FIVE_MINUTES, MINUTE)
rdd_arctic_last_300_map = rdd_arctic_last_300.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_antarctica_300_trends = rdd_antarctica_last_300_map.join(rdd_antarctica_first_300_map).map(lambda x: (calculate_percentage_variation(x[1], FIVE_MINUTES)))
rdd_antarctica_300_trends.foreachRDD(saveTrend)

rdd_arctic_300_trends = rdd_arctic_last_300_map.join(rdd_arctic_first_300_map).map(lambda x: (calculate_percentage_variation(x[1], FIVE_MINUTES)))
rdd_arctic_300_trends.foreachRDD(saveTrend)

# 10 minutes
rdd_antarctica_first_600 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Antarctica'", x,y), None, TEN_MINUTES, MINUTE)
rdd_antarctica_first_600_map = rdd_antarctica_first_600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_first_600 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Arctic'", x,y), None, TEN_MINUTES, MINUTE)
rdd_arctic_first_600_map = rdd_arctic_first_600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))


rdd_antarctica_last_600 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Antarctica'", x,y), None, TEN_MINUTES, MINUTE)
rdd_antarctica_last_600_map = rdd_antarctica_last_600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_last_600 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Arctic'", x,y), None, TEN_MINUTES, MINUTE)
rdd_arctic_last_600_map = rdd_arctic_last_600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_antarctica_600_trends = rdd_antarctica_last_600_map.join(rdd_antarctica_first_600_map).map(lambda x: (calculate_percentage_variation(x[1], TEN_MINUTES)))
rdd_antarctica_600_trends.foreachRDD(saveTrend)

rdd_arctic_600_trends = rdd_arctic_last_600_map.join(rdd_arctic_first_600_map).map(lambda x: (calculate_percentage_variation(x[1], TEN_MINUTES)))
rdd_arctic_600_trends.foreachRDD(saveTrend)

# 30 minutes
rdd_antarctica_first_1800 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Antarctica'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_antarctica_first_1800_map = rdd_antarctica_first_1800.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_first_1800 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Arctic'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_arctic_first_1800_map = rdd_arctic_first_1800.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))


rdd_antarctica_last_1800 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Antarctica'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_antarctica_last_1800_map = rdd_antarctica_last_1800.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_last_1800 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Arctic'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_arctic_last_1800_map = rdd_arctic_last_1800.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_antarctica_1800_trends = rdd_antarctica_last_1800_map.join(rdd_antarctica_first_1800_map).map(lambda x: (calculate_percentage_variation(x[1], THIRTY_MINUTES)))
rdd_antarctica_1800_trends.foreachRDD(saveTrend)

rdd_arctic_1800_trends = rdd_arctic_last_1800_map.join(rdd_arctic_first_1800_map).map(lambda x: (calculate_percentage_variation(x[1], THIRTY_MINUTES)))
rdd_arctic_1800_trends.foreachRDD(saveTrend)

# 1 Hour
rdd_antarctica_first_3600 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Antarctica'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_antarctica_first_3600_map = rdd_antarctica_first_3600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_first_3600 = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Arctic'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_arctic_first_3600_map = rdd_arctic_first_3600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))


rdd_antarctica_last_3600 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Antarctica'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_antarctica_last_3600_map = rdd_antarctica_last_3600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_last_3600 = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Arctic'", x,y), None, THIRTY_MINUTES, MINUTE)
rdd_arctic_last_3600_map = rdd_arctic_last_3600.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_antarctica_3600_trends = rdd_antarctica_last_3600_map.join(rdd_antarctica_first_3600_map).map(lambda x: (calculate_percentage_variation(x[1], THIRTY_MINUTES)))
rdd_antarctica_3600_trends.foreachRDD(saveTrend)

rdd_arctic_3600_trends = rdd_arctic_last_3600_map.join(rdd_arctic_first_3600_map).map(lambda x: (calculate_percentage_variation(x[1], THIRTY_MINUTES)))
rdd_arctic_3600_trends.foreachRDD(saveTrend)

# 6 Hour
rdd_antarctica_first_6_h = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Antarctica'", x,y), None, SIX_HOUR, MINUTE)
rdd_antarctica_first_6_h_map = rdd_antarctica_first_6_h.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_first_6_h = valuesRDD.reduceByWindow(lambda x,y: get_first_record("'Arctic'", x,y), None, SIX_HOUR, MINUTE)
rdd_arctic_first_6_h_map = rdd_arctic_first_6_h.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))


rdd_antarctica_last_6_h = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Antarctica'", x,y), None, SIX_HOUR, MINUTE)
rdd_antarctica_last_6_h_map = rdd_antarctica_last_6_h.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_arctic_last_6_h = valuesRDD.reduceByWindow(lambda x,y: get_last_record("'Arctic'", x,y), None, SIX_HOUR, MINUTE)
rdd_arctic_last_6_h_map = rdd_arctic_last_6_h.map(lambda x : (x[0], (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7])))

rdd_antarctica_6_h_trends = rdd_antarctica_last_6_h_map.join(rdd_antarctica_first_6_h_map).map(lambda x: (calculate_percentage_variation(x[1], SIX_HOUR)))
rdd_antarctica_6_h_trends.foreachRDD(saveTrend)

rdd_arctic_6_h_trends = rdd_arctic_last_6_h_map.join(rdd_arctic_first_6_h_map).map(lambda x: (calculate_percentage_variation(x[1], SIX_HOUR)))
rdd_arctic_6_h_trends.foreachRDD(saveTrend)

# Start
ssc.start()
ssc.awaitTermination()