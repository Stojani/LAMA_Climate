from flask import Flask, render_template, request, url_for, jsonify
from cassandra.cluster import Cluster
from datetime import datetime
from json import JSONEncoder
import csv

# Init web application from this file
app = Flask(__name__)

def cassandra_connection():

    """
        Connection object for Cassandra
        :return: session, cluster
    """
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS climate
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }
        """)
    session.set_keyspace('climate')
    session.execute("""
        CREATE TABLE IF NOT EXISTS location_data (timestamp int, temperature float, wind float, humidity float, location text, weather text, PRIMARY KEY(location, timestamp))
        """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS trends_view (interval text, actual_temperature float, temperature_trend float, actual_wind float, wind_trend float, actual_humidity float, humidity_trend float, location text, weather text, PRIMARY KEY(location, interval))
        """)
    return session, cluster

session, cluster = cassandra_connection()

class MyEncoder(JSONEncoder):
        def default(self, o):
            return o.__dict__    


class LocationTrend():
    def __init__(self, interval, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, location, weather):
        self.interval = interval
        self.actual_temperature = actual_temperature
        self.temperature_trend = round(temperature_trend, 2)
        self.actual_wind = round(actual_wind,2)
        self.wind_trend = round(wind_trend,2)
        self.actual_humidity = actual_humidity
        self.humidity_trend = round(humidity_trend, 2)
        self.location = location,
        self.weather = weather

def parseInterval(interval):
    switcher = {
        "60": "1 m",
        "300": "5 m",
        "600": "10 m",
        "1800": "30 m",
        "3600": "1 h",
        "21600": "6 h"
    }
    return switcher.get(interval, "Invalid interval")

def parseLocation(location):
    print(location)
    parsed = location.replace("(", "").replace(")", "").replace(".", "").replace("\'", "")
    return parsed

def readTrendData():
    climate_view_data = []

    # rows = session.execute('SELECT * FROM trends_view ') #ORDER BY interval ASC

    rows = session.execute('SELECT interval, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, location, weather FROM trends_view ')
    for (interval, actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, location, weather) in rows:
        
        newTrend = LocationTrend(parseInterval(interval), actual_temperature, temperature_trend, actual_wind, wind_trend, actual_humidity, humidity_trend, parseLocation(location), weather)
        climate_view_data.append(newTrend)
    return climate_view_data

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/climate/view")
def getAntarticaClimateView():
    return render_template('trends.html', trends=readTrendData(), time=datetime.now())

@app.route("/climate/test")
def getAntarticaClimateTest():
    climate_view_data = []
    rows = session.execute('SELECT * FROM location_data')

    for (location, timestamp, wind, temperature, weather, humidity) in rows:
        print(location, timestamp, wind, temperature, weather, humidity)
        data = {
            'location' : location,
            'time': timestamp,
            'temperature': temperature,
            'wind': wind,
            'humidity': humidity, 
            'weather' : weather
        }
        climate_view_data.append(data)
    return jsonify(climate_view_data)


@app.route("/climate/view/delete")
def deleteViewsTable():
    session.execute("DROP TABLE trends_view")
    return render_template("index.html") 

@app.route("/climate/view/json")
def getTrendsDataJson():
    return jsonify(MyEncoder().encode(readTrendData()))

def getCurrentSeason():
    today = datetime.now()
    season= ''
    if (today.month >=3 and today.month < 10):
        season='winter'
    else:
        season='summer'
    return season

def readHistoricalMeasures(season):
    filename = 'data/historical_{}_measures.csv'.format(season)
    historical_measures = []

    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hist_measure = [row['season'], row['year'], row['measure'], row['value']]
            historical_measures.append(hist_measure)

    return historical_measures

@app.route("/climate/historical/measures")
def getHistoricalAntarticaClimateView():
    curr_season = getCurrentSeason()
    return render_template('historical_measures.html', measures=readHistoricalMeasures(curr_season), season=curr_season)

def readHistoricalTrends(season):
    filename = 'data/historical_{}_trends.csv'.format(season)
    historical_trends = []

    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            hist_trend = [row['season'], row['year'], row['measure'], row['value'], row['trend']]
            historical_trends.append(hist_trend)

    return historical_trends

@app.route("/climate/historical/trends")
def getHistoricalTrends():
    curr_season = getCurrentSeason()
    return render_template('historical_trends.html', trends=readHistoricalTrends(curr_season), season=curr_season)

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

@app.route('/shutdown', methods=['GET'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'