from kafka import KafkaProducer
import urllib.request
import urllib.parse
import threading
import sys
from datetime import datetime
from json import dumps, loads
import time

city = sys.argv[1]
secondsInterval = int(sys.argv[2])
aws = bool(sys.argv[3])

ec2_instance_name = ''

bootstrap_servers = '127.0.0.1:9093'
if aws == True:
    bootstrap_servers = ec2_instance_name+':9092'

# API ID of OpenWeather 
api_key = ""

url = 'https://api.openweathermap.org/data/2.5/weather?q='+city+'&appid='+api_key

# producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer = KafkaProducer(
   value_serializer=lambda m: dumps(m).encode('utf-8'), 
   bootstrap_servers='127.0.0.1:9092')


# code from http://stackoverflow.com/a/14035296/4592067
def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t


# Get data from API and spam on kafka channel
def readWeather():
    response = urllib.request.urlopen(url)
    # now = datetime.now(), dumps(now, indent=4, sort_keys=True, default=str)
    now = time.time()
    # print(response.read().decode('utf-8'))
    # producer.send("climate", response)
    # producer.send("climate", value={"data": response.read().decode('utf-8')})
    value = loads(response.read()) #.decode('utf-8')
    temperature = value["main"]["temp"] #data.main.temp
    wind = value["wind"]["speed"] #data.wind.speed
    humidity = value["main"]["humidity"] #data.humidity
    
    # print("temperature", temperature)
    # print("wind", wind)
    # print("humidity", humidity)

    # Actual weather
    weather = value["weather"][0]["main"]

    # Place name
    location = value["name"]

    producer.send("climate-locations", value={"location": location, "datetime" : int(now), "temp" : temperature, "wind": wind, "humidity": humidity, "weather" : weather})
    print('> Sent at' +str(now) + " - {location: "+ location + ", temp: "+ str(temperature) + ", wind: "+ str(wind)+", humidity: "+ str(humidity)+ " }")

# set up the timer
timer = set_interval(readWeather, secondsInterval)

# {
#     'coord': {'lon': 16.41, 'lat': -78.16}, 
#     'weather': [
#         {'id': 803, 'main': 'Clouds', 'description': 'broken clouds', 'icon': '04d'}
#     ], 
#     'base': 'stations', 
#     'main': {
#         'temp': 223.19, 
#         'feels_like': 214.78, 
#         'temp_min': 223.19, 
#         'temp_max': 223.19, 'pressure': 1030, 'humidity': 52, 'sea_level': 1030, 'grnd_level': 646
#     }, 
#     'visibility': 10000, 
#     'wind': {'speed': 6.31, 'deg': 355}, 
#     'clouds': {'all': 55}, 'dt': 1601023968, 
#     'sys': {'sunrise': 1601007017, 'sunset': 1601054494}, 
#     'timezone': 7200, 
#     'id': 6255152, 
#     'name': 'Antarctica', 
#     'cod': 200
# }
