#!/usr/bin/python3
from kafka import KafkaConsumer
from json import loads
from datetime import datetime
import socket
import sys
import time

consumer = KafkaConsumer(
   'climate-locations',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group-1',
    value_deserializer=lambda m: loads(m.decode('utf-8')),
    # bootstrap_servers='ec2_instance_here:9092'
    bootstrap_servers='127.0.0.1:9092')

def send_message_to_spark(tcp_connection):
    for message in consumer:
        try:
            
            # date = message.value["datetime"][1:11]
            # time = message.value["datetime"][12:20]
            timestamp = message.value["datetime"]
            timeObj = time.localtime(timestamp)
            hour = timeObj.tm_hour
            minute = timeObj.tm_min
            temperature = message.value["temp"]
            wind = message.value["wind"]
            humidity = message.value["humidity"]
            weather = message.value["weather"]
            location = message.value["location"]
            formatted_output = (location, timestamp, hour, minute, temperature, wind, humidity, weather)
            value = str(formatted_output)+'\n'
            now = datetime.now().time() # time object
            print("Sent > ", now)
            tcp_connection.send(value.encode())
        except:
            e = sys.exc_info()[0]
            print("Error:", e)    

TCP_IP = "127.0.0.1"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting climate data.")
send_message_to_spark(conn)