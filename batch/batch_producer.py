#!/usr/bin/python3

from kafka import KafkaProducer
import json
import urllib.request
from time import sleep


producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], 
                         value_serializer = lambda v: json.dumps(v).encode('utf-8'))

# considerazioni:
# sembra che le misure richieste vengano eseguite ogni 10 minuti da openweathermap
# per l'analisi batch effettuiamo le misure ogni 30/60 minuti e le salviamo in una tabella apposita 'batch_climate_streams' del db,
# successivamente queste misure verranno aggregate giornalmente registrando i valori di interesse (temp massima/minima/media del giorno ecc...)
# la tabella 'batch_climate_streams' viene usata per leggere i dati registrati per poi aggregarli, 
# infine i dati letti dalla tabella possono essere rimossi

while True:

    url = 'https://api.openweathermap.org/data/2.5/weather?q=antarctica&units=metric&appid=<chiave-privata>'
    response = urllib.request.urlopen(url)
    res = response.read().decode('utf-8')

    #print(res)
    producer.send('batch-climate', res)
    producer.flush()

    sleep(3600)