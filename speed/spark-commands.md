**Avvio Pipeline**
1. Avvio consumer in Python/consumer.py
2. Avvio in Spark/bin spark-submit stream-processor.py


**In caso di problemi**
> fuser -n tcp -k 9001 
> sudo lsof -i -P -n | grep LISTEN 
