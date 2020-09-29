KAFKA

1) Installazione Docker Compose (https://docs.docker.com/compose/install/)

> git clone https://github.com/wurstmeister/kafka-docker.git 
> cd kafka-docker

# Update KAFKA_ADVERTISED_HOST_NAME inside 'docker-compose.yml',
# For example, set it to 172.17.0.1
> gedit docker-compose.yml 
> docker-compose up -d

# Optional - Scale the cluster by adding more brokers (Will start a single zookeeper instance)
> docker-compose scale kafka=3

# You can check the proceses running with:
> docker-compose ps

# Destroy the cluster when you are done with it
> docker-compose stop

$KAFKA_HOME/bin/kafka-topics.sh --create --topic climate \
--partitions 4 --replication-factor 2 \
--bootstrap-server `broker-list.sh`

// EXPOSE KAFKA BROKER
# From your terminal run:
> docker exec -i -t -u root $(docker ps | grep docker_kafka | cut -d' ' -f1) /bin/bash
# $(docker ps | grep docker_kafka | cut -d' ' -f1) - Will return the docker process ID of the Kafka Docker running so you can acces it

# Esecuzione di Kafka in modalità d'inoltro
1. sudo docker ps | grep docker_kafka | cut -d' ' -f1
2. sudo docker exec -i -t -u root 8938b25953ae /bin/bash

# Create a topic
bash> $KAFKA_HOME/bin/kafka-topics.sh --create --partitions 4 --bootstrap-server kafka:9092 --topic climate

# Create a consumer
bash> $KAFKA_HOME/bin/kafka-console-consumer.sh --from-beginning --bootstrap-server kafka:9092 --topic=test 

# Create a producer
bash> $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic=test



Cassandra
1. Creazione della cartella in Docker,
2. creazione data/node1 e data/node2 nello stesso folder 
2. > docker pull Cassandra
3. creazione docker-compose.yml
4. > docker-compose up -d
Check Funzionamento
5. > docker exec -it cas1 nodetool status
   > docker exec -it cas2 nodetool status
6. Accesso Cassandra CQL : docker exec -it cas2 cqlsh

