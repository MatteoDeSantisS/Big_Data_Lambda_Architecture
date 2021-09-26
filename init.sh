#!/bin/bash
----- Preparazione -----
sleep 15
echo ----- Creazione del topic ----
$KAFKA_HOME/bin/kafka-topics.sh --create --topic sensors-data --zookeeper localhost:2181 --partitions 1 --replication-factor 1

echo ----- Creazione delle tabelle cassandra -----
cqlsh -f ./cassandra/schema_tables.cql

echo ----- Avvio del kafka producer ----- &
python3 kafka/kafka-producer.py &

echo ----- Avvio dello streamer processor ----- &
./script/stream-processor.sh & 

echo ----- Avvio del daily streamer ----- &
./script/daily-streamer.sh & 

echo ----- Avvio script per viste mensili ----- &
./script/month-script.sh & 

echo ----- Avvio script per viste annuali ----- &
./script/year-script.sh &

#Avvio del server flask 
gnome-terminal -- /bin/sh -c 'echo ----- Avvio Flask Server ---- non chiudere questa console -----;cd dashboard; flask run'
