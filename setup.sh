#!/bin/bash

#Avvia Hadoop
echo ----- Setup Hadoop -----
hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh

echo ----- Setup Cassandra -----
#Avvia Cassandra
$CASSANDRA_HOME/bin/cassandra &

echo ----- Setup Zookeeper ----- &
#Avvia Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

#Avvia Kafka
gnome-terminal -- /bin/sh -c 'echo ----- Setup Kafka -----; sleep 60; $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties'




