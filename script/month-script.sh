#!/bin/bash

# 
sleep 300
while true
do 
    $SPARK_HOME/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1" --master local[4] batch/month-most-polluted.py
    echo ---- Script di creazione delle view mensili [riparte ogni 300s] --- non chiudere questa console -----
    sleep 300
done