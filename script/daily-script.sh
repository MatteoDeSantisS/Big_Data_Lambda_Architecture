#!/bin/bash

#Avvia il Daily Streamer
$SPARK_HOME/bin/spark-submit --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,com.datastax.cassandra:cassandra-driver-core:4.0.0" --master local[4] streaming/daily-streamer.py


