from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max

spark_session = SparkSession \
    .builder \
    .appName("Pollution_Streaming_Analyzer") \
    .config("spark.cassandra.connection.host", "localhost:9042") \
    .getOrCreate()

month_most_polluted = spark_session \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="stuff", table="pollution") \
    .load()

city_data = month_most_polluted \
    .select('city', 'state', 'year', 'month', 'no2mean', 'no2aqi')

by_state = month_most_polluted \
    .groupBy('month', 'year', 'state', 'no2aqi', 'no2mean') \
    .agg(avg('no2mean'), max('no2aqi'))\
    .orderBy('no2aqi')\
    .select('month', 'year', 'state', 'no2aqi', 'no2mean')

city_data.createOrReplaceTempView("CityData")
by_state.createOrReplaceTempView("MostPollutedByState")

#joinDf = spark_session.sql("select * from MostPollutedByState m, CityData cd where m.state == cd.state & m.year == cd.year \
#                           & m.month == cd.month & m.no2mean == cd.no2mean & m.no2aqi == cd.no2aqi")

joinDf = by_state.join(city_data, (by_state.state == city_data.state) & 
                                    (by_state.year == city_data.year) &
                                    (by_state.month == city_data.month) &
                                    (by_state.no2mean == city_data.no2mean) &
                                    (by_state.no2aqi == city_data.aqi))\
                .select('city','state','month','year','no2mean','no2aqi')

query = joinDf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="stuff", table="month_most_polluted")\
    .save()

