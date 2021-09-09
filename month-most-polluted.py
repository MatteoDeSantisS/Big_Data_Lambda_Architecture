from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg, get_json_object,month, year, unix_timestamp
from pyspark.sql.types import TimestampType 

import uuid

spark_session = SparkSession \
    .builder \
    .appName("Pollution_Streaming_Analyzer") \
    .config("spark.cassandra.connection.host", "localhost:9042") \
    .getOrCreate()

dataframe = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensors-data") \
    .option("startingOffsets", "earliest") \
    .load()

dataframe = dataframe.selectExpr("CAST(value AS STRING)")

dataframe = dataframe.select(
    get_json_object(dataframe.value, '$.State').alias('state'),
    get_json_object(dataframe.value, '$.County').alias('county'),
    get_json_object(dataframe.value, '$.City').alias('city'),
    get_json_object(dataframe.value, '$.Date Local').alias('datelocal'),
    get_json_object(dataframe.value, '$.NO2 Mean').alias('no2mean'),
    get_json_object(dataframe.value, '$.NO2 AQI').alias('no2aqi'),
    get_json_object(dataframe.value, '$.SO2 Mean').alias('so2mean'),
    get_json_object(dataframe.value, '$.SO2 AQI').alias('so2aqi'),
    get_json_object(dataframe.value, '$.CO Mean').alias('comean'),
    get_json_object(dataframe.value, '$.CO AQI').alias('coaqi'),
    get_json_object(dataframe.value, '$.O3 Mean').alias('o3mean'),
    get_json_object(dataframe.value, '$.O3 AQI').alias('o3aqi'),
    get_json_object(dataframe.value, '$.Timestamp').alias('ts')
)




views_state = dataframe \
    .withColumn("month",month("datelocal"))\
    .withColumn("year",year("datelocal"))\
    .withColumn('eventdate', unix_timestamp(col('ts'), "yyyy-MM-dd HH:mm:ss").cast(TimestampType())) \
    .withWatermark("eventdate", '2 minutes') \
    .groupBy("state", "month", "year","eventdate") \
    .agg(avg('no2mean').alias('no2avg')) \
        .select("state", "year", "month", "no2avg","eventdate")     
        

query = views_state.writeStream \
    .trigger(processingTime ="10 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("keyspace", "stuff")\
    .option("table","month_most_polluted")\
    .option("checkpointLocation", '/tmp/check_point/') \
    .outputMode("append")\
    .start()

query.awaitTermination()
