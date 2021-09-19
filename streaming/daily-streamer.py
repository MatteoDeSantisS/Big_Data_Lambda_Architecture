from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, get_json_object, month, year

THRESHOLD = 50

spark_session = SparkSession \
    .builder \
    .appName("Pollution Streamer") \
    .config("spark.cassandra.connection.host", "localhost:9042") \
    .getOrCreate()

dataframe = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensors-data") \
    .option("startingOffsets", "earliest") \
    .load()

dataframe = dataframe.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

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
)

dataframe = dataframe \
    .select('state', 'county', 'city', 'datelocal', 'no2mean', 'no2aqi', 'so2mean', 'so2aqi', 'comean', 'coaqi', 'o3mean', 'o3aqi') \
    .filter((col('so2aqi').isNotNull()) | (col('no2aqi').isNotNull()) | (col('o3aqi').isNotNull()) | (col('coaqi').isNotNull()))

dataframe = dataframe \
    .withColumn("day", dayofmonth("datelocal")) \
    .withColumn("month", month("datelocal")) \
    .withColumn("year", year("datelocal")) \
    .select('state', 'county', 'city', 'day', 'month', 'year', 'no2mean', 'no2aqi', 'so2mean', 'so2aqi', 'comean', 'coaqi', 'o3mean', 'o3aqi')

most_polluted_by_day = dataframe \
    .filter( 
        ((col('no2aqi') > THRESHOLD) & (col('so2aqi') > THRESHOLD)) | ((col('coaqi') > THRESHOLD) & (col('o3aqi') > THRESHOLD)) |
        ((col('no2aqi') > THRESHOLD) & (col('o3aqi') > THRESHOLD)) | ((col('coaqi') > THRESHOLD) & (col('so2aqi') > THRESHOLD)) |
        ((col('no2aqi') > THRESHOLD) & (col('coaqi') > THRESHOLD)) |  ((col('o3aqi') > THRESHOLD) & (col('so2aqi') > THRESHOLD))|
        (col('no2aqi') > THRESHOLD) | (col('so2aqi') > THRESHOLD) | (col('coaqi') > THRESHOLD) | (col('o3aqi') > THRESHOLD)
    )\
    .select('state', 'county', 'city', 'day', 'month', 'year', 'no2aqi', 'so2aqi', 'coaqi', 'o3aqi')

query = most_polluted_by_day.writeStream \
    .trigger(processingTime="5 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("keyspace", "stuff")\
    .option("checkpointLocation", '/tmp/daily/aaa/checkpoint/') \
    .option("table", "daily_pollution")\
    .outputMode("append")\
    .start()

query.awaitTermination()
