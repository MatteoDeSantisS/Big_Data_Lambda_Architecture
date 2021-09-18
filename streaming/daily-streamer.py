from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, get_json_object, month, year

NO2TRESHOLD = 51
SO2TRESHOLD = 51
COTRESHOLD = 51
O3TRESHOLD = 51

spark_session = SparkSession \
    .builder \
    .appName("Pollution_Streaming_Analyzer") \
    .config("spark.cassandra.connection.host", "localhost:9042") \
    .getOrCreate()

daily_pollution = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensors-data") \
    .option("startingOffsets", "earliest") \
    .load()

daily_pollution = daily_pollution.selectExpr(
    "CAST(key AS STRING)", "CAST(value AS STRING)")

daily_pollution = daily_pollution.select(
    get_json_object(daily_pollution.value, '$.State').alias('state'),
    get_json_object(daily_pollution.value, '$.County').alias('county'),
    get_json_object(daily_pollution.value, '$.City').alias('city'),
    get_json_object(daily_pollution.value, '$.Date Local').alias('datelocal'),
    get_json_object(daily_pollution.value, '$.NO2 Mean').alias('no2mean'),
    get_json_object(daily_pollution.value, '$.NO2 AQI').alias('no2aqi'),
    get_json_object(daily_pollution.value, '$.SO2 Mean').alias('so2mean'),
    get_json_object(daily_pollution.value, '$.SO2 AQI').alias('so2aqi'),
    get_json_object(daily_pollution.value, '$.CO Mean').alias('comean'),
    get_json_object(daily_pollution.value, '$.CO AQI').alias('coaqi'),
    get_json_object(daily_pollution.value, '$.O3 Mean').alias('o3mean'),
    get_json_object(daily_pollution.value, '$.O3 AQI').alias('o3aqi'),
)

daily_pollution = daily_pollution \
    .select('state', 'county', 'city', 'day', 'month', 'year', 'no2mean', 'no2aqi', 'so2mean', 'so2aqi', 'comean', 'coaqi', 'o3mean', 'o3aqi') \
    .filter((col('so2aqi').isNotNull()) | (col('no2aqi').isNotNull()) | (col('o3aqi').isNotNull()) | (col('coaqi').isNotNull()))

daily_pollution = daily_pollution \
    .withColumn("day", dayofmonth("datelocal")) \
    .withColumn("month", month("datelocal")) \
    .withColumn("year", year("datelocal"))

most_polluted_by_day = daily_pollution \
    .filter(
        ((col('no2aqimax') >= NO2TRESHOLD) & (col('so2aqimax') >= SO2TRESHOLD)) | ((col('coaqimax') >= COTRESHOLD) & (col('o3aqimax') >= O3TRESHOLD)) |
        ((col('no2aqimax') >= NO2TRESHOLD) & (col('o3aqimax') >= O3TRESHOLD)) | ((col('coaqimax') >= COTRESHOLD) & (col('so2aqimax') >= SO2TRESHOLD)) |
        ((col('no2aqimax') >= NO2TRESHOLD) & (col('coaqimax') >= COTRESHOLD)) |  ((col('o3aqimax') >= O3TRESHOLD) & (col('so2aqimax') >= SO2TRESHOLD))) \
    .select('state', 'city', 'day', 'month', 'year', 'no2mean', 'no2aqimax', 'so2mean', 'so2aqimax', 'comean', 'coaqimax', 'o3mean', 'o3aqimax')

query = most_polluted_by_day.writeStream \
    .trigger(processingTime="5 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("keyspace", "stuff")\
    .option("checkpointLocation", '/tmp/daily/checkpoint/') \
    .option("table", "daily_pollution")\
    .outputMode("append")\
    .start()

query.awaitTermination()
