from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, get_json_object, month, year

cluster_seeds = ['cass1:9042','cass2:9043','cass:9044']
spark_session = SparkSession \
    .builder \
    .appName("Pollution Streamer") \
    .config("spark.cassandra.connection.host", ','.join(cluster_seeds)) \
    .config("spark.cassandra.auth.username","cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.executor.memory", "2g")\
    .config("spark.executor.cores", "2")\
    .config("spark.cores.max","2")\
    .config("spark.executor.instances","1")\
    .getOrCreate()
#.config("spark.scheduler.mode", "FAIR")\
dataframe = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensors-data") \
    .option("startingOffsets", "earliest") \
    .load()

dataframe = dataframe.selectExpr(
    "CAST(key AS STRING)", "CAST(value AS STRING)")

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
    .withColumn("day", dayofmonth("datelocal")) \
    .withColumn("month", month("datelocal")) \
    .withColumn("year", year("datelocal"))

dataframe = dataframe \
    .select('state', 'county', 'city', 'day', 'month', 'year', 'no2mean', 'no2aqi', 'so2mean', 'so2aqi', 'comean', 'coaqi', 'o3mean', 'o3aqi') \
    .filter((col('so2aqi').isNotNull()) | (col('no2aqi').isNotNull()) | (col('o3aqi').isNotNull()) | (col('coaqi').isNotNull()))

query = dataframe.writeStream \
    .trigger(processingTime="5 seconds")\
    .format("org.apache.spark.sql.cassandra")\
    .option("keyspace", "airqualitykeyspace")\
    .option("checkpointLocation", '/tmp/stream/checkpoint/') \
    .option("table", "pollution")\
    .outputMode("append")\
    .start()

query.awaitTermination()