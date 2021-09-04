from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, get_json_object
import uuid

spark_session = SparkSession \
    .builder \
    .appName("Pollution Analyzer") \
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
    get_json_object(dataframe.value, '$.State Code').alias('stateCode'),
    get_json_object(dataframe.value, '$.Address').alias('address'),
    get_json_object(dataframe.value, '$.State').alias('state'),
    get_json_object(dataframe.value, '$.County').alias('county'),
    get_json_object(dataframe.value, '$.City').alias('city'),
    get_json_object(dataframe.value, '$.Date Local').alias('dateLocal'),
    get_json_object(dataframe.value, '$.NO2 Mean').alias('no2Mean'),
    get_json_object(dataframe.value, '$.O3 Mean').alias('o3Mean'),
    get_json_object(dataframe.value, '$.SO2 Mean').alias('so2Mean'),
    get_json_object(dataframe.value, '$.CO Mean').alias('coMean')
)

dataframe = dataframe.groupBy("state").agg(avg("no2Mean"),avg("o3Mean"),avg("so2Mean"),avg("coMean"))

#dataframe.withColumn("uuid", uuid.uuid4())

query = dataframe.writeStream \
    .trigger(processingTime="10 seconds")\
    .outputMode('update') \
    .format("org.apache.spark.sql.cassandra")\
    .option("keyspace", "stuff")\
    .option("table","pollution")\
    .outputMode("update")\
    .start()

query.awaitTermination()
