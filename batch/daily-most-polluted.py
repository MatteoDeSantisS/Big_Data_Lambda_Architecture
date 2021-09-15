from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, col
from utils import NO2TRESHOLD, SO2TRESHOLD, COTRESHOLD, O3TRESHOLD

spark_session = SparkSession \
    .builder \
    .appName("Daily Most-Polluted City") \
    .config("spark.cassandra.connection.host", "localhost:9042") \
    .getOrCreate()

pollution = spark_session \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="stuff", table="pollution") \
    .load()

#Aggrega i dati di una stessa città per stato, mese e anno
pollution = pollution \
    .groupBy('state', 'city', 'day', 'month', 'year') \
    .agg(
        avg('no2mean').alias('no2mean'), max('no2aqi').alias('no2aqimax'),
        avg('so2mean').alias('so2mean'), max('so2aqi').alias('so2aqimax'), 
        avg('comean').alias('comean'), max('coaqi').alias('coaqimax'),
        avg('o3mean').alias('o3mean'), max('o3aqi').alias('o3aqimax')) \
    .select('state', 'city', 'day', 'month', 'year', 'no2mean', 'no2aqimax', 'so2mean', 'so2aqimax', 'comean', 'coaqimax', 'o3mean', 'o3aqimax')

most_polluted_by_day = pollution \
    .filter(
        ((col('no2aqimax') >= NO2TRESHOLD) & (col('so2aqimax') >= SO2TRESHOLD)) | ((col('coaqimax') >= COTRESHOLD) & (col('o3aqimax') >= O3TRESHOLD)) |
        ((col('no2aqimax') >= NO2TRESHOLD) & (col('o3aqimax') >= O3TRESHOLD)) | ((col('coaqimax') >= COTRESHOLD) & (col('so2aqimax') >= SO2TRESHOLD)) |
        ((col('no2aqimax') >= NO2TRESHOLD) & (col('coaqimax') >= COTRESHOLD)) |  ((col('o3aqimax') >= O3TRESHOLD) & (col('so2aqimax') >= SO2TRESHOLD))) \
    .select('state', 'city', 'day', 'month', 'year', 'no2mean', 'no2aqimax', 'so2mean', 'so2aqimax', 'comean', 'coaqimax', 'o3mean', 'o3aqimax')

query = most_polluted_by_day \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="stuff", table="daily_most_polluted")\
    .save()


