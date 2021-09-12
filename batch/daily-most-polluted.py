from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max

spark_session = SparkSession \
    .builder \
    .appName("Month Most-Polluted City") \
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
    .agg(avg('no2mean').alias('no2mean'), max('no2aqi').alias('no2aqimax') ) \
    .select('state', 'city', 'day', 'month', 'year', 'no2mean', 'no2aqimax')

most_polluted_by_day = pollution \
    .withColumnRenamed('state', 'dstate').withColumnRenamed('day', 'dday').withColumnRenamed('month', 'dmonth').withColumnRenamed('year','dyear') \
    .groupBy('dstate', 'dday', 'dmonth', 'dyear') \
    .agg(max('no2aqimax').alias('dno2aqimax')) \
    .select('dstate', 'dday', 'dmonth', 'dyear', 'dno2aqimax')

joinDf = most_polluted_by_day.join(pollution,
    (most_polluted_by_day.dstate == pollution.state) &
    (most_polluted_by_day.dday == pollution.day) &
    (most_polluted_by_day.dmonth == pollution.month) &
    (most_polluted_by_day.dyear == pollution.year) &
    (most_polluted_by_day.dno2aqimax == pollution.no2aqimax) 
).select('city', 'state', 'day', 'month', 'year', 'no2mean', 'no2aqimax')

query = joinDf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="stuff", table="daily_most_polluted")\
    .save()


