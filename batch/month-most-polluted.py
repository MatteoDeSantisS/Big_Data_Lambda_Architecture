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
    .groupBy('state', 'city', 'month', 'year') \
    .agg(avg('no2mean').alias('no2mean'), max('no2aqi').alias('no2aqimax') ) \
    .select('state', 'city', 'month', 'year', 'no2mean', 'no2aqimax')

most_polluted_by_month = pollution \
    .withColumnRenamed('state', 'mstate').withColumnRenamed('month', 'mmonth').withColumnRenamed('year','myear') \
    .groupBy('mstate', 'mmonth', 'myear') \
    .agg(max('no2aqimax').alias('mno2aqimax')) \
    .select('mstate', 'mmonth', 'myear', 'mno2aqimax')

joinDf = most_polluted_by_month.join(pollution,
    (most_polluted_by_month.mstate == pollution.state) &
    (most_polluted_by_month.mmonth == pollution.month) &
    (most_polluted_by_month.myear == pollution.year) &
    (most_polluted_by_month.mno2aqimax == pollution.no2aqimax) 
).select('city', 'state', 'month', 'year', 'no2mean', 'no2aqimax')

query = joinDf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="stuff", table="month_most_polluted")\
    .save()


