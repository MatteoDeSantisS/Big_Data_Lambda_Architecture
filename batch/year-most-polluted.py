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
    .groupBy('state', 'city', 'year') \
    .agg(avg('no2mean').alias('no2mean'), max('no2aqi').alias('no2aqimax') ) \
    .select('state', 'city', 'year', 'no2mean', 'no2aqimax')

most_polluted_by_month = pollution \
    .withColumnRenamed('state', 'ystate').withColumnRenamed('year','yyear') \
    .groupBy('ystate', 'yyear') \
    .agg(max('no2aqimax').alias('yno2aqimax')) \
    .select('ystate', 'yyear', 'yno2aqimax')

joinDf = most_polluted_by_month.join(pollution,
    (most_polluted_by_month.ystate == pollution.state) &
    (most_polluted_by_month.yyear == pollution.year) &
    (most_polluted_by_month.yno2aqimax == pollution.no2aqimax) 
).select('city', 'state', 'year', 'no2mean', 'no2aqimax')

query = joinDf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="stuff", table="year_most_polluted")\
    .save()


