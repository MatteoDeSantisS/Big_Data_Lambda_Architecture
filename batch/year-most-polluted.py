from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, format_number

spark_session = SparkSession \
    .builder \
    .appName("Year Most-Polluted City") \
    .config("spark.cassandra.connection.host", "localhost:9042") \
    .getOrCreate()

pollution = spark_session \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="airqualitykeyspace", table="pollution") \
    .load()

#------------------------------------------------------------ NO2 ------------------------------------------------------------------
no2_pollution = pollution \
    .groupBy('state', 'city', 'year') \
    .agg(avg('no2mean').alias('no2mean'), avg('no2aqi').alias('no2aqi') ) \
    .select('state', 'city', 'year', 'no2mean', 'no2aqi')

no2_most_polluted = pollution \
    .withColumnRenamed('state', 'ystate').withColumnRenamed('year','yyear') \
    .groupBy('ystate', 'yyear') \
    .agg(avg('no2aqi').alias('yno2aqi')) \
    .select('ystate', 'yyear', 'yno2aqi')

no2_joindf = no2_most_polluted.join(no2_pollution,
    (no2_most_polluted.ystate == no2_pollution.state) &
    (no2_most_polluted.yyear == no2_pollution.year) &
    (no2_most_polluted.yno2aqi == no2_pollution.no2aqi) 
).select('city', 'state', 'year', 'no2mean', 'no2aqi')

no2_joindf = no2_joindf \
    .withColumn('no2mean', format_number(no2_joindf['no2mean'], 2))

query = no2_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="no2_year")\
    .save()

#------------------------------------------------------------ SO2 ------------------------------------------------------------------

so2_pollution = pollution \
    .groupBy('state', 'city', 'year') \
    .agg(avg('so2mean').alias('so2mean'), avg('so2aqi').alias('so2aqi') ) \
    .select('state', 'city', 'year', 'so2mean', 'so2aqi')

so2_most_polluted = pollution \
    .withColumnRenamed('state', 'ystate').withColumnRenamed('year','yyear') \
    .groupBy('ystate', 'yyear') \
    .agg(avg('so2aqi').alias('yso2aqi')) \
    .select('ystate', 'yyear', 'yso2aqi')

so2_joindf = so2_most_polluted.join(so2_pollution,
    (so2_most_polluted.ystate == so2_pollution.state) &
    (so2_most_polluted.yyear == so2_pollution.year) &
    (so2_most_polluted.yso2aqi == so2_pollution.so2aqi) 
).select('city', 'state', 'year', 'so2mean', 'so2aqi')

so2_joindf = so2_joindf \
    .withColumn('so2mean', format_number(so2_joindf['so2mean'], 2))

query = so2_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="so2_year")\
    .save()

#------------------------------------------------------------ CO ------------------------------------------------------------------

co_pollution = pollution \
    .groupBy('state', 'city', 'year') \
    .agg(avg('comean').alias('comean'), avg('coaqi').alias('coaqi') ) \
    .select('state', 'city', 'year', 'comean', 'coaqi')

co_most_polluted = pollution \
    .withColumnRenamed('state', 'ystate').withColumnRenamed('year','yyear') \
    .groupBy('ystate', 'yyear') \
    .agg(avg('coaqi').alias('ycoaqi')) \
    .select('ystate', 'yyear', 'ycoaqi')

co_joindf = co_most_polluted.join(co_pollution,
    (co_most_polluted.ystate == co_pollution.state) &
    (co_most_polluted.yyear == co_pollution.year) &
    (co_most_polluted.ycoaqi == co_pollution.coaqi) 
).select('city', 'state', 'year', 'comean', 'coaqi')

co_joindf = co_joindf \
    .withColumn('comean', format_number(co_joindf['comean'], 2))

query = co_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="co_year")\
    .save()


#------------------------------------------------------------ O3 ------------------------------------------------------------------

o3_pollution = pollution \
    .groupBy('state', 'city', 'year') \
    .agg(avg('o3mean').alias('o3mean'), avg('o3aqi').alias('o3aqi') ) \
    .select('state', 'city', 'year', 'o3mean', 'o3aqi')

o3_most_polluted = pollution \
    .withColumnRenamed('state', 'ystate').withColumnRenamed('year','yyear') \
    .groupBy('ystate', 'yyear') \
    .agg(avg('o3aqi').alias('yo3aqi')) \
    .select('ystate', 'yyear', 'yo3aqi')

o3_joindf = o3_most_polluted.join(o3_pollution,
    (o3_most_polluted.ystate == o3_pollution.state) &
    (o3_most_polluted.yyear == o3_pollution.year) &
    (o3_most_polluted.yo3aqi == o3_pollution.o3aqi) 
).select('city', 'state', 'year', 'o3mean', 'o3aqi')

o3_joindf = o3_joindf \
    .withColumn('o3mean', format_number(o3_joindf['o3mean'], 2))

query = o3_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="o3_year")\
    .save()
