from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, format_number

cluster_seeds = ['cass1:9042','cass2:9043','cass3:9094']
spark_session = SparkSession \
    .builder \
    .appName("Month Most-Polluted City") \
    .config("spark.cassandra.connection.host", ','.join(cluster_seeds)) \
    .config("spark.cassandra.auth.username","cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.executor.memory", "2g")\
    .config("spark.executor.cores", "2")\
    .config("spark.cores.max","2")\
    .config("spark.executor.instances","1")\
    .getOrCreate()
 #.config("spark.scheduler.mode", "FAIR")\
pollution = spark_session \
    .read \
    .format("org.apache.spark.sql.cassandra") \
    .options(keyspace="airqualitykeyspace", table="pollution") \
    .load()

#--------------------------------------------------- NO2 -------------------------------------------------------
no2_pollution = pollution \
    .groupBy('state', 'city', 'month', 'year') \
    .agg(avg('no2mean').alias('no2mean'), avg('no2aqi').alias('no2aqi') ) \
    .select('state', 'city', 'month', 'year', 'no2mean', 'no2aqi')

no2_most_polluted = pollution \
    .withColumnRenamed('state', 'mstate').withColumnRenamed('month', 'mmonth').withColumnRenamed('year','myear') \
    .groupBy('mstate', 'mmonth', 'myear') \
    .agg(avg('no2aqi').alias('mno2aqi')) \
    .select('mstate', 'mmonth', 'myear', 'mno2aqi')

no2_joindf = no2_most_polluted.join(no2_pollution,
    (no2_most_polluted.mstate == no2_pollution.state) &
    (no2_most_polluted.mmonth == no2_pollution.month) &
    (no2_most_polluted.myear == no2_pollution.year) &
    (no2_most_polluted.mno2aqi == no2_pollution.no2aqi) 
).select('city', 'state', 'month', 'year', 'no2mean', 'no2aqi')

no2_joindf = no2_joindf \
    .withColumn('no2mean', format_number(no2_joindf['no2mean'], 2))\

query = no2_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="no2_month")\
    .save()

# ----------------------------------------------- SO2 ---------------------------------------------------------

so2_pollution = pollution \
    .groupBy('state', 'city', 'month', 'year') \
    .agg(avg('so2mean').alias('so2mean'), avg('so2aqi').alias('so2aqi') ) \
    .select('state', 'city', 'month', 'year', 'so2mean', 'so2aqi')

so2_most_polluted = pollution \
    .withColumnRenamed('state', 'mstate').withColumnRenamed('month', 'mmonth').withColumnRenamed('year','myear') \
    .groupBy('mstate', 'mmonth', 'myear') \
    .agg(avg('so2aqi').alias('mso2aqi')) \
    .select('mstate', 'mmonth', 'myear', 'mso2aqi')

so2_joindf = so2_most_polluted.join(so2_pollution,
    (so2_most_polluted.mstate == so2_pollution.state) &
    (so2_most_polluted.mmonth == so2_pollution.month) &
    (so2_most_polluted.myear == so2_pollution.year) &
    (so2_most_polluted.mso2aqi == so2_pollution.so2aqi) 
).select('city', 'state', 'month', 'year', 'so2mean', 'so2aqi')

so2_joindf = so2_joindf \
    .withColumn('so2mean', format_number(so2_joindf['so2mean'], 2))

query = so2_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="so2_month")\
    .save()

# ----------------------------------------------- CO ---------------------------------------------------------

co_pollution = pollution \
    .groupBy('state', 'city', 'month', 'year') \
    .agg(avg('comean').alias('comean'), avg('coaqi').alias('coaqi') ) \
    .select('state', 'city', 'month', 'year', 'comean', 'coaqi')

co_most_polluted = pollution \
    .withColumnRenamed('state', 'mstate').withColumnRenamed('month', 'mmonth').withColumnRenamed('year','myear') \
    .groupBy('mstate', 'mmonth', 'myear') \
    .agg(avg('coaqi').alias('mcoaqi')) \
    .select('mstate', 'mmonth', 'myear', 'mcoaqi')

co_joindf = co_most_polluted.join(co_pollution,
    (co_most_polluted.mstate == co_pollution.state) &
    (co_most_polluted.mmonth == co_pollution.month) &
    (co_most_polluted.myear == co_pollution.year) &
    (co_most_polluted.mcoaqi == co_pollution.coaqi) 
).select('city', 'state', 'month', 'year', 'comean', 'coaqi')

co_joindf = co_joindf \
    .withColumn('comean', format_number(co_joindf['comean'], 2))

query = co_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="co_month")\
    .save()

# ----------------------------------------------- O3 ---------------------------------------------------------

o3_pollution = pollution \
    .groupBy('state', 'city', 'month', 'year') \
    .agg(avg('o3mean').alias('o3mean'), avg('o3aqi').alias('o3aqi') ) \
    .select('state', 'city', 'month', 'year', 'o3mean', 'o3aqi')

o3_most_polluted = pollution \
    .withColumnRenamed('state', 'mstate').withColumnRenamed('month', 'mmonth').withColumnRenamed('year','myear') \
    .groupBy('mstate', 'mmonth', 'myear') \
    .agg(avg('o3aqi').alias('mo3aqi')) \
    .select('mstate', 'mmonth', 'myear', 'mo3aqi')

o3_joindf = o3_most_polluted.join(o3_pollution,
    (o3_most_polluted.mstate == o3_pollution.state) &
    (o3_most_polluted.mmonth == o3_pollution.month) &
    (o3_most_polluted.myear == o3_pollution.year) &
    (o3_most_polluted.mo3aqi == o3_pollution.o3aqi) 
).select('city', 'state', 'month', 'year', 'o3mean', 'o3aqi')

o3_joindf = o3_joindf \
    .withColumn('o3mean', format_number(o3_joindf['o3mean'], 2))

query = o3_joindf \
    .write \
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(keyspace="airqualitykeyspace", table="o3_month")\
    .save()
