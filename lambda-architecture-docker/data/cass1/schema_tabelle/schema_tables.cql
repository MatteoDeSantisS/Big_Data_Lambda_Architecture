//Create keyspace
CREATE KEYSPACE IF NOT EXISTS AirQualityKeySpace WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};

//Create table
CREATE TABLE IF NOT EXISTS AirQualityKeySpace.pollution (state text, county text, city text, day int, month int, year int, no2mean double, no2aqi double, so2mean double, so2aqi double, comean double, coaqi double, o3mean double, o3aqi double, PRIMARY KEY ((year), month, day, state, city)) with clustering order by (month desc);

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.daily_pollution (state text, county text, city text, day int, month int, year int, no2aqi double, so2aqi double, coaqi double, o3aqi double, PRIMARY KEY ((year), month, day, state, county, city)) with clustering order by (month desc, day desc);

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.no2_month (state text, city text, month int, year int, no2mean double, no2aqi int, PRIMARY KEY ((year), month, state, city)) with clustering order by (month desc);

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.so2_month (state text, city text, month int, year int, so2mean double, so2aqi int, PRIMARY KEY ((year), month, state, city)) with clustering order by (month desc);

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.co_month (state text, city text, month int, year int, comean double, coaqi int, PRIMARY KEY ((year), month, state, city)) with clustering order by (month desc);

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.o3_month (state text, city text, month int, year int, o3mean double, o3aqi int, PRIMARY KEY ((year), month, state, city)) with clustering order by (month desc);

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.o3_year (state text, city text, year int, o3mean double, o3aqi int, PRIMARY KEY ((year), state, city));

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.co_year (state text, city text, year int, comean double, coaqi int, PRIMARY KEY ((year), month, state, city));

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.so2_year (state text, city text, year int, so2mean double, so2aqi int, PRIMARY KEY ((year), month, state, city));

CREATE TABLE IF NOT EXISTS AirQualityKeySpace.no2_year (state text, city text, year int, no2mean double, no2aqi int, PRIMARY KEY ((year), month, state, city));
