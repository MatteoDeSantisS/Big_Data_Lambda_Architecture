import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import json

csv = pd.read_csv("epa_daily.csv")
epa_daily = csv.values.tolist()

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    current_date = datetime.strptime(epa_daily[0][4], "%Y-%m-%d")

    for record in epa_daily:
        if(current_date==datetime.strptime(record[4], "%Y-%m-%d")):
            record = tuple(record)
            to_send = {
                'state_code': record[0],
                'county_code': record[1],
                'parameter_name': record[2],
                'sample_duration': record[3],
                'date_local': record[4],
                'units_of_measure': record[5],
                'observation_count': record[6],
                'arithmetic_mean': record[7],
                'state_name': record[8],
                'county_name': record[9],
                'city_name': record[10]
            }
            producer.send('sensors-data', value = to_send)
        else:
            time.sleep(10)
            current_date = datetime.strptime(record[4], "%Y-%m-%d")
            record = tuple(record)
            to_send = {
                'state_code': record[0],
                'county_code': record[1],
                'parameter_name': record[2],
                'sample_duration': record[3],
                'date_local': record[4],
                'units_of_measure': record[5],
                'observation_count': record[6],
                'arithmetic_mean': record[7],
                'state_name': record[8],
                'county_name': record[9],
                'city_name': record[10]
            }
            producer.send('sensors-data', value = to_send)