import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import json

csv = pd.read_csv("../data/pollution.csv")
pollution = csv.values.tolist()

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer = lambda x: json.dumps(x).encode('utf-8'))

while True:
    current_date = datetime.strptime(pollution[0][5], "%Y-%m-%d")

    for record in pollution:
        if(current_date == datetime.strptime(record[5], "%Y-%m-%d")):
            record = tuple(record)
            data_to_send = {
                'State Code': record[0],
                'Address': record[1],
                'State': record[2],
                'County': record[3],
                'City': record[4],
                'Date Local': record[5],
                'NO2 Mean': record[6],
                'O3 Mean': record[7],
                'SO2 Mean': record[8],
                'CO Mean': record[9]
            }
            producer.send('sensors-data', value = data_to_send)
        else:
            time.sleep(10)
            current_date = datetime.strptime(record[5], "%Y-%m-%d")
            record = tuple(record)
            data_to_send = {
                'State Code': record[0],
                'Address': record[1],
                'State': record[2],
                'County': record[3],
                'City': record[4],
                'Date Local': record[5],
                'NO2 Mean': record[6],
                'O3 Mean': record[7],
                'SO2 Mean': record[8],
                'CO Mean': record[9]
            }
            producer.send('sensors-data', value = data_to_send)