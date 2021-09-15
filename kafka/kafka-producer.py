import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import json

csv = pd.read_csv("../data/pollution.csv")
pollution = csv.values.tolist()

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer = lambda x: json.dumps(x).encode('utf-8'))

while True:
    current_date = datetime.strptime(pollution[0][3], "%Y-%m-%d")

    for record in pollution:
        
        if(current_date == datetime.strptime(record[3], "%Y-%m-%d")):
            record = tuple(record)
            data_send = {
            'State': record[0],
            'County': record[1],
            'City': record[2],
            'Date Local': record[3],
            'NO2 Mean': record[4],
            'NO2 AQI': record[5],
            'SO2 Mean': record[6],
            'SO2 AQI': record[7],
            'CO Mean': record[8],
            'CO AQI': record[9],
            'O3 Mean': record[8],
            'O3 AQI': record[9],
            #'Timestamp': timestamp
            }
            print(data_send)
            producer.send('sensors-data', value = data_send)
        else:
            time.sleep(30)
            current_date = datetime.strptime(record[3], "%Y-%m-%d")
            record = tuple(record)
            data_send = {
            'State': record[0],
            'County': record[1],
            'City': record[2],
            'Date Local': record[3],
            'NO2 Mean': record[4],
            'NO2 AQI': record[5],
            'SO2 Mean': record[6],
            'SO2 AQI': record[7],
            'CO Mean': record[8],
            'CO AQI': record[9],
            'O3 Mean': record[8],
            'O3 AQI': record[9],
            #'Timestamp': timestamp
            }
            producer.send('sensors-data', value = data_send)