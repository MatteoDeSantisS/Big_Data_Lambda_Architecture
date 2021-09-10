import pandas as pd
from kafka import KafkaProducer
import time
import json

csv = pd.read_csv("../data/pollution.csv")
pollution = csv.values.tolist()

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    data_sent = 0

    for record in pollution:
        #timestamp = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        record = tuple(record)
        data_to_send = {
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
        producer.send('sensors-data', value=data_to_send)
        data_sent += 1
        if (data_sent % 1000) == 0:
            time.sleep(10)
