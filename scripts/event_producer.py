import json
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
import datetime
import requests
import time

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092", request_timeout_ms=60000,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'), api_version=(0, 10, 1))

json_message = None
city_name = None
temperature = None
humidity = None
weather_api_endpoint = None
applied = None

def weather_detail(weather_api_endpoint):
    api_response = requests.get(weather_api_endpoint)
    json_data = api_response.json()
    city_name = json_data['name']
    humidity = json_data['main']['humidity']
    temperature = json_data['main']['temp']
    feel_like=json_data['main']['feels_like']
    temp_diff=json_data['main']['feels_like']-json_data['main']['temp_max']
    tempmin=json_data['main']['temp_min']
    tempmax=json_data['main']['temp_max']
    wind_speed=json_data['wind']['speed']
    country= json_data['sys']['country']
    json_message = {'country':country,'city_name': city_name, 'temperature': round(temperature),'temp_min':round(tempmin),'temp_max':round(tempmax),'feel_like':round(feel_like),'humidity': humidity,
                   'wind_speed':round(wind_speed ),'temp_diff':round(temp_diff), 'creation_time': datetime.datetime.now().isoformat()}
    return json_message


def main(api_key):
    global kafka_topic
    while True:
        for city_name in ['Jakarta','Bogor','Depok','Tangerang','Bekasi']:
            # api_key = os.getenv("WEATHER_API_KEY")
            api_key = "c8ef4afa0a03a92329cadda8e5ba09ae"
            weather_api_endpoint = f'http://api.openweathermap.org/data/2.5/weather?q={city_name}&units=imperial&appid={api_key}'

            json_message = weather_detail(weather_api_endpoint)
            print(json_message)
            if type(kafka_topic) == bytes:
                kafka_topic = kafka_topic.decode('utf-8')
            producer.send(kafka_topic,json_message)

            print('wait for 10 seconds')
            time.sleep(5)
            producer.flush()


if __name__ == '__main__':
   main(producer)
