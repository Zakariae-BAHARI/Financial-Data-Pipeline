import requests
from confluent_kafka import Producer
import json
import time

# Kafka Producer configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(**conf)

# Alpha Vantage API endpoint
api_key = 'X4IQ127ELWY7IV84'
symbol = 'AAPL'  # Example stock symbol
api_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}'

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def fetch_data():
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json().get('Time Series (1min)', {})
        for timestamp, values in data.items():
            message = {'timestamp': timestamp, **values}
            producer.produce('alpha_vantage_data', key=timestamp, value=json.dumps(message), callback=delivery_report)
            producer.flush()
            time.sleep(1)  # simulate delay
    else:
        print(f"Failed to fetch data: {response.status_code}")

if __name__ == '__main__':
    fetch_data()
