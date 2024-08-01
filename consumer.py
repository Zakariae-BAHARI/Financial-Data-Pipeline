from confluent_kafka import Consumer, KafkaError
import psycopg2
import json

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(**conf)
consumer.subscribe(['alpha_vantage_data'])

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="mydatabase",
    user="postgres",
    password="password",
    host="localhost"
)
cur = conn.cursor()

# Create table
cur.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INT
    )
''')
conn.commit()

def consume_data():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        data = json.loads(msg.value().decode('utf-8'))
        cur.execute(
            "INSERT INTO stock_data (timestamp, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)",
            (data['timestamp'], data['1. open'], data['2. high'], data['3. low'], data['4. close'], data['5. volume'])
        )
        conn.commit()

if __name__ == '__main__':
    try:
        consume_data()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()
