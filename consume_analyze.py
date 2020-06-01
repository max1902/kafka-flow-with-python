import json
from time import sleep

import psycopg2
from kafka import KafkaConsumer

fields = [
    'title',
    'brand',
    'series',
    'USD',
    'EUR',
    'UAH',
    'Kilometer',
    'Fuel Type',
    'Transmission',
    'Wheel Drive',
    'Color',
    'seller',
    'phone',
]


def db_connection():
    return psycopg2.connect(
        database='main_db',
        user='max',
        password='maxadmin',
        host='127.0.0.1',
        port='5432',
    )


def create_table(cursor):
    cursor.execute("""CREATE TABLE cars (
        title VARCHAR,
        brand VARCHAR,
        series VARCHAR,
        USD VARCHAR,
        EUR VARCHAR,
        UAH VARCHAR,
        Kilometer VARCHAR,
        FuelType VARCHAR,
        Transmission VARCHAR,
        WheelDrive VARCHAR,
        Color VARCHAR,
        Seller VARCHAR,
        Phone VARCHAR
    )""")


if __name__ == '__main__':
    parsed_topic_name = 'parsed_cars_pages'
    consumer = KafkaConsumer(
        parsed_topic_name,
        auto_offset_reset='earliest',
        bootstrap_servers=['localhost:9092'],
        consumer_timeout_ms=1000,
    )
    conn = db_connection()
    cursor = conn.cursor()
    create_table(cursor)
    for msg in consumer:
        record = json.loads(msg.value)
        data = [record.get(field, '') for field in fields]
        print(data)
        insert_query = 'INSERT INTO cars VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        cursor.execute(insert_query, tuple(data))
        conn.commit()
    conn.close()
    consumer.close()
