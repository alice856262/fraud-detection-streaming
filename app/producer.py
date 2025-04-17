import csv
import random
import time
from kafka import KafkaProducer
from json import dumps
import datetime as dt
import threading

class DataProducer:
    def __init__(self, hostip='localhost'):
        self.browsing_topic = 'browsing'
        self.transaction_topic = 'transaction'
        self.hostip = hostip
        self.producer = self.connect_kafka_producer()
        self.running = False

    def connect_kafka_producer(self):
        _producer = None
        try:
            _producer = KafkaProducer(
                bootstrap_servers=[f'{self.hostip}:9092'],
                value_serializer=lambda x: dumps(x).encode('utf-8')
            )
        except Exception as ex:
            print(f"Error connecting to Kafka: {str(ex)}")
        return _producer

    def read_browsing_data(self, file_name, num_rows, start_row=0):
        rows = []
        with open(file_name, 'rt') as file_handle:
            reader = csv.DictReader(file_handle)
            for _ in range(start_row):
                try:
                    next(reader)
                except StopIteration:
                    return rows

            try:
                for _ in range(num_rows):
                    rows.append(next(reader))
            except StopIteration:
                return rows
        return rows

    def read_transaction_data(self, file_name, start_time, end_time):
        with open(file_name, 'rt') as file:
            reader = csv.DictReader(file)
            for row in reader:
                event_time = dt.datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S.%f')
                if start_time <= event_time < end_time:
                    yield row

    def send_data_to_kafka(self, topic, data):
        try:
            self.producer.send(topic, value=data)
        except Exception as ex:
            print(f"Error sending data: {str(ex)}")

    def stream_data(self, browsing_file, transaction_file):
        line = 0
        while self.running:
            # Simulate reading a random number of rows (500-1000) from browsing behavior data
            num_rows = random.randint(500, 1000)
            line = line + num_rows + 1
            browsing_data = list(self.read_browsing_data(browsing_file, num_rows, start_row=line))

            if not browsing_data:
                break

            # Get the start and end event_time from the browsing data
            start_time = dt.datetime.strptime(browsing_data[0]['event_time'], '%Y-%m-%d %H:%M:%S.%f')
            end_time = dt.datetime.strptime(browsing_data[-1]['event_time'], '%Y-%m-%d %H:%M:%S.%f')

            # Add 'ts' (Unix timestamp) column spread evenly over 5 seconds
            browsing_batch_size = len(browsing_data)
            for i, row in enumerate(browsing_data):
                row['ts'] = int(time.time() - 5) + i // (browsing_batch_size // 5)

            # Send browsing data to Kafka
            self.send_data_to_kafka(self.browsing_topic, browsing_data)

            # Read corresponding transaction data within the time range
            transaction_data = list(self.read_transaction_data(transaction_file, start_time, end_time))

            # Add 'ts' (Unix timestamp) column
            for i, row in enumerate(transaction_data):
                row['ts'] = int(time.time())

            # Send transaction data to Kafka
            self.send_data_to_kafka(self.transaction_topic, transaction_data)

            time.sleep(5)

    def start(self, browsing_file, transaction_file):
        self.running = True
        self.stream_thread = threading.Thread(
            target=self.stream_data,
            args=(browsing_file, transaction_file)
        )
        self.stream_thread.start()

    def stop(self):
        self.running = False
        if hasattr(self, 'stream_thread'):
            self.stream_thread.join()
        if self.producer:
            self.producer.close()

if __name__ == '__main__':
    producer = DataProducer()
    try:
        producer.start(
            './data/dataset/new_browsing_behaviour.csv',
            './data/dataset/new_transactions.csv'
        )
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        producer.stop() 