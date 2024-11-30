import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Database connection parameters
DB_NAME = "autonomous_vehicle_system"
DB_USER = "root"
DB_PASSWORD = ""
DB_HOST = "192.168.0.15"
DB_PORT = "26256"

# Function to insert data into a table
def insert_data_into_table(cursor, table_name, dataframe, columns):
    """
    Insert data into the specified table.
    
    :param cursor: Database cursor object
    :param table_name: Name of the table
    :param dataframe: DataFrame containing the data
    :param columns: Columns to insert data into
    """
    values = [tuple(row) for row in dataframe[columns].itertuples(index=False)]
    # Convert dataframe to list of tuples
    # values = [tuple(x) for x in dataframe.to_numpy()]
    
    # Convert route_points to JSON if the table is 'routes'
    if table_name == 'routes':
        for i, value in enumerate(values):
            value = list(value)
            value[6] = json.dumps(value[6])  # Convert route_points to JSON
            values[i] = tuple(value)

    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    print(sql)  # Print the SQL statement for verification
    execute_values(cursor, sql, values)
    conn.commit()

# Kafka Consumer configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker's address
TOPICS = ['vehicles', 'vehicle_status', 'routes', 'control_commands', 'collision_warnings', 'road_sensors', 'traffic_signals']  # List of topics to subscribe to
GROUP_ID = 'python_kafka_consumer'  # Consumer group ID

def consume_messages():
    # Consumer configuration
    conf = {
        'bootstrap.servers': KAFKA_BROKER,  # Kafka broker(s)
        'group.id': GROUP_ID,               # Consumer group ID
        'auto.offset.reset': 'earliest',    # Start from the earliest message
    }

    # Create a Consumer instance
    consumer = Consumer(conf)
    vehicle_consumer = Consumer(conf)
    other_consumer = Consumer(conf) 

    try:
        # Subscribe to the topics
        # consumer.subscribe(TOPICS)
        vehicle_consumer.subscribe(['vehicles'])
        other_consumer.subscribe(['vehicle_status', 'routes', 'control_commands', 'collision_warnings', 'road_sensors', 'traffic_signals'])
        print(f"Subscribed to topics: {TOPICS}")

        while True:
            # Poll for a message from the 'vehicles' topic
            msg = vehicle_consumer.poll(timeout=2.0)  # Timeout in seconds

            if msg is None:
                break  # Exit the inner loop to poll from other topics
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    continue

            # Process the message
            print(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()}")
            data = json.loads(msg.value().decode('utf-8'))
            table_name = msg.topic()
            dataframe = pd.DataFrame([data])
            columns = dataframe.columns.tolist()
            insert_data_into_table(cursor, table_name, dataframe, columns)

        while True:
            # Poll for a message from the other topics
            msg = other_consumer.poll(timeout=1.0)  # Timeout in seconds

            if msg is None:
                continue  # Exit the inner loop to poll from 'vehicles' topic again
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    continue

            # Process the message
            print(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()}")
            data = json.loads(msg.value().decode('utf-8'))
            table_name = msg.topic()
            dataframe = pd.DataFrame([data])
            columns = dataframe.columns.tolist()
            insert_data_into_table(cursor, table_name, dataframe, columns)

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# Connect to the CockroachDB database
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    print("Connected to the database successfully.")

    consume_messages()

except Exception as e:
    print(f"Error: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()