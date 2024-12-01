from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from confluent_kafka import Consumer, KafkaError
import json

# Database connection parameters
DB_HOST = "192.168.0.231"
DB_PORT = 5200
DB_USER = "admin"
DB_PASSWORD = "admin"
KEYSPACE = "autonomous_vehicle_system"

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'control_commands'
KAFKA_GROUP_ID = 'cassandra_consumer_group12'

def connect_to_cassandra():
    """
    Connect to the Cassandra database and create a session.
    
    :return: Cassandra session
    """
    auth_provider = PlainTextAuthProvider(username=DB_USER, password=DB_PASSWORD)
    cluster = Cluster([DB_HOST], port=DB_PORT, auth_provider=auth_provider)
    session = cluster.connect(KEYSPACE)
    print("Connected to Cassandra")
    return session

def check_table_exists(session, table_name):
    """
    Check if a table exists in the keyspace.
    
    :param session: Cassandra session
    :param table_name: Name of the table to check
    :return: True if the table exists, False otherwise
    """
    query = f"""
    SELECT table_name FROM system_schema.tables
    WHERE keyspace_name='{KEYSPACE}' AND table_name='{table_name}';
    """
    result = session.execute(query)
    return result.one() is not None

def create_control_commands_table(session):
    """
    Create the control_commands table in the autonomous_vehicle_system keyspace.
    
    :param session: Cassandra session
    """
    query = """
    CREATE TABLE IF NOT EXISTS control_commands (
        command_id TEXT PRIMARY KEY,
        vehicle_id INT,
        timestamp TIMESTAMP,
        command_type TEXT,
        details TEXT
    );
    """
    session.execute(query)

def consume_kafka_messages():
    """
    Consume messages from Kafka and insert them into the Cassandra control_commands table.
    """
    # Kafka consumer configuration
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    }

    # Create Kafka consumer
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    # Connect to Cassandra
    session = connect_to_cassandra()

    # Check if the control_commands table exists, and create it if it doesn't
    if not check_table_exists(session, 'control_commands'):
        create_control_commands_table(session)
        print("Table control_commands created successfully")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    continue

            # Process the message
            message_value = msg.value().decode('utf-8')
            command_data = json.loads(message_value)

            # Insert data into Cassandra
            insert_command(session, command_data)

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer and Cassandra session
        consumer.close()
        session.shutdown()

def insert_command(session, command_data):
    """
    Insert a command record into the Cassandra control_commands table.
    
    :param session: Cassandra session
    :param command_data: Dictionary containing command data
    """
    query = """
    INSERT INTO control_commands (command_id, vehicle_id, timestamp, command_type, details)
    VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (
        command_data['command_id'],
        command_data['vehicle_id'],
        command_data['timestamp'],
        command_data['command_type'],
        command_data['details']
    ))

# Start consuming Kafka messages
consume_kafka_messages()