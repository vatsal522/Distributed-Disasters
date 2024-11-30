from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'  # Replace with your Kafka broker's address
TOPICS = ['vehicles', 'vehicle_status', 'routes', 'control_commands', 'road_sensors', 'collision_warnings', 'traffic_signals']  # List of topics to delete

def delete_topics(broker, topics):
    admin_client = AdminClient({'bootstrap.servers': broker})

    # Delete topics
    fs = admin_client.delete_topics(topics, operation_timeout=30)

    # Wait for each operation to finish
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} deleted")
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")

if __name__ == "__main__":
    delete_topics(KAFKA_BROKER, TOPICS)
