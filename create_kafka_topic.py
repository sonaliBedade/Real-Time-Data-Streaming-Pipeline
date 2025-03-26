from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers="localhost:29092")
topic = NewTopic(name="processed-user-login", num_partitions=1, replication_factor=1)

admin_client.create_topics([topic])
print("Topic created successfully!")
admin_client.close()
