from kafka import KafkaConsumer, TopicPartition
import json

class KafkaInput:
	def __init__(self, bootstrap_server, topic):
		self.bootstrap_server = bootstrap_server
		self.topic = topic

	def createConsumer(self):
		cdcdata_consumer = KafkaConsumer(
		bootstrap_servers=self.bootstrap_server,
		group_id="cdcstream-data-group",
		auto_offset_reset='earliest',
		enable_auto_commit=False,
		value_deserializer=lambda m: json.loads(m.decode('utf-8'))
		)
		partitions = [TopicPartition(self.topic, partition) for partition in
                           cdcdata_consumer.partitions_for_topic(self.topic)]
		cdcdata_consumer.assign(partitions)
		return cdcdata_consumer;
