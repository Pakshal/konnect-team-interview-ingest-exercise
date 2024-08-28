from kafka import KafkaProducer
import configparser


import json

def main():
	config = configparser.ConfigParser()
	config.read('env-test.txt')
	input_stream=config.get("source-config","source")

	producer = KafkaProducer(bootstrap_servers=config.get("source-config","bootstrap_servers"),value_serializer=lambda v: json.dumps(v).encode('utf-8'))

	with open('./stream.jsonl', 'r') as json_file:
		json_list = list(json_file)

	for json_str in json_list:
		result = json.loads(json_str)
		print(f"result: {result}")
		producer.send(config.get("source-config","topic"),result)


if __name__=="__main__":
	main()
