from kafka import TopicPartition,OffsetAndMetadata
from concurrent.futures import ThreadPoolExecutor
import configparser
import requests
import json
import KafkaInput
import OpenStreamOutput


class CDCStream:
	def __init__(self,consumer,index,topic,openSearchOutput):
		self.consumer = consumer
		self.index = index
		self.topic=topic
		self.openSearchOutput = openSearchOutput

	def read_events(self):
		print("Inside")
		executor = ThreadPoolExecutor(max_workers=3)
		for each_cdc_stream in self.consumer:
			self.index=self.index+1
			topic_partition = TopicPartition(self.topic, each_cdc_stream.partition)
			offset = OffsetAndMetadata(each_cdc_stream.offset + 1, None)
			topic_offset = {topic_partition: offset}
			print(each_cdc_stream.value,topic_offset);
			executor.submit(CDCStream.__process_events, each_cdc_stream.value,topic_offset,str(self.index),self.openSearchOutput)

	def __process_events(cdc_stream,topic_offset,index,openSearchOutput):
		if(CDCStream.__push_downstream(cdc_stream,index,openSearchOutput)):
			cdcdata_consumer.commit(offsets=topic_offset)# commit the offset

	#Asynchromous way can be added but just to ensure that record got inserted in opensearch we are waiting for the result.
	def __push_downstream(cdc_stream,index,openSearchOutput):
		#Retry meachnism can be added in case of failure...
		try:
			openSearchOutput.ingestOutput(cdc_stream,index)
		except Exception as e: 
			print(e)

def main():
	config = configparser.ConfigParser()
	config.read('env-test.txt')
	input_stream=config.get("source-config","source")
	output_stream=config.get("sink-config","sink")
	openSearchOutput = OpenStreamOutput.OpenStreamOutput(config.get("sink-config","opensearch_url"))

	#Using inheritance it can be extended to other input sources as well...
	if input_stream=="kafka":
		topic=config.get("source-config","topic")
		kafkaInput = KafkaInput.KafkaInput(config.get("source-config","bootstrap_servers"),topic)
		consumer = kafkaInput.createConsumer();
		#Can be read from file or blob.So in case of fialure we can fetch the last index and update accordingly.Even index needs to be updated in case ingestion to opensearh fails...
		index=0;
		cdcStreamProcessor = CDCStream(consumer,index,topic,openSearchOutput);
		cdcStreamProcessor.read_events();

if __name__=="__main__":
	main()
