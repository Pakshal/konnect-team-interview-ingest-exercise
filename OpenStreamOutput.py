import requests

class OpenStreamOutput:
	def __init__(self,outputUrl):
		self.outputUrl=outputUrl

	def ingestOutput(self,cdc_stream,index):
		res=requests.put('http://'+self.outputUrl+'/cdc/_doc/'+index,json=cdc_stream)
		if(res.status_code==200):
			print("Insertion Sucessful")
			return True
		return False
