import http.client
import json
import selectors

class KiteIndexClient:
	
	def __init__(self):
		self.selectors = selectors.DefaultSelector()
		self.connections = []
		self.responses = []
		self.batches = []


	def query(self):

		for i in range(2):
			
			conn = http.client.HTTPConnection('localhost', 8181)

			headers = {'Content-type': 'application/json'}

			foo = {'text': 'Hello HTTP #1 **cool**, and #1!'}
			json_data = json.dumps(foo)

			conn.request('POST', '/query', json_data, headers)

			self.connections.append(conn)

		for c in self.connections:
			r = c.getresponse()
			self.responses.append(r)
			self.selectors.register(r, selectors.EVENT_READ, self.read)


		while True:
			r = client.next()
			if r is None:
				break

			# got result from Hnsw index and do heap sort to get nbest
			print(r)
		

	def build_index(self):
		pass

	def delete_index(self):
		pass

	def read(self, response, mask):
		try:
			print(response.fileno())
			return response.read().decode()
		except Exception as msg:
			print(msg)
			raise

		return None

	def next(self):

		if len(self.batches) != 0:
			return self.batches.pop()

		while True:
			if len(self.responses) == 0:
				break

			events = self.selectors.select(None)

			for key, mask in events:
				callback = key.data
				res = callback(key.fileobj, mask)
				self.batches.append(res)
				self.selectors.unregister(key.fileobj)
				try:
					key.fileobj.close()
				except OSError as msg:
					print(msg)

				self.responses.remove(key.fileobj)
			
			if len(self.batches) > 0:
				return self.batches.pop()

		if len(self.batches) == 0:
			return None

		return self.batches.pop()

	def close(self):
		for c in self.connections:
			c.close()

		self.selectors.close()
				

if __name__ == "__main__":
	
	client = KiteIndexClient()
	try:
		client.query()
	except Exception as msg:
		print(msg)
	finally:
		client.close()

