from dataclasses import dataclass
import http.client
import json
from json import JSONEncoder
import selectors
import copy

class IndexRequest:

	def __init__(self, schema, path, fragid, fragcnt, colref, config):
		self.schema = self.to_schema(schema)
		self.path = path
		self.fragment = [fragid, fragcnt]
		self.colref = colref
		self.config = config.dict()

	def to_schema(self, schema):
		s = []
		for c in schema:
			obj = {}
			obj['name'] = c[0]
			obj['type'] = c[1]
			s.append(obj)

		return s

	def json(self):
		return json.dumps(self.__dict__)

@dataclass
class IndexConfig:
	ef_construction: int
	dimension : int
	
	def dict(self):
		return self.__dict__

class IndexClient:
	
	def __init__(self, schema, path, hosts, fragcnt, colref, config):
		self.selectors = selectors.DefaultSelector()
		self.connections = []
		self.responses = []
		self.batches = []
		self.fragcnt = fragcnt
		self.hosts = []
		nhost = len(hosts)

		self.requests = []
		for i in range(fragcnt):
			host = hosts[i % nhost]
			hostport = host.split(':')
			self.hosts.append(hostport)
			self.requests.append(IndexRequest(schema, path, i, fragcnt, colref, config))
			
	def query(self):

		for host, req in zip(self.hosts, self.requests)
			conn = http.client.HTTPConnection(host[0], int(host[1]))
			headers = {'Content-type': 'application/json'}
			json_data = json.dumps(req)
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
		

	def create_index(self, config):
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

	hosts = ["localhost:8181"]
	fragcnt = 3
	schema = [("id", "int64"), ("docid", "int64"), ("embedding", "float[]")]
	path = "tmp/vector/vector*.csv"
	index_colref = {"id": "id", "embedding": "embedding"}
	
	config = IndexConfig(1.0, 1536)
	req = IndexRequest(schema, path, 2, fragcnt, index_colref, config)
	print(req.json())
	#print(req.json(config))

	client = IndexClient(schema, path, hosts, fragcnt, index_colref, config)
	try:
		#client.create_index()
		client.query()
	except Exception as msg:
		print(msg)
	finally:
		client.close()

