from dataclasses import dataclass
import http.client
import json
from json import JSONEncoder
import selectors
import copy
from kite import kite

class IndexRequest:

	def __init__(self, name, schema, path, fragid, fragcnt, colref, filespec, config):
		self.name = name
		self.schema = schema
		self.path = path
		self.fragment = [fragid, fragcnt]
		self.colref = colref
		self.config = config.dict()
		self.filespec = filespec.toJSON()
		self.embedding = []

	def set_embedding(self, embedding):
		self.embedding = embedding

class IndexRequestEncoder(JSONEncoder):
	def default(self, o):
		return o.__dict__

@dataclass
class IndexConfig:
	space: str             # possible options are l2, cosine or ip
	dimension : int        # number of dimension
	M : int                # parameter that defines the maximum number of outgoing connections in the graph
	ef_construction: int   # parameter that controls speed/accuracy trade-off during the index construction
	max_elements: int      # max number of elements 
	ef: int                # ef should always be > k
	num_threads: int       # default number of threads to use in add_items or knn_query. Not that calling p.set_num_threads(3) is equaivalent to p.num_threads=3.
	k: int                 # number of the closest elements
	
	def dict(self):
		return self.__dict__

class IndexClient:
	
	def __init__(self, name, schema, path, hosts, fragcnt, colref, filespec, config):
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
			h = hostport[0]
			p = int(hostport[1])
			self.hosts.append((h, p))
			self.requests.append(IndexRequest(name, schema, path, i, fragcnt, colref, filespec, config))
			
	def query(self, embedding):

		for host, req in zip(self.hosts, self.requests):
			conn = http.client.HTTPConnection(host[0], host[1])
			headers = {'Content-Type': 'application/json'}
			req.set_embedding(embedding)
			json_data = json.dumps(req, cls=IndexRequestEncoder)
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
		

	def create_index(self):
		for host, req in zip(self.hosts, self.requests):
			conn = http.client.HTTPConnection(host[0], host[1])
			headers = {'Content-Type': 'application/json'}
			req.set_embedding(embedding)
			json_data = json.dumps(req, cls=IndexRequestEncoder)
			conn.request('POST', '/create', json_data, headers)
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

	def delete_index(self):
		pass

	def read(self, response, mask):
		try:
			print(response.fileno())
			return response.read().decode()
		except Exception as msg:
			print("Exception: ", msg)
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
				# close socket later with connection.close()
				#try:
				#	key.fileobj.close()
				#except OSError as msg:
				#	print(msg)

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
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	path = "tmp/vector/vector*.parquet"
	index_colref = {"id": "id", "embedding": "embedding"}
	
	space = 'ip'
	dim = 1536
	M = 16
	ef_construction = 200
	max_elements = 10000
	ef = 50
	num_threads = 1
	k = 10
	config = IndexConfig(space, dim, M, ef_construction, max_elements, ef, num_threads, k)

	embedding = [1.0333,2.3455,3.334]

	client = None
	try:
		filespec = kite.ParquetFileSpec()
		client = IndexClient("movie", schema, path, hosts, fragcnt, index_colref, filespec, config)
		client.create_index()
		#client = IndexClient("movieindex", schema, path, hosts, fragcnt, index_colref, filespec, config)
		#client.query(embedding)
	except Exception as msg:
		print('Exception: ', msg)
	finally:
		if client is not None:
			client.close()

