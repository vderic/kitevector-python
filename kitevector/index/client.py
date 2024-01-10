from dataclasses import dataclass
import http.client
import json
from json import JSONEncoder
import selectors
import copy
from kite import kite
from kitevector.index import index

class IndexRequest:

	def __init__(self, schema, path, fragid, fragcnt, filespec, config):
		self.schema = schema
		self.path = path
		self.fragment = [fragid, fragcnt]
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
	name: str
	space: str             # possible options are l2, cosine or ip
	dimension : int        # number of dimension
	M : int                # parameter that defines the maximum number of outgoing connections in the graph
	ef_construction: int   # parameter that controls speed/accuracy trade-off during the index construction
	max_elements: int      # max number of elements 
	ef: int                # ef should always be > k
	num_threads: int       # default number of threads to use in add_items or knn_query. Not that calling p.set_num_threads(3) is equaivalent to p.num_threads=3.
	k: int                 # number of the closest elements
	id: str
	embedding: str
	
	def dict(self):
		return self.__dict__

class IndexClient:
	
	def __init__(self, schema, path, hosts, fragcnt, filespec, config):
		self.selectors = selectors.DefaultSelector()
		self.connections = []
		self.responses = []
		self.batches = []
		self.fragcnt = fragcnt
		self.hosts = []
		self.config = config
		nhost = len(hosts)

		self.requests = []
		for i in range(fragcnt):
			host = hosts[i % nhost]
			hostport = host.split(':')
			h = hostport[0]
			p = int(hostport[1])
			self.hosts.append((h, p))
			self.requests.append(IndexRequest(schema, path, i, fragcnt, filespec, config))

	def get_config(self):
		return self.config
			
	def query(self, embedding, k=None):

		for host, req in zip(self.hosts, self.requests):
			conn = http.client.HTTPConnection(host[0], host[1])
			headers = {'Content-Type': 'application/json'}
			req.set_embedding(embedding)
			if k is not None:
				req.config['k'] = k
			json_data = json.dumps(req, cls=IndexRequestEncoder)

			conn.request('POST', '/query', json_data, headers)
			self.connections.append(conn)

		for c in self.connections:
			r = c.getresponse()
			self.responses.append(r)
			self.selectors.register(r, selectors.EVENT_READ, self.read)


		sort = [index.IndexSort(k) for i in range(len(embedding))]
		while True:
			r = self.next()
			if r is None:
				break

			# got result from Hnsw index and do heap sort to get nbest
			response = json.loads(r)
			if response['status'] != 'ok':
				raise Exception("server error")

			for s, ids, distances in zip(sort, response['ids'], response['distances']):
				s.add(ids, distances)

		ids = []
		distances = []
		for s in sort:
			id, distance = s.get()
			ids.append(id)
			distances.append(distance)

		return ids, distances
		

	def create_index(self):
		for host, req in zip(self.hosts, self.requests):
			conn = http.client.HTTPConnection(host[0], host[1])
			headers = {'Content-Type': 'application/json'}
			json_data = json.dumps(req, cls=IndexRequestEncoder)
			conn.request('POST', '/create', json_data, headers)
			self.connections.append(conn)

		for c in self.connections:
			r = c.getresponse()
			self.responses.append(r)
			self.selectors.register(r, selectors.EVENT_READ, self.read)


		while True:
			r = self.next()
			if r is None:
				break

			# got result from Hnsw index and do heap sort to get nbest
			response = json.loads(r)
			if response['status'] != 'ok':
				raise Exception("create_index: server error")

	def delete_index(self):
		for host, req in zip(self.hosts, self.requests):
			conn = http.client.HTTPConnection(host[0], host[1])
			headers = {'Content-Type': 'application/json'}
			json_data = json.dumps(req, cls=IndexRequestEncoder)
			conn.request('DELETE', '/delete', json_data, headers)
			self.connections.append(conn)

		for c in self.connections:
			r = c.getresponse()
			self.responses.append(r)
			self.selectors.register(r, selectors.EVENT_READ, self.read)


		while True:
			r = self.next()
			if r is None:
				break

			# got result from Hnsw index and do heap sort to get nbest
			response = json.loads(r)
			if response['status'] != 'ok':
				raise Exception("delete_index: server error")

	def read(self, response, mask):
		try:
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
