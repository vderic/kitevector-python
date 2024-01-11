from dataclasses import dataclass
import http.client
import json
from json import JSONEncoder
import selectors
import copy
from kite import kite
from kitevector.index import index

class IndexRequest:

	def __init__(self, schema, path, fragid, fragcnt, filespec, index_params):
		self.schema = schema
		self.path = path
		self.fragment = [fragid, fragcnt]
		self.index_params = index_params
		self.filespec = filespec.toJSON()
		self.embedding = []

	def set_embedding(self, embedding):
		self.embedding = embedding

class IndexRequestEncoder(JSONEncoder):
	def default(self, o):
		return o.__dict__

class IndexClient:
	
	def __init__(self, schema, path, hosts, fragcnt, filespec, index_params):
		self.selectors = selectors.DefaultSelector()
		self.connections = []
		self.responses = []
		self.batches = []
		self.fragcnt = fragcnt
		self.hosts = []
		self.index_params = index_params
		nhost = len(hosts)

		self.requests = []
		for i in range(fragcnt):
			host = hosts[i % nhost]
			hostport = host.split(':')
			h = hostport[0]
			p = int(hostport[1])
			self.hosts.append((h, p))
			self.requests.append(IndexRequest(schema, path, i, fragcnt, filespec, index_params))

	def get_index_params(self):
		return self.index_params
			
	def query(self, embedding, k=None):

		for host, req in zip(self.hosts, self.requests):
			conn = http.client.HTTPConnection(host[0], host[1])
			headers = {'Content-Type': 'application/json'}
			req.set_embedding(embedding)
			params = req.index_params['params']
			if k is not None:
				params['k'] = k
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

	def status(self):
		for host, req in zip(self.hosts, self.requests):
			conn = http.client.HTTPConnection(host[0], host[1])
			headers = {'Content-Type': 'application/json'}
			json_data = json.dumps(req, cls=IndexRequestEncoder)
			conn.request('POST', '/status', json_data, headers)
			self.connections.append(conn)

		for c in self.connections:
			r = c.getresponse()
			self.responses.append(r)
			self.selectors.register(r, selectors.EVENT_READ, self.read)


		responses = []
		while True:
			r = self.next()
			if r is None:
				break

			responses.append(r)

		return responses

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
