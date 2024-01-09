import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
import json
import numpy as np
import pickle
import hnswlib
from kite import kite
from kite.xrg import xrg
from functools import partial
import argparse
import threading

class Index:

	datadir = None
	kite_port = 0
	indexes = {}
	idxlocks = {}

	@classmethod
	def init(cls, datadir, kite_port):
		cls.datadir = datadir
		cls.kite_port = kite_port

		cls.load(datadir)

	@classmethod
	def get_indexkey(cls, req):
		return '{}_{}_{}'.format(req['name'], req['fragment'][0], req['fragment'][1])

	@classmethod
	def get_lock(cls, idxname):
		if cls.idxlocks.get(idxname) == None:
			cls.idxlocks[idxname] = threading.Lock()
		return cls.idxlocks[idxname]

	@classmethod
	def load(cls, datadir):
		if not os.path.isdir(datadir):
			raise Exception("data directory not exists")
		
		idxlist = ['movieindex_0_3', 'movieindex_1_3', 'movieindex_2_3']
		for idx in idxlist:
			with cls.get_lock(idx):
				print("KiteIndex.load")
				# load the index inside the lock

	@classmethod
	def query(cls, req):	
		idx = None
		idxname = cls.get_indexkey(req)
		print(idxname)
		with cls.get_lock(idxname):
			idx = cls.indexes[idxname]

		# found the index and get the nbest
		print("KiteIndex.query")

	@classmethod
	def create(cls, req):
		print("KiteIndex.create")
		idxname = cls.get_indexkey(req)
		with cls.get_lock(idxname):
			# create index inside the lock

			schema = req['schema']
			filespec = kite.FileSpec.fromJSON(req['filespec'])
			fragment = req['fragment']
			host = ['localhost:{}'.format(cls.kite_port)]
			path = req['path']
			idxcfg = req['config']
			space = idxcfg['space']
			dim = idxcfg['dimension']
			max_elements = idxcfg['max_elements']
			ef_construction = idxcfg['ef_construction']
			M = idxcfg['M']
			p = hnswlib.Index(space=space, dim = dim)
			p.init_index(max_elements=max_elements, ef_construction=ef_construction, M=M)

			colref = req['colref']
			idcol = colref['id']
			embeddingcol = colref['embedding']
			sql = '''SELECT {}, {} FROM "{}"'''.format(idcol, embeddingcol, path)
			
			kitecli = kite.KiteClient()
			try:
				kitecli.host(host).sql(sql).schema(schema).filespec(filespec).fragment(fragment[0], fragment[1]).submit()

				while True:
					iter = kitecli.next_batch()
					if iter is None:
						break
					else:
						p.add_items(np.float32(iter.value_array[1]), iter.value_array[0])
			except Exception as msg:
				print(msg)
				raise
			finally:
				kitecli.close()

			fname = '{}.hnsw'.format(idxname)
			fname = os.path.join(cls.datadir, fname)
			print("saving index file ", fname)
			with open(fname, 'wb') as fp:
				pickle.dump(p, fp)

			cls.indexes[idxname] = p

	@classmethod
	def delete(cls, req):
		print("KiteIndex.delete")
	
class RequestHandler(BaseHTTPRequestHandler):

	def create_index(self):
		content_length = int(self.headers.get("Content-Length"))
		body = self.rfile.read(content_length)
		print(str(body))

		try:
			Index.create(json.loads(body))

			status = {'status': 'ok'}
			msg = json.dumps(status).encode('utf-8')
		
			self.send_response(200)
			self.send_header("Content-Length", len(msg))
			self.send_header("Content-Type", "application/json")
			self.end_headers()
			self.wfile.write(msg)
		except Exception as e1:
			print('KeyError: ', e1)
			self.send_response(402)
			status = b'''{'status': 'error'}'''
			self.wfile.write(status)
			

	def query(self):
		content_length = int(self.headers.get("Content-Length"))
		body = self.rfile.read(content_length)
		print(str(body))

		try:
			Index.query(json.loads(body))

			msg=b'''[[0.3, 1], [0.4, 2]]'''

			self.send_response(200)
			self.send_header("Content-Length", len(msg))
			self.send_header("Content-Type", "application/json")
			self.end_headers()
			self.wfile.write(msg)
		except KeyError as e1:
			print('KeyError: ', e1)
			self.send_response(402)
			status = b'''{'status': 'error'}'''
			self.wfile.write(status)
		except Exception as e2:
			print(e2)
			self.send_response(402)
			status = b'''{'status': 'error'}'''
			self.wfile.write(status)
			

	
	def do_DELETE(self):
		if self.path == '/delete':
			self.delete_index()

	def do_POST(self):
		print("path = " , self.path)

		if self.path == '/create':
			self.create_index()
		elif self.path == '/query':
			self.query()
		else:
			pass

#class ThreadingHttpServer(ThreadingMixIn, HTTPServer):
#	pass

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', '--port', type=int, default=8181)
	parser.add_argument('--kite', type=int, default=7878)
	parser.add_argument('datadir')
	args = parser.parse_args()

	Index.init(args.datadir, args.kite)
	httpd = ThreadingHTTPServer(('', args.port), RequestHandler)

	try:
		httpd.serve_forever()
	except KeyboardInterrupt:
		pass

	httpd.server_close()

