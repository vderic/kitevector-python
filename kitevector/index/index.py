import os
import json
import numpy as np
import pickle
import hnswlib
from kite import kite
from kite.xrg import xrg
from functools import partial
import argparse
import threading
import glob
import heapq

class IndexSort:

	def __init__(self, nbest):
		self.heap = []
		self.nbest = nbest

	def add(self, ids, distances):
		for id, score in zip(ids, distances):
			if len(self.heap) <= self.nbest:
				heapq.heappush(self.heap, (score, id))
			else:
				heapq.heapreplace(self.heap, (score, id))
		
	def get(self):
		if len(self.heap) == self.nbest+1:
			heapq.heappop(self.heap)

		scores = []
		ids = []
		for i in range(len(self.heap)):
			t = heapq.heappop(self.heap)
			scores.append(t[0])
			ids.append(t[1])

		return ids, scores


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
		cfg = req['config']
		return '{}_{}_{}'.format(cfg['name'], req['fragment'][0], req['fragment'][1])

	@classmethod
	def get_lock(cls, idxname):
		lock = cls.idxlocks.get(idxname)
		if lock == None:
			lock = threading.Lock()
			cls.idxlocks[idxname] = lock
		return lock

	@classmethod
	def load(cls, datadir):
		if not os.path.isdir(datadir):
			raise Exception("data directory not exists")
		
		flist = glob.glob('*.hnsw', root_dir = cls.datadir)
		print(flist)
		for f in flist:
			idxname = os.path.splitext(os.path.basename(f))[0]
			fpath = os.path.join(cls.datadir, f)
			with cls.get_lock(idxname):
				print("KiteIndex.load ", idxname)
				# load the index inside the lock
				with open(fpath, 'rb') as fp:
					idx = pickle.load(fp)
					cls.indexes[idxname] = idx

	@classmethod
	def query(cls, req):	
		idx = None
		idxname = cls.get_indexkey(req)
		print(idxname)
		with cls.get_lock(idxname):
			idx = cls.indexes[idxname]

		# found the index and get the nbest
		print("KiteIndex.query")
		embedding = np.float32(req['embedding'])
		ef = req['config']['ef']
		k  = req['config']['k']
		idx.set_ef(ef)
		ids, distances = idx.knn_query(embedding, k=k)
		return ids, distances

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

			idcol = idxcfg['id']
			embeddingcol = idxcfg['embedding']
			sql = '''SELECT {}, {} FROM "{}"'''.format(idcol, embeddingcol, path)
			
			# TODO: check max_elements and resize as needed
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
		idxname = cls.get_indexkey(req)
		print("KiteIndex.delete: ", idxname)
		with cls.get_lock(idxname):
			fpath = os.path.join(cls.datadir, '{}.hnsw'.format(idxname))
			if os.path.exists(fpath):
				os.remove(fpath)
			cls.indexes.pop(idxname)
			cls.idxlocks.pop(idxname)
			

if __name__ == "__main__":

	ids1 = [1,2,3,4,5,6,7,8]
	scores1 = [0.3, 0.1, 0.5, 0.2, 0.7, 0.0, 0.4, 0.8]
	ids2 = [10,20,30,40,50,60,70,80]
	scores2 = [0.32, 0.15, 0.57, 0.23, 0.73, 0.3, 0.54, 0.48]
	sort = IndexSort(5)

	sort.add(ids1, scores1)
	sort.add(ids2, scores2)

	ids, scores = sort.get()

	print(ids, scores)

	
