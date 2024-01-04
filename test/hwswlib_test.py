import hnswlib
import random
import math
import numpy as np
import pickle
from kite import kite
from kite.xrg import xrg
from kitevector import vector

def gen_embedding(nitem):
	ret = []
	for x in range(nitem):
		ret.append(random.uniform(-1,1))
	sum = 0
	for x in ret:
		sum += x*x
	sum = math.sqrt(sum)
	# normalize
	for i in range(len(ret)):
		ret[i] = ret[i] / sum
	return ret

def build_index(schema,hosts, filespec, path, dim):
	sql = '''select id, embedding from "{}"'''.format(path)
	num_elements = 10000

	kitecli = kite.KiteClient()

	p = hnswlib.Index(space = 'ip', dim = dim) # possible options are l2, cosine or ip
	p.init_index(max_elements = num_elements, ef_construction = 200, M = 16)

	try:

		kitecli.host(hosts).sql(sql).schema(schema).filespec(filespec).fragment(-1,3).submit()

		while True:
			iter = kitecli.next_batch()
			if iter is None:
				break
			else:
				ids = iter.value_array[0]
				embeddings = iter.value_array[1]
				p.add_items(embeddings, ids)
				
		return p

	except Exception as msg:
		print(msg)
	finally:
		kitecli.close()




if __name__ == "__main__":

	dim = 1536
	schema = [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]', 0, 0)]
	hosts = ["localhost:7878"]
	path = 'tmp/vector/vector*.parquet'

	random.seed(1)

	p = build_index(schema, hosts, kite.ParquetFileSpec(), path, dim)

	# Controlling the recall by setting ef:
	p.set_ef(50) # ef should always be > k

	# Query dataset, k - number of the closest elements (returns 2 numpy arrays)
	embedding = gen_embedding(dim)
	labels, distances = p.knn_query(embedding, k = 20)
	print(labels[0])
	print(distances[0])

	filter = ['id IN (' + ','.join([str(id) for id in labels[0]]) + ')']

	vs = vector.KiteVector(schema, hosts, path, kite.ParquetFileSpec(), 3)
	rows, scores = vs.inner_product(["embedding", embedding], ['id', 'docid'], nbest=10, filter=filter)

	print(rows)
	print(scores)

	# Index objects support pickling
	# WARNING: serialization via pickle.dumps(p) or p.__getstate__() is NOT thread-safe with p.add_items method!
	# Note: ef parameter is included in serialization; random number generator is initialized with random_seed on Index load
	p_copy = pickle.loads(pickle.dumps(p)) # creates a copy of index p using pickle round-trip

	### Index parameters are exposed as class properties:
	print(f"Parameters passed to constructor:  space={p_copy.space}, dim={p_copy.dim}") 
	print(f"Index construction: M={p_copy.M}, ef_construction={p_copy.ef_construction}")
	print(f"Index size is {p_copy.element_count} and index capacity is {p_copy.max_elements}")
	print(f"Search speed/quality trade-off parameter: ef={p_copy.ef}")
