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

def load(schema,hosts, filespec, path):
	sql = '''select id, embedding from "{}"'''.format(path)

	ids = []
	embeddings = []
	kitecli = kite.KiteClient()

	try:

		kitecli.host(hosts).sql(sql).schema(schema).filespec(filespec).fragment(-1,3).submit()

		while True:
			iter = kitecli.next_batch()
			if iter is None:
				break
			else:
				ids.extend(iter.value_array[0])
				embeddings.extend(iter.value_array[1])
				
		return ids, np.float32(embeddings)

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

	ids, embeddings = load(schema, hosts, kite.ParquetFileSpec(), path)

	print(len(ids), " records loaded")

	p = hnswlib.Index(space = 'ip', dim = dim) # possible options are l2, cosine or ip
	p.init_index(max_elements = len(ids), ef_construction = 200, M = 16)

	p.add_items(embeddings, ids)
	# Controlling the recall by setting ef:
	p.set_ef(50) # ef should always be > k

	# Query dataset, k - number of the closest elements (returns 2 numpy arrays)
	labels, distances = p.knn_query(embeddings, k = 1)

	recall = 0
	for id, label in zip(ids, labels.reshape(-1)):
		if id == label:
			recall += 1

	print("Recall for the batch: ", recall, "/", len(ids))

	#print(labels[0])
	#print(distances[0])

	embed = gen_embedding(dim)
	labels, distances = p.knn_query(embed, k = 10)
	index_filter = vector.ScalarArrayOpExpr('id', labels.reshape(-1))

	vs = vector.KiteVector(schema, hosts, 3)
	vs.format(kite.ParquetFileSpec()).table(path).select(['id', 'docid']).order_by(vector.Embedding('embedding').inner_product(embed))
	vs.filter(index_filter).limit(6)
	rows, scores = vs.execute()
	print(rows)
	print(scores)

	# for postgres, generate SQL like below
	pg = vector.PgVector()
	sql = pg.table(path).select(['id', 'docid']).order_by(vector.Embedding('embedding').inner_product(embed)).filter(index_filter).limit(6).sql()
	print(sql)




	# Index objects support pickling
	# WARNING: serialization via pickle.dumps(p) or p.__getstate__() is NOT thread-safe with p.add_items method!
	# Note: ef parameter is included in serialization; random number generator is initialized with random_seed on Index load
	p_copy = pickle.loads(pickle.dumps(p)) # creates a copy of index p using pickle round-trip

	### Index parameters are exposed as class properties:
	print(f"Parameters passed to constructor:  space={p_copy.space}, dim={p_copy.dim}") 
	print(f"Index construction: M={p_copy.M}, ef_construction={p_copy.ef_construction}")
	print(f"Index size is {p_copy.element_count} and index capacity is {p_copy.max_elements}")
	print(f"Search speed/quality trade-off parameter: ef={p_copy.ef}")
