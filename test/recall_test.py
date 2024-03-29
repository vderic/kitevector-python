import json
from kite import kite
from kite.xrg import xrg
from kitevector.index import client
from kitevector import vector as kv
import random
import math
import numpy as np

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

	random.seed(1)

	idx_hosts = ["localhost:8878"]
	kite_hosts = ['localhost:7878']
	fragcnt = 3
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	path = "tmp/vector/vector*.parquet"
	filespec = kite.ParquetFileSpec()
	
	# index specific setting
	index_params = {
		"name" : "movie",
		"metric_type": "ip",
		"index_type": "hnsw",
		"params":{
			"dimension": 1536,
			"max_elements": 100,
			"M": 16,
			"ef_construction": 200,
			"ef" : 50,
			"num_threads": 1,
			"k" : 10,
			"id_field" : "id",
			"vector_field": "embedding"
		}
	}

	cli = None

	try:
		ids, embeddings = load(schema, kite_hosts, filespec, path)

		# create indexx
		#cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, index_params)
		#cli.create_index()


		recall = 0
		for id, embedding in zip(ids, embeddings):	
			# query index
			cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, index_params)
			res_ids = cli.query([embedding.tolist()],1)
			if res_ids[0] == id:
				recall += 1

		print("recall = ", recall, " / ", len(ids))

		# delete index
		#cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, index_params)
		#cli.delete_index()
	except Exception as msg:
		print('New Exception: ', msg)
	finally:
		if cli is not None:
			cli.close()

