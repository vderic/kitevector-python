import json
from kite import kite
from kite.xrg import xrg
from kitevector.index import client
from kitevector import vector as kv
import random
import math

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


if __name__ == "__main__":

	random.seed(1)

	idx_hosts = ["localhost:8878"]
	kite_hosts = ['localhost:7878']
	fragcnt = 3
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	path = "tmp/vector/vector*.parquet"

	dim = 1536
	
	# index specific setting
	index_params = {
		"name" : "movie",
		"metric_type": "ip",
		"index_type": "hnsw",
		"params":{
			"dimension": dim,
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

	# flat index
	flat_index_params = {
		"index_type": "flat",
		"metric_type": "ip",
		"params":{
			"id_field" : "id",
			"vector_field": "embedding"
		}
	}

	cli = None
	embedding = gen_embedding(dim)

	try:
		filespec = kite.ParquetFileSpec()

		# create indexx
		#cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, index_params)
		#cli.create_index()

		# flat index or no index brute force search
		vs = kv.KiteVector(schema, kite_hosts, fragcnt)
		vs.format(filespec).table(path).select(['id', 'docid']).limit(3)
		vs.index(flat_index_params, embedding)
		rows = vs.execute()
		print(rows)

		# query index
		# cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, index_params)
		# ids = cli.query([embedding],3)
		# print(ids)

		# kitevector
		vs = kv.KiteVector(schema, kite_hosts, fragcnt)
		vs.format(filespec).table(path).select(['id', 'docid']).limit(3)
		vs.index(index_params, embedding, hosts=idx_hosts)
		rows = vs.execute()
		print(rows)


		# delete index
		#cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, index_params)
		#cli.delete_index()
	except Exception as msg:
		print('Exception: ', msg)
	finally:
		if cli is not None:
			cli.close()

