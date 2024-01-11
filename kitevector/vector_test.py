import sys
import random
import math
from kite import kite
from kite.xrg import xrg

import vector as kv

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
	path = 'tmp/vector/vector*.parquet'
	filespec = kite.ParquetFileSpec()
	kite_hosts = ['localhost:7878']
	idx_hosts = ['localhost:8878']
	schema = [{'name':'id', 'type': 'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]

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
			"embedding_field": "embedding"
		}
	}

	vs = kv.KiteVector(schema, kite_hosts, 3)
	embed = gen_embedding(1536)
	vs.format(filespec).select([kv.Var('id'), kv.OpExpr('+', 'docid', 2)]).table(path).index(index_params, embed, idx_hosts).limit(5)
	#print(vs.sql())
	results = vs.execute()
	print(results)
