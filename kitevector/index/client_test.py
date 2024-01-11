import json
from kite import kite
import client
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

	hosts = ["localhost:8878"]
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

		# create index
		cli = client.IndexClient(schema, path, hosts, fragcnt, filespec, index_params)
		cli.create_index()

		# query index
		cli = client.IndexClient(schema, path, hosts, fragcnt, filespec, index_params)
		ids, distances = cli.query([gen_embedding(dim)],3)
		print(ids, distances)

		# delete index
		cli = client.IndexClient(schema, path, hosts, fragcnt, filespec, index_params)
		cli.delete_index()
	except Exception as msg:
		print('Exception: ', msg)
	finally:
		if cli is not None:
			cli.close()

