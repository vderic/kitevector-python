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

	hosts = ["localhost:8181"]
	fragcnt = 3
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	path = "tmp/vector/vector*.parquet"
	index_colref = {"id": "id", "embedding": "embedding"}
	
	space = 'ip'
	dim = 1536
	M = 16
	ef_construction = 200
	max_elements = 10000
	ef = 50
	num_threads = 1
	k = 10
	config = client.IndexConfig(space, dim, M, ef_construction, max_elements, ef, num_threads, k)

	cli = None
	try:
		filespec = kite.ParquetFileSpec()
		#cli = client.IndexClient("movie", schema, path, hosts, fragcnt, index_colref, filespec, config)
		#cli.create_index()
		cli = client.IndexClient("movie", schema, path, hosts, fragcnt, index_colref, filespec, config)
		cli.query([gen_embedding(dim)])
	except Exception as msg:
		print('New Exception: ', msg)
	finally:
		if cli is not None:
			cli.close()
