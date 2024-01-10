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

	idx_hosts = ["localhost:8181"]
	kite_hosts = ['localhost:7878']
	fragcnt = 3
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	path = "tmp/vector/vector*.parquet"
	
	# index specific setting
	space = 'ip'
	dim = 1536
	M = 16
	ef_construction = 200
	max_elements = 10000
	ef = 50
	num_threads = 1
	k = 10
	id_col = "id"     # column name of index column
	embedding_col = "embedding"   # column name of embedding column
	idxname = 'movie'    # index name and idenitifier

	config = client.IndexConfig(idxname, space, dim, M, ef_construction, 
		max_elements, ef, num_threads, k, id_col, embedding_col)

	cli = None
	embedding = gen_embedding(dim)

	try:
		filespec = kite.ParquetFileSpec()

		# create indexx
		#cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, config)
		#cli.create_index()

		# query index
		cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, config)
		ids = cli.query([embedding],3)
		print(ids)

		# kitevector
		vs = kv.KiteVector(schema, kite_hosts, fragcnt)
		vs.format(filespec).table(path).select(['id', 'docid']).order_by(kv.VectorExpr('embedding').inner_product(embedding)).limit(3)
		vs.index(idx_hosts, config)
		rows = vs.execute()
		print(rows)


		# delete index
		#cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, config)
		#cli.delete_index()
	except Exception as msg:
		print('New Exception: ', msg)
	finally:
		if cli is not None:
			cli.close()
