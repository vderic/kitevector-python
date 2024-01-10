import argparse
import json
from kite import kite
from kite.xrg import xrg
from kitevector.index import client
from kitevector import vector as kv

if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument('--delete', action='store_true')
	args = parser.parse_args()

	idx_hosts = ["localhost:8878"]
	kite_hosts = ['localhost:7878']
	fragcnt = 3
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	path = "tmp/vector/vector*.parquet"
	filespec = kite.ParquetFileSpec()

	# index specific setting
	space = 'ip'
	dim = 1536
	M = 16
	ef_construction = 200
	max_elements = 100
	ef = 50
	num_threads = 1
	k = 10
	id_col = "id"     # column name of index column
	embedding_col = "embedding"   # column name of embedding column
	idxname = 'movie'    # index name and idenitifier

	config = client.IndexConfig(idxname, space, dim, M, ef_construction,
		max_elements, ef, num_threads, k, id_col, embedding_col)

	cli = None

	try:

		cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, config)

		if args.delete:
			# delete index
			cli.delete_index()
		else:
			# create indexx
			cli.create_index()
	except Exception as e:
		print('Exception:', e)
	finally:
		if cli is not None:
			cli.close()
