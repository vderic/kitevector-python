import argparse
import json
import time
from kite import kite
from kite.xrg import xrg
from kitevector.index import client
from kitevector import vector as kv

if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--interval', type=int, default=10)
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

		while True:
			cli = client.IndexClient(schema, path, idx_hosts, fragcnt, filespec, index_params)
			responses = cli.status()
			ok_cnt = 0
			processing_cnt = 0
			record_processed = 0

			#print(responses)
			for r in responses:
				res = json.loads(r)
				if res['status'] == 'ok':
					ok_cnt += 1
				elif res['status'] == 'processing':
					processing_cnt += 1
				elif res['status'] == 'error':
					raise Exception(res['message'])


				record_processed += res['element_count']
			if ok_cnt == fragcnt:
				print("index done = {}/{}, elements processed = {}".format(ok_cnt, fragcnt, record_processed))
				print("Index is ready")
				break
			else:
				print("index done = {}/{}, elements processed = {}".format(ok_cnt, fragcnt, record_processed))

			time.sleep(args.interval)

	except Exception as e:
		print('Exception:', e)
	finally:
		if cli is not None:
			cli.close()
