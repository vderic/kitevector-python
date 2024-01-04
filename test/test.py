import json
import sys
import random
import math
import numpy as np
import pandas as pd

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

if __name__ == "__main__":

	random.seed(1)

	path = 'tmp/vector/vector*.parquet'
	#csvspec = kite.CsvFileSpec()
	parquetspec = kite.ParquetFileSpec()
	hosts = ['localhost:7878']
	fragcnt = 3
	schema =  [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]')]


	try:
		embedding = ["embedding", gen_embedding(1536)]
		id_cname = "id"
		threshold = -1
		fragcnt = 3
		index = None
		nbest = 3

		vs = vector.KiteVector(schema, hosts, path, parquetspec, fragcnt)
		ids, scores = vs.inner_product(embedding, id_cname, threshold, nbest)
		print(ids)
		print(scores)

		filter = 'id IN (999, 4833)'
		vs = vector.KiteVector(schema, hosts, path, parquetspec, fragcnt)
		ids, scores = vs.inner_product(embedding, id_cname, threshold, nbest, filter=filter)
		print(ids)
		print(scores)
	except Exception as msg:
		print(msg)
