import json
import sys
import random
import numpy as np
import pandas as pd

from kite import kite
from kite.xrg import xrg
from kitevector import vector

def gen_embedding(nitem):
	ret = []
	for x in range(nitem):
		ret.append(random.random())
	sum = 0
	for x in ret:
		sum += x
	# normalize
	for i in range(len(ret)):
		ret[i] = ret[i] / sum
	return ret

if __name__ == "__main__":

	path = 'tmp/vector/vector*.parquet'
	#csvspec = kite.CsvFileSpec()
	parquetspec = kite.ParquetFileSpec()
	hosts = ['localhost:7878']
	fragcnt = 3


	try:
		embedding = gen_embedding(1536)
		threshold = -1
		fragcnt = 3
		index = None
		nbest = 3

		vs = vector.KiteVector(hosts, path, parquetspec, fragcnt)
		ids, scores = vs.inner_product(embedding, threshold, nbest)
		print(ids)
		print(scores)

		#ids = [1,2]
		#vs = vector.KiteVector(hosts, path, parquetspec, fragcnt)
		#res = vs.inner_product(embedding, threshold, nbest, ids=ids)
		#print(res)

		#docids = [10,20]
		#vs = vector.KiteVector(hosts, path, parquetspec, fragcnt)
		#res = vs.inner_product(embedding, threshold, nbest, docids=docids)
		#print(res)

		#filter = 'id IN (1,3)'
		#vs = vector.KiteVector(hosts, path, parquetspec, fragcnt)
		#res = vs.inner_product(embedding, threshold, nbest, filter=filter)
		#print(res)
	except Exception as msg:
		print(msg)
