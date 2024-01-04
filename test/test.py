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
		embedding = gen_embedding(1536)
		threshold = 0.02
		fragcnt = 3
		index = None
		nbest = 3

		vs = vector.KiteVector(schema, hosts, path, parquetspec, fragcnt)
		vs.select(['id', 'docid']).order_by(vector.Embedding('embedding').inner_product(embedding))
		rows, scores = vs.filter(vector.Expr('id IN (999, 4833)')).limit(nbest).do()
		print(rows)
		print(scores)
	except Exception as msg:
		print(msg)
