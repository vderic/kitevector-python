import sys
import random
import math
from kite import kite
from kite.xrg import xrg

import vector

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
	hosts = ['localhost:7878']
	schema =  [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]')]

	vs = vector.KiteVector(schema, hosts, path, filespec)

	cols, scores = vs.inner_product(["embedding", gen_embedding(1536)], ['id', 'docid'], threshold=-1, nbest=3, filter=['id IN (999, 4833)'])
	#cols, scores = vs.inner_product(["embedding", gen_embedding(1536)], ['id', 'docid'], threshold=-1, nbest=3)
	#cols, scores = vs.inner_product(["embedding", gen_embedding(1536)], ['id', 'docid'], nbest=3)
	print(cols)
	print(scores)


