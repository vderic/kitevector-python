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

	path = 'tmp/vector/vector*.parquet'
	filespec = kite.ParquetFileSpec()
	hosts = ['localhost:7878']
	schema =  [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]', 0, 0)]

	vs = vector.KiteVector(schema, hosts, path, filespec)

	ids, scores = vs.inner_product(["embedding", gen_embedding(1536)], "id", threshold=-1, nbest=3)
	#res = vs.inner_product([4,6,8], -100, 3, ids=[1,2], docids=[10,20])
	#res = vs.inner_product([4,6,8], -100, 3, filter='docid IN (10,30)')
	print(ids)
	print(scores)


