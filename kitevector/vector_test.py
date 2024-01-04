import sys
import random
from kite import kite
from kite.xrg import xrg

import vector

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
	filespec = kite.ParquetFileSpec()
	hosts = ['localhost:7878']

	vs = vector.KiteVector(hosts, path, filespec)

	ids, scores = vs.inner_product(gen_embedding(1536), -1, 3)
	#res = vs.inner_product([4,6,8], -100, 3, ids=[1,2], docids=[10,20])
	#res = vs.inner_product([4,6,8], -100, 3, filter='docid IN (10,30)')
	print(ids)
	print(scores)


