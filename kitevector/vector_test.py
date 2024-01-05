import sys
import random
import math
from kite import kite
from kite.xrg import xrg

import vector as kv

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

	vs = kv.KiteVector(schema, hosts, 3)
	embed = gen_embedding(1536)
	vs.format(filespec).select(['id', 'docid']).table(path).order_by(kv.VectorExpr("embedding").inner_product(embed))
	vs.filter(kv.OpExpr('>', kv.VectorExpr("embedding").inner_product(embed), 0.07)).limit(5)
	#print(vs.sql())
	rows, scores = vs.execute()
	print(rows)
	print(scores)
