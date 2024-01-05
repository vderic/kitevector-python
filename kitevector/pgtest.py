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
	path = 'ext_ai'

	vs = kv.PgVector()
	embed = gen_embedding(1536)
	#vs.select(['id', 'docid']).table(path).order_by(vector.Embedding("embedding").inner_product(embed))
	vs.select(['id', 'docid']).table(path)
	vs.filter(kv.OpExpr('>', kv.VectorExpr("embedding").inner_product(embed), 0.07)).limit(5)
	sql = vs.sql()
	print(sql)

	vs1 = kv.PgVector()
	vs1.select(['id', 'docid']).table(path).order_by(kv.Expr("docid"))
	vs1.filter(kv.ScalarArrayOpExpr("id", [1,2,3])).filter(kv.OpExpr('>', 'id', 0.7)).limit(5)
	sql = vs1.sql()
	print(sql)
