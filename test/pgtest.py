import sys
import random
import math
from kite import kite
from kite.xrg import xrg
from kitevector import vector as kv

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

	# index specific setting
	index_params = {
		"metric_type": "l2",
		"index_type": "hnsw",
		"params":{
			"id_field" : "id",
			"vector_field": "embedding"
		}
	}

	vs = kv.PgVector()
	embed = gen_embedding(1536)

	vs1 = kv.PgVector()
	vs1.select([kv.Var('id'), kv.OpExpr('-', 'docid', 6)]).table(path).index(index_params, embed)
	vs1.filter(kv.ScalarArrayOpExpr("id", [1,2,3])).filter(kv.OpExpr('>', 'id', 0.7)).limit(5)
	sql = vs1.sql()
	print(sql)
