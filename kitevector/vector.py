import sys
import heapq

from kite import kite
from kite.xrg import xrg

class KiteVector:

	schema =  [('id', 'int64'), ('docid', 'string'), ('embedding', 'float[]', 0, 0)]
	path = None
	filespec = None
	hosts = None
	fragcnt = 0

	def __init__(self, hosts, path, filespec, fragcnt = 3):
		self.path = path
		self.filespec = filespec
		self.hosts = hosts
		self.fragcnt = fragcnt
		

	def inner_product(self, embedding, threshold, nbest = 50, ids=None):

		embed = '{' + ",".join([str(item) for item in embedding]) + '}'
		sql = '''select embedding <#> '{}', id from "{}" where embedding <#> '{}' > {} '''.format(embed, self.path, embed, threshold)
		#print(sql)

		if ids is not None:
			sql = sql + ' AND ' + 'id IN (' + ','.join([str(id) for id in ids]) + ')'

		#columns = [c[0] for c in self.schema]
		kitecli = kite.KiteClient()
		h = []
		try:
			kitecli.host(self.hosts).sql(sql).schema(self.schema).filespec(self.filespec).fragment(-1, self.fragcnt).submit()

			#print("run SQL: ", sql)

			while True:
				iter = kitecli.next_row()
				if iter is None:
					break
				else:
					#print("flag=", iter.flags, ", values=", iter.values)
					#print(tuple(iter.values))
					if len(h) <= nbest:
						heapq.heappush(h, tuple(iter.values))
					else:
						heapq.heapreplace(h, tuple(iter.values))

			res = []
			for i in range(len(h)):
				t = heapq.heappop(h)
				res.append(t[1])


			return res

		except OSError as msg:
			print(msg)
			raise
		finally:
			kitecli.close()

