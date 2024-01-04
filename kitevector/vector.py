import sys
import heapq

from kite import kite
from kite.xrg import xrg

class KiteVector:

	schema =  [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]', 0, 0)]
	path = None
	filespec = None
	hosts = None
	fragcnt = 0

	def __init__(self, hosts, path, filespec, fragcnt = 3):
		self.path = path
		self.filespec = filespec
		self.hosts = hosts
		self.fragcnt = fragcnt
		

	def inner_product(self, embedding, threshold, nbest = 50, ids=None, docids=None, filter=None):

		embed = '{' + ",".join([str(item) for item in embedding]) + '}'
		sql = '''select embedding <#> '{}', id from "{}" where embedding <#> '{}' > {} '''.format(embed, self.path, embed, threshold)
		#print(sql)

		if ids is not None and len(ids) > 0:
			if len(ids) == 1:
				sql = sql + ' AND ' + 'id = ' + str(id[0])
			else:
				sql = sql + ' AND ' + 'id IN (' + ','.join([str(id) for id in ids]) + ')'

		if docids is not None and len(docids) > 0:
			if len(docids) == 1:
				sql = sql + ' AND ' + 'docid = ' + str(docids[0])
			else:
				sql = sql + ' AND ' + 'docid IN (' + ','.join([str(docid) for docid in docids]) + ')'

		if filter is not None:
			sql = sql + ' AND ' + filter

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

			ids = []
			scores = []
			for i in range(len(h)):
				t = heapq.heappop(h)
				ids.append(t[1])
				scores.append(t[0])


			return ids, scores

		except OSError as msg:
			print(msg)
			raise
		finally:
			kitecli.close()

