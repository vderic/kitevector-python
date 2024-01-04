import sys
import heapq

from kite import kite
from kite.xrg import xrg

class KiteVector:

	schema =  None
	path = None
	filespec = None
	hosts = None
	fragcnt = 0

	def __init__(self, schema, hosts, path, filespec, fragcnt = 3):
		self.path = path
		self.schema = schema
		self.filespec = filespec
		self.hosts = hosts
		self.fragcnt = fragcnt
		

	def inner_product(self, embedding, projection, threshold=None, nbest = 50, filter=None):

		embed_cname = embedding[0]
		embed_data = embedding[1]

		embed = '{' + ",".join([str(item) for item in embed_data]) + '}'
		project = ','.join(projection)

		sql = '''select {} <#> '{}', {} from "{}"'''.format(embed_cname, embed, project, self.path)

		if threshold is not None or filter is not None:
			sql += ' WHERE '

		if threshold is not None:
			sql += '''{} <#> '{}' > {}'''.format(embed_cname, embed, threshold)


		if filter is not None:
			sql += ' AND ' + filter

		#print(sql)
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

			# skip the first item
			if len(h) == nbest+1:
				heapq.heappop(h)


			scores = []
			cols = []
			for i in range(len(h)):
				if i < nbest:
					t = heapq.heappop(h)
					scores.append(t[0])
					cols.append(t[1:])


			return cols, scores

		except OSError as msg:
			print(msg)
			raise
		finally:
			kitecli.close()

