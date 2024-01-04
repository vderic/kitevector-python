import sys
import heapq

from kite import kite
from kite.xrg import xrg

class Expr:

	expr = None

	def __init__(self, expr):
		self.expr = expr
		
	def __str__(self):
		return self.expr

class Embedding(Expr):
	
	cname = None
	embedding = None
	operator = None
	gtval = None

	def __init__(self, cname):
		self.cname = cname
	
	def inner_product(self, embedding):
		self.embedding = embedding
		self.operator = '<#>'
		return self

	def gt(self, threshold):
		self.gtval = threshold
		return self
		
	def __str__(self):
		value = '{' + ','.join([str(e) for e in self.embedding]) + '}'
		ret = "{} {} '{}'".format(self.cname, self.operator, value)
		if self.gtval is not None:
			ret += ' > {}'.format(self.gtval)
		return ret


class KiteVector:

	schema =  None
	path = None
	filespec = None
	hosts = None
	fragcnt = 0

	projection = None
	order_by = None
	filters = []
	limit = None

	def __init__(self, schema, hosts, path, filespec, fragcnt = 3):
		self.path = path
		self.schema = schema
		self.filespec = filespec
		self.hosts = hosts
		self.fragcnt = fragcnt
		
	def select(self, projection):
		self.projection = projection
		return self

	def order_by(self, expr):
		self.order_by = expr

		return self

	def limit(self, limit):
		self.limit = limit
		return self

	def filter(self, expr):
		self.filters.append(expr)
		return self

	def do(self):
		project = []
		if self.order_by is not None:
			project.append(str(self.order_by))

		if self.projection is not None and len(self.projection) > 0:
			project.extend(self.projection)

		sql = '''SELECT {} FROM "{}"'''.format(','.join(project), self.path)

		if self.filters is not None and len(self.filters) > 0:
			sql += ' WHERE '
			sql += ' AND '.join([str(f) for f in self.filters])
			
		#print(sql)

		if self.order_by is None or self.limit is None:
			raise ValueError("ORDER BY or LIMIT is absent")

		return self.sort(sql)

	def sort(self, sql):
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
					if len(h) <= self.limit:
						heapq.heappush(h, tuple(iter.values))
					else:
						heapq.heapreplace(h, tuple(iter.values))

			# skip the first item
			if len(h) == self.limit+1:
				heapq.heappop(h)


			scores = []
			cols = []
			for i in range(len(h)):
				t = heapq.heappop(h)
				scores.append(t[0])
				cols.append(t[1:])

			return cols, scores

		except OSError as msg:
			print(msg)
			raise
		finally:
			kitecli.close()


	def inner_product(self, embedding, projection, threshold=None, nbest = 50, filter=None):

		embed_cname = embedding[0]
		embed_data = embedding[1]

		embed = '{' + ",".join([str(item) for item in embed_data]) + '}'
		project = ','.join(projection)

		sql = '''select {} <#> '{}', {} from "{}"'''.format(embed_cname, embed, project, self.path)

		filters = []
		if threshold is not None:
			filters.append('''{} <#> '{}' > {}'''.format(embed_cname, embed, threshold))

		if filter is not None:
			filters.extend(filter)

		if len(filters) > 0:
			sql += ' WHERE '
			sql += ' AND '.join(filters)

		#print(sql)
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

