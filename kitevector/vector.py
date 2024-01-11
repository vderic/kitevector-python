import sys
import heapq
import numpy as np

from kite import kite
from kite.xrg import xrg
from kitevector.index import client

class Expr:

	def __init__(self, expr):
		self.expr = expr
		
	def __str__(self):
		return self.expr

	def sql(self):
		return self.expr

class Var(Expr):
	def __init__(self, cname):
		self.cname = cname

	def __str__(self):
		return self.cname

	def sql(self):
		return self.cname

class VectorExpr(Expr):
	def __init__(self, cname):
		self.cname = cname

	def inner_product(self, embedding):
		return OpExpr('<#>', self, Embedding(embedding))

	def l2_distance(self, embedding):
		return OpExpr('<->', self, Embedding(embedding))

	def cosine_distance(self, embedding):
		return OpExpr('<=>', self, Embedding(embedding))

	def __str__(self):
		return self.cname

	def sql(self):
		return self.__str__()

class Embedding(Expr):
	
	def __init__(self, embedding):
		self.embedding = embedding
	
	def __str__(self):
		return '\'{' + ','.join([str(e) for e in self.embedding]) + '}\''

	def sql(self):
		return '\'[' + ','.join([str(e) for e in self.embedding]) + ']\''

class ScalarArrayOpExpr(Expr):

	def __init__(self, left, right):
		self.left = left
		self.right = right

	def __str__(self):
		ret = '''{} IN ({})'''.format(self.left, ','.join([str(e) for e in self.right]))
		return ret

	def sql(self):
		return self.__str__()

class OpExpr(Expr):

	def __init__(self, op, left, right):
		self.op = op
		self.left = left
		self.right = right

	def __str__(self):
		leftsql = None
		if isinstance(self.left, Expr):
			leftsql = str(self.left)
		elif isinstance(self.left, list) or isinstance(self.left, np.ndarray):
			leftsql = '\'{' + ','.join([str(e) for e in self.left]) + '}\''
		else:
			leftsql = str(self.left)

		rightsql = None
		if isinstance(self.right, Expr):
			rightsql = str(self.right)
		elif isinstance(self.right, list) or isinstance(self.right, np.ndarray):
			rightsql = '\'{' + ','.join([str(e) for e in self.right]) + '}\''
		else:
			rightsql = str(self.right)

		ret = '''{} {} {}'''.format(leftsql, self.op, rightsql)
		return ret

	def sql(self):
		leftsql = None
		if isinstance(self.left, Expr):
			leftsql = self.left.sql()
		elif isinstance(self.left, list) or isinstance(self.left, np.ndarray):
			leftsql = '\'{' + ','.join([e.sql() if isinstance(e, Expr) else str(e) for e in self.left]) + '}\''
		else:
			leftsql = str(self.left)

		rightsql = None
		if isinstance(self.right, Expr):
			rightsql = self.right.sql()
		elif isinstance(self.right, list) or isinstance(self.right, np.ndarray):
			rightsql = '\'{' + ','.join([e.sql() if isinstance(e, Expr) else str(e) for e in self.right]) + '}\''
		else:
			rightsql = str(self.right)

		ret = '''{} {} {}'''.format(leftsql, self.op, rightsql)
		return ret


class BaseVector:

	def __init__(self):
		self.projection = None
		self.orderby = None
		self.filters = []
		self.nlimit = None
		self.path = None
		self.filespec = None

	def select(self, projection):
		self.projection = projection
		return self

	def order_by(self, expr):
		self.orderby = expr

		return self

	def limit(self, limit):
		self.nlimit = limit
		return self

	def filter(self, expr):
		self.filters.append(expr)
		return self

	def table(self, path):
		self.path = path
		return self
	
	def format(self, filespec):
		self.filespec = filespec
		return self


class PgVector(BaseVector):

	def __init__(self):
		super().__init__()

	def sql(self):
		sql = '''SELECT {} FROM "{}"'''.format(','.join([c.sql() if isinstance(c, Expr) else c for c in self.projection]), self.path)

		if self.filters is not None and len(self.filters) > 0:
			sql += ' WHERE '
			sql += ' AND '.join([f.sql() for f in self.filters])
			
		if self.orderby is not None:
			sql += ' ORDER BY ' + self.orderby.sql()

		if self.nlimit is not None:
			sql += ' LIMIT {}'.format(self.nlimit)
		return sql


	
class KiteVector(BaseVector):

	def __init__(self, schema, hosts, fragcnt = 3):
		super().__init__()
		self.schema = schema
		self.hosts = hosts
		self.fragcnt = fragcnt
		self.indexcli = None
		self.index_params = None
		self.index_hosts = None
		self.data = None
		
	def order_by(self, expr):
		raise ValueError('KiteVector does not support order_by().  Use index() instead.')

	def index(self, index_params, data, hosts=None):
		self.index_hosts = hosts
		self.index_params = index_params
		self.data = data

		params = self.index_params['params']
		metric_type = self.index_params['metric_type']
		if metric_type == 'ip':
			self.orderby = VectorExpr(params['embedding_field']).inner_product(self.data)
		else:
			raise ValueError('only inner product is supported')

		#elif metric_type == 'cosine':
		#	self.orderby = VectorExpr(params['embedding_field']).cosine_distance(self.data)
		#elif metric_ttpe == 'l2':
		#	self.orderby = VectorExpr(params['embedding_field']).l2_distance(self.data)
		return self

	def sql(self):
		if self.orderby is None or self.nlimit is None:
			raise ValueError("ORDER BY or LIMIT is absent")

		if self.indexcli is None:
			return self.flat_sql()
		else:
			return self.index_sql()


	def index_sql(self):
		if self.indexcli is None:
			raise ValueError("Index not defined")

		project = []
		if self.projection is None or len(self.projection) == 0:
			raise ValueError("projection not defined")

		params = self.index_params['params']

		project.append(params['id_field'])
		project.extend(self.projection)

		sql = '''SELECT {} FROM "{}"'''.format(','.join([str(c) if isinstance(c, Expr) else c for c in project]), self.path)

		if self.filters is not None and len(self.filters) > 0:
			sql += ' WHERE '
			sql += ' AND '.join([str(f) for f in self.filters])
			
		#print(sql)
		return sql
		

	def flat_sql(self):
		project = []
		if self.orderby is not None:
			project.append(self.orderby)

		params = self.index_params['params']
		project.append(params['id_field'])

		if self.projection is not None and len(self.projection) > 0:
			project.extend(self.projection)

		sql = '''SELECT {} FROM "{}"'''.format(','.join([str(c) if isinstance(c, Expr) else c for c in project]), self.path)

		if self.filters is not None and len(self.filters) > 0:
			sql += ' WHERE '
			sql += ' AND '.join([str(f) for f in self.filters])
			
		#print(sql)
		return sql


	def execute(self):

		if self.index_params is None:
			raise ValueError('index_params is not defined')

		params = self.index_params['params']
		if self.index_params['index_type'] == 'flat':
			sql = self.sql()
			return self.sort(sql)

		if self.index_hosts is None:
			raise ValueError('index hosts is not defined')
		if not isinstance(self.orderby, OpExpr): 
			raise ValueError("order by is not OpExpr")
		if not isinstance(self.orderby.left, VectorExpr):
			raise ValueError("order by left is not VectorExpr")
		if not isinstance(self.orderby.right, Embedding):
			raise ValueError("order by left is not Embedding")

		self.indexcli = client.IndexClient(self.schema, self.path, self.index_hosts, self.fragcnt, self.filespec, self.index_params)
		ids, distances = self.indexcli.query([self.orderby.right.embedding], self.nlimit)

		if len(ids) == 0:
			return []

		idfilter = ScalarArrayOpExpr(params['id_field'], ids[0])
		self.filter(idfilter)
		sql = self.index_sql()
		dict = self.scan(sql)

		values = []
		for id in ids[0]:
			values.append(dict[id])

		results = {'ids': ids[0], 'distances': distances[0], 'values': values}
		return results

	def scan(self, sql):
		kitecli = kite.KiteClient()
		dict = {}
		try:
			kitecli.host(self.hosts).sql(sql).schema(self.schema).filespec(self.filespec).fragment(-1, self.fragcnt).submit()

			#print("run SQL: ", sql)
			while True:
				iter = kitecli.next_row()
				if iter is None:
					break
				else:
					dict[iter.values[0]] = iter.values[1:]
			return dict

		except OSError as msg:
			print(msg)
			raise
		finally:
			kitecli.close()



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
					if len(h) <= self.nlimit:
						heapq.heappush(h, tuple(iter.values))
					else:
						heapq.heapreplace(h, tuple(iter.values))

			# skip the first item
			if len(h) == self.nlimit+1:
				heapq.heappop(h)

			ids = []
			distances = []
			values = []
			for i in range(len(h)):
				t = heapq.heappop(h)
				ids.insert(0, t[1])
				distances.insert(0, t[0])
				values.insert(0, list(t[2:]))

			results = {'ids': ids, 'distances': distances, 'values': values}
			return results

		except OSError as msg:
			print(msg)
			raise
		finally:
			kitecli.close()
