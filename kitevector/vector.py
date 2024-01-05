import sys
import heapq
import numpy as np

from kite import kite
from kite.xrg import xrg

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
			leftsql = '\'{' + ','.join([str(e) for e in self.left]) + '}\''
		else:
			leftsql = str(self.left)

		rightsql = None
		if isinstance(self.right, Expr):
			rightsql = self.right.sql()
		elif isinstance(self.right, list) or isinstance(self.right, np.ndarray):
			rightsql = '\'{' + ','.join([str(e) for e in self.right]) + '}\''
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
		
	def sql(self):
		project = []
		if self.orderby is not None:
			project.append(self.orderby)

		if self.projection is not None and len(self.projection) > 0:
			project.extend(self.projection)

		sql = '''SELECT {} FROM "{}"'''.format(','.join([str(c) if isinstance(c, Expr) else c for c in project]), self.path)

		if self.filters is not None and len(self.filters) > 0:
			sql += ' WHERE '
			sql += ' AND '.join([str(f) for f in self.filters])
			
		#print(sql)

		if self.orderby is None or self.nlimit is None:
			raise ValueError("ORDER BY or LIMIT is absent")

		return sql

	def execute(self):
		sql = self.sql()
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
					if len(h) <= self.nlimit:
						heapq.heappush(h, tuple(iter.values))
					else:
						heapq.heapreplace(h, tuple(iter.values))

			# skip the first item
			if len(h) == self.nlimit+1:
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
