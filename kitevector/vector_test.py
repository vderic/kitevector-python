import sys
from kite import kite
from kite.xrg import xrg

import vector

if __name__ == "__main__":

	path = 'tmp/vector/vector*.csv'
	filespec = kite.CsvFileSpec()
	hosts = ['localhost:7878']

	vs = vector.KiteVector(hosts, path, filespec)

	res = vs.inner_product([4,6,8], -100, 3, filter=[0,1,2])
	print(res)


