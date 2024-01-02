import sys
from kite import kite
from kite.xrg import xrg

import vector

if __name__ == "__main__":

	path = 'tmp/vector/vector*.csv'
	filespec = kite.CsvFileSpec()
	hosts = ['localhost:7878']

	vs = vector.KiteVectorStore(hosts, path, filespec)

	res = vs.nbest([4,6,8], -100, 3)
	print(res)


