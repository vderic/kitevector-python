import json
import sys
import numpy as np
import pandas as pd

from kite import kite
from kite.xrg import xrg
from kitevector import vector

if __name__ == "__main__":

	path = 'tmp/vector/vector*.csv'
	csvspec = kite.CsvFileSpec()
	parquetspec = kite.ParquetFileSpec()
	hosts = ['localhost:7878']
	fragcnt = 3

	vs = vector.KiteVector(hosts, path, csvspec, fragcnt)

	try:
		embedding = [4,6,8]
		threshold = -70
		fragcnt = 3
		index = None
		nbest = 3
		res = vs.inner_product(embedding, threshold, nbest, index)
		print(res)
	except Exception as msg:
		print(msg)
