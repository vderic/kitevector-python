import json
import sys
import numpy as np
import pandas as pd

from kite import kite
from kite.xrg import xrg
import kitevs

if __name__ == "__main__":

	path = 'tmp/vector/vector*.csv'
	filespec = kite.CsvFileSpec()
	hosts = ['localhost:7878']

	vs = kitevs.KiteVectorStore(hosts, path, filespec)

	res = vs.nbest([4,6,8], None, -100, 3)
	print(res)


