import os
import argparse
from kitevector.index import httpd

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', '--port', type=int, default=8181)
	parser.add_argument('--kite', type=int, default=7878)
	parser.add_argument('datadir')
	args = parser.parse_args()

	httpd.run(args.port, args.datadir, args.kite)
