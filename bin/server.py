import os
import argparse
import signal
import sys
from kitevector.index import httpd

def handler_sigterm(signum, frame):
	sys.exit(0)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', '--port', type=int, default=8181)
	parser.add_argument('--kite', type=int, default=7878)
	parser.add_argument('datadir')
	args = parser.parse_args()


	signal.signal(signal.SIGINT, handler_sigterm)
	signal.signal(signal.SIGTERM, handler_sigterm)

	httpd.run(args.port, args.datadir, args.kite)
