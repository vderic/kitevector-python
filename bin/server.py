import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
import json
import numpy as np
import pickle
import hnswlib
from kite import kite
from kite.xrg import xrg
from functools import partial
import argparse
import threading
import glob
from kitevector.index import index

class RequestHandler(BaseHTTPRequestHandler):

	def create_index(self):
		content_length = int(self.headers.get("Content-Length"))
		body = self.rfile.read(content_length)
		#print(str(body))

		try:
			index.Index.create(json.loads(body))

			status = {'status': 'ok'}
			msg = json.dumps(status).encode('utf-8')
		
			self.send_response(200)
			self.send_header("Content-Length", len(msg))
			self.send_header("Content-Type", "application/json")
			self.end_headers()
			self.wfile.write(msg)
		except Exception as e1:
			print('KeyError: ', e1)
			self.send_response(402)
			status = b'''{'status': 'error'}'''
			self.wfile.write(status)
			

	def query(self):
		content_length = int(self.headers.get("Content-Length"))
		body = self.rfile.read(content_length)
		#print(str(body))

		try:
			index.Index.query(json.loads(body))

			msg=b'''[[0.3, 1], [0.4, 2]]'''

			self.send_response(200)
			self.send_header("Content-Length", len(msg))
			self.send_header("Content-Type", "application/json")
			self.end_headers()
			self.wfile.write(msg)
		except KeyError as e1:
			print('KeyError: ', e1)
			self.send_response(402)
			status = b'''{'status': 'error'}'''
			self.wfile.write(status)
		except Exception as e2:
			print(e2)
			self.send_response(402)
			status = b'''{'status': 'error'}'''
			self.wfile.write(status)
			

	
	def do_DELETE(self):
		if self.path == '/delete':
			self.delete_index()

	def do_POST(self):
		print("path = " , self.path)

		if self.path == '/create':
			self.create_index()
		elif self.path == '/query':
			self.query()
		else:
			pass

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-p', '--port', type=int, default=8181)
	parser.add_argument('--kite', type=int, default=7878)
	parser.add_argument('datadir')
	args = parser.parse_args()

	Index.init(args.datadir, args.kite)
	httpd = ThreadingHTTPServer(('', args.port), RequestHandler)

	try:
		httpd.serve_forever()
	except KeyboardInterrupt:
		pass

	httpd.server_close()

