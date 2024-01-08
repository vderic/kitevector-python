from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
import json

class KiteIndexServerContext:
	indexes = {}

class RequestHandler(BaseHTTPRequestHandler):

	def create_index(self):
		content_length = int(self.headers.get("Content-Length"))
		body = self.rfile.read(content_length)
		print(str(body))

		status = {'status': 'ok'}
		msg = json.dumps(status).encode('utf-8')
		
		self.send_response(200)
		self.send_header("Content-Length", len(msg))
		self.send_header("Content-Type", "application/json")
		self.end_headers()

		self.wfile.write(msg)

	def query(self):
		content_length = int(self.headers.get("Content-Length"))
		body = self.rfile.read(content_length)
		print(str(body))

		embedding = [12,2,3,4]
		msg = json.dumps(embedding).encode('utf-8')
		
		self.send_response(200)
		self.send_header("Content-Length", len(msg))
		self.send_header("Content-Type", "application/json")
		self.end_headers()

		self.wfile.write(msg)


	
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



def run():
	server = ('', 8181)
	httpd = HTTPServer(server, RequestHandler)
	httpd.serve_forever()

if __name__ == "__main__":
	run()
