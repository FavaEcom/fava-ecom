"""
FAVA ECOM — Servidor para Deploy (Railway/Render)
Proxy para APIs Bling e ML + serve arquivos HTML
"""
import http.server
import urllib.request
import urllib.error
import json
import os

PORTA = int(os.environ.get('PORT', 8080))

PROXY = {
    '/api/bling/': 'https://www.bling.com.br/Api/v3/',
    '/api/ml/':    'https://api.mercadolibre.com/',
}

class Handler(http.server.SimpleHTTPRequestHandler):

    def log_message(self, fmt, *args):
        if '/api/' in self.path:
            print(f"[{args[1] if len(args)>1 else '?'}] {self.path[:80]}")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors(); self.end_headers()

    def do_GET(self):
        if self.path.startswith('/api/'): self._proxy('GET')
        else: super().do_GET()

    def do_POST(self):
        if self.path.startswith('/api/'): self._proxy('POST')
        else: self.send_error(405)

    def do_PUT(self):
        if self.path.startswith('/api/'): self._proxy('PUT')
        else: self.send_error(405)

    def _proxy(self, method):
        url = None
        for prefix, base in PROXY.items():
            if self.path.startswith(prefix):
                url = base + self.path[len(prefix):]
                break
        if not url:
            self.send_error(404); return

        headers = {}
        for h in ['Authorization','Content-Type','Accept']:
            v = self.headers.get(h)
            if v: headers[h] = v
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'

        body = None
        if method in ('POST','PUT'):
            n = int(self.headers.get('Content-Length',0))
            if n: body = self.rfile.read(n)

        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
                self.send_response(r.status)
                self._cors()
                self.send_header('Content-Type','application/json')
                self.end_headers()
                self.wfile.write(data)
        except urllib.error.HTTPError as e:
            data = e.read()
            self.send_response(e.code)
            self._cors()
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self.send_response(500)
            self._cors()
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error':str(e)}).encode())

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET,POST,PUT,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Authorization,Content-Type,Accept')

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f"Fava Ecom rodando na porta {PORTA}")
    server = http.server.HTTPServer(('0.0.0.0', PORTA), Handler)
    server.serve_forever()
