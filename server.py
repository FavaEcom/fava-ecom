"""
FAVA ECOM — Servidor para Deploy (Railway)
Proxy para APIs Bling e ML + serve arquivos HTML
+ rota /api/cmv-cache para receber CMV do script Python local
"""

import http.server
import urllib.request
import urllib.error
import json
import os

PORTA = int(os.environ.get('PORT', 8080))

# Arquivo onde o CMV fica salvo no servidor
CMV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cmv_cache.json')

PROXY = {
    '/api/bling/': 'https://www.bling.com.br/Api/v3/',
    '/api/ml/':    'https://api.mercadolibre.com/',
    '/api/mp/':    'https://api.mercadopago.com/',
}

class Handler(http.server.SimpleHTTPRequestHandler):

    def log_message(self, fmt, *args):
        if '/api/' in self.path:
            print(f"[{args[1] if len(args)>1 else '?'}] {self.path[:80]}")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors(); self.end_headers()

    def do_GET(self):
        # Rota: GET /api/cmv-cache — retorna CMV salvo
        if self.path == '/api/cmv-cache':
            self._cmv_get()
        elif self.path.startswith('/api/'):
            self._proxy('GET')
        else:
            super().do_GET()

    def do_POST(self):
        # Rota: POST /api/cmv-cache — salva CMV enviado pelo script Python
        if self.path == '/api/cmv-cache':
            self._cmv_post()
        elif self.path.startswith('/api/'):
            self._proxy('POST')
        else:
            self.send_error(405)

    def do_PUT(self):
        if self.path.startswith('/api/'):
            self._proxy('PUT')
        else:
            self.send_error(405)

    # ── CMV CACHE ─────────────────────────────────────────────────────────────
    def _cmv_get(self):
        """Retorna o CMV salvo — o painel chama isso no startup."""
        try:
            if os.path.exists(CMV_FILE):
                with open(CMV_FILE, 'r', encoding='utf-8') as f:
                    data = f.read()
                self.send_response(200)
                self._cors()
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(data.encode())
            else:
                self.send_response(200)
                self._cors()
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{}')
        except Exception as e:
            self._json_error(500, str(e))

    def _cmv_post(self):
        """Recebe CMV do script Python local e salva no servidor."""
        try:
            n = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(n) if n else b'{}'
            dados = json.loads(body)

            # Mescla com o existente (não sobrescreve tudo — acumula)
            existente = {}
            if os.path.exists(CMV_FILE):
                with open(CMV_FILE, 'r', encoding='utf-8') as f:
                    existente = json.load(f)

            existente.update(dados)  # novos sobrescrevem antigos

            with open(CMV_FILE, 'w', encoding='utf-8') as f:
                json.dump(existente, f, ensure_ascii=False)

            resp = json.dumps({'ok': True, 'n': len(existente)}).encode()
            self.send_response(200)
            self._cors()
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(resp)
            print(f'[CMV] {len(dados)} SKUs recebidos, total: {len(existente)}')

        except Exception as e:
            self._json_error(500, str(e))

    # ── PROXY ──────────────────────────────────────────────────────────────────
    def _proxy(self, method):
        url = None
        for prefix, base in PROXY.items():
            if self.path.startswith(prefix):
                url = base + self.path[len(prefix):]
                break
        if not url:
            self.send_error(404); return

        headers = {}
        for h in ['Authorization', 'Content-Type', 'Accept']:
            v = self.headers.get(h)
            if v: headers[h] = v
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'

        body = None
        if method in ('POST', 'PUT'):
            n = int(self.headers.get('Content-Length', 0))
            if n: body = self.rfile.read(n)

        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
                self.send_response(r.status)
                self._cors()
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(data)
        except urllib.error.HTTPError as e:
            data = e.read()
            self.send_response(e.code)
            self._cors()
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self._json_error(500, str(e))

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Authorization,Content-Type,Accept')

    def _json_error(self, code, msg):
        self.send_response(code)
        self._cors()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'error': msg}).encode())

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f'Fava Ecom rodando na porta {PORTA}')
    server = http.server.HTTPServer(('0.0.0.0', PORTA), Handler)
    server.serve_forever()
