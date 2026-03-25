"""
FAVA ECOM — Servidor Proxy + Painel
=====================================
Resolve o problema de CORS das APIs Bling e ML.
Clique duas vezes para iniciar.
Acesse: http://localhost:8080/painel_fava.html
         http://localhost:8080/calculadora_fava.html
"""
import http.server
import urllib.request
import urllib.error
import json
import os
import sys
import webbrowser
import threading

PORTA = 8080
PASTA = os.path.dirname(os.path.abspath(__file__))
os.chdir(PASTA)

ROTAS_PROXY = {
    '/api/bling/': 'https://www.bling.com.br/Api/v3/',
    '/api/ml/':    'https://api.mercadolibre.com/',
    '/api/mp/':    'https://api.mercadopago.com/',
}

class ProxyHandler(http.server.SimpleHTTPRequestHandler):

    def log_message(self, fmt, *args):
        if self.path.startswith('/api/'):
            status = args[1] if len(args) > 1 else '?'
            print(f'  [{status}] {self.path[:80]}')

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith('/api/'):
            self._proxy('GET')
        else:
            super().do_GET()

    def do_POST(self):
        if self.path.startswith('/api/'):
            self._proxy('POST')
        else:
            self.send_error(405)

    def do_PUT(self):
        if self.path.startswith('/api/'):
            self._proxy('PUT')
        else:
            self.send_error(405)

    def _proxy(self, method):
        url_destino = None
        for prefixo, base in ROTAS_PROXY.items():
            if self.path.startswith(prefixo):
                caminho = self.path[len(prefixo):]
                url_destino = base + caminho
                break

        if not url_destino:
            self.send_error(404)
            return

        headers = {}
        for h in ['Authorization', 'Content-Type', 'Accept']:
            v = self.headers.get(h)
            if v:
                headers[h] = v
        if 'Accept' not in headers:
            headers['Accept'] = 'application/json'

        body = None
        if method in ('POST', 'PUT'):
            length = int(self.headers.get('Content-Length', 0))
            if length:
                body = self.rfile.read(length)

        try:
            req = urllib.request.Request(url_destino, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
                self.send_response(resp.status)
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
            self.send_response(500)
            self._cors()
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Authorization, Content-Type, Accept')


def main():
    print('=' * 55)
    print('  FAVA ECOM — Servidor Proxy + Painel')
    print('=' * 55)
    print(f'\n  Pasta: {PASTA}')

    for arq in ['painel_fava.html', 'calculadora_fava.html']:
        existe = 'OK' if os.path.exists(arq) else 'NAO ENCONTRADO'
        print(f'  [{existe}] {arq}')

    print(f'\n  Iniciando na porta {PORTA}...')

    try:
        server = http.server.HTTPServer(('localhost', PORTA), ProxyHandler)
    except OSError:
        print(f'\n  ERRO: Porta {PORTA} em uso. Feche outros servidores.')
        input('\n  Pressione Enter para fechar...')
        sys.exit(1)

    print(f'\n  Rodando em: http://localhost:{PORTA}')
    print(f'  Painel:      http://localhost:{PORTA}/painel_fava.html')
    print(f'  Calculadora: http://localhost:{PORTA}/calculadora_fava.html')
    print(f'\n  Mantenha esta janela aberta!')
    print(f'  Para fechar: Ctrl+C\n')

    threading.Timer(1.5, lambda: webbrowser.open(f'http://localhost:{PORTA}/painel_fava.html')).start()
    threading.Timer(2.5, lambda: webbrowser.open(f'http://localhost:{PORTA}/calculadora_fava.html')).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n  Encerrado.')

    input('\n  Pressione Enter para fechar...')


if __name__ == '__main__':
    main()
