"""
FAVA ECOM — Renovador Automático de Token Bling
================================================
Roda em segundo plano e renova o token antes de expirar.
Deixa esse script rodando minimizado no terminal.

Como usar:
1. Rode: python renovar_bling_auto.py
2. Deixe o terminal aberto/minimizado
3. Ele renova automaticamente a cada 5h30min
"""
import json, os, time, base64, webbrowser, urllib.request, urllib.parse

CLIENT_ID     = '19df6720532752f6888d5f0aad392bc8829974d3'
CLIENT_SECRET = '590eed8f0b2fb1998e3f60335cef2a17bf5b2135fc69ec4a5ae925f520a8'
REDIRECT_URI  = 'https://www.favaecom.com.br'
TOKENS_FILE   = r'C:\FAVAECOM\bling_tokens.json'
CONFIG_FILE   = r'C:\FAVAECOM\scripts\config.json'
INTERVALO     = 5 * 3600 + 30 * 60  # 5h30min em segundos

def salvar_token(access, refresh):
    # Salva em bling_tokens.json
    dados = {'access_token': access, 'refresh_token': refresh, 'expires_in': 21600}
    with open(TOKENS_FILE, 'w') as f:
        json.dump(dados, f, indent=2)
    # Salva em config.json
    if os.path.exists(CONFIG_FILE):
        try:
            cfg = json.load(open(CONFIG_FILE, encoding='utf-8'))
            cfg['bling_token'] = access
            cfg['bling_refresh_token'] = refresh
            json.dump(cfg, open(CONFIG_FILE, 'w', encoding='utf-8'), indent=2, ensure_ascii=False)
        except: pass
    print(f'  ✅ Token salvo: {access[:30]}...')

def autorizar_manual():
    """Abre navegador para autorização manual."""
    auth_url = (
        f'https://www.bling.com.br/Api/v3/oauth/authorize'
        f'?response_type=code'
        f'&client_id={CLIENT_ID}'
        f'&state=fava_ecom'
        f'&scope=produtos+pedidos+vendas+contatos+estoques+notasfiscais+anuncios'
    )
    print('\n  Abrindo navegador para autorização...')
    webbrowser.open(auth_url)
    print('  Autorize no Bling e cole a URL aqui:\n')
    entrada = input('  URL ou código: ').strip()
    if 'code=' in entrada:
        code = urllib.parse.parse_qs(urllib.parse.urlparse(entrada).query).get('code', [''])[0]
    else:
        code = entrada

    creds = base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
    dados = urllib.parse.urlencode({
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': REDIRECT_URI,
    }).encode()
    req = urllib.request.Request(
        'https://www.bling.com.br/Api/v3/oauth/token',
        data=dados,
        headers={
            'Authorization': f'Basic {creds}',
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        method='POST'
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
    salvar_token(resp['access_token'], resp['refresh_token'])
    return resp['access_token'], resp['refresh_token']

def renovar_refresh(refresh_token):
    """Tenta renovar usando refresh token."""
    try:
        creds = base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
        dados = urllib.parse.urlencode({
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
        }).encode()
        req = urllib.request.Request(
            'https://www.bling.com.br/Api/v3/oauth/token',
            data=dados,
            headers={
                'Authorization': f'Basic {creds}',
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            method='POST'
        )
        resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
        if resp.get('access_token'):
            salvar_token(resp['access_token'], resp.get('refresh_token', refresh_token))
            return resp['access_token'], resp.get('refresh_token', refresh_token)
    except Exception as e:
        print(f'  ⚠️ Refresh falhou: {e}')
    return None, None

def main():
    print('='*55)
    print('  FAVA ECOM — Renovador Automático Bling')
    print('  Deixe este terminal aberto/minimizado')
    print('='*55)

    refresh_token = ''

    # Tenta carregar token existente
    if os.path.exists(TOKENS_FILE):
        try:
            t = json.load(open(TOKENS_FILE, encoding='utf-8'))
            refresh_token = t.get('refresh_token', '')
            print(f'\n  Token existente encontrado.')
        except: pass

    # Se não tem refresh, faz autorização manual
    if not refresh_token:
        print('\n  Nenhum token encontrado. Fazendo autorização inicial...')
        _, refresh_token = autorizar_manual()

    print(f'\n  ✅ Iniciando renovação automática a cada 5h30min')
    print(f'  Próxima renovação em 5h30min\n')

    ciclo = 0
    while True:
        time.sleep(INTERVALO)
        ciclo += 1
        hora = time.strftime('%H:%M:%S')
        print(f'\n[{hora}] Ciclo {ciclo} — Renovando token Bling...')

        access, novo_refresh = renovar_refresh(refresh_token)
        if access:
            refresh_token = novo_refresh
            print(f'  ✅ Renovado com sucesso!')
        else:
            print(f'  ❌ Refresh falhou — fazendo autorização manual...')
            try:
                _, refresh_token = autorizar_manual()
            except Exception as e:
                print(f'  ❌ Erro: {e}')
                print(f'  Tentando novamente em 30 minutos...')
                time.sleep(1800)

if __name__ == '__main__':
    main()
