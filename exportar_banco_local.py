"""
FAVA ECOM — exportar_banco_local.py (v2 — schema real)
=======================================================
Exporta banco.db local → Railway PostgreSQL.

Tabelas detectadas:
  anuncios_ml  → /api/db/listings-batch  (mlb, sku, preco, taxa_pct, frete_rs, tipo)
  produtos     → /api/cmv-cache          (sku, cmv_brasil, cmv_parana)

Uso:
  python exportar_banco_local.py               → exporta tudo
  python exportar_banco_local.py --so-anuncios → só anúncios ML
  python exportar_banco_local.py --so-produtos → só CMV produtos
"""

import sqlite3
import requests
import sys
import os
import math

# ── CONFIGURAÇÃO ─────────────────────────────────────────────────
BANCO_DB = r'C:\FAVAECOM\banco.db'
BASE_URL = 'https://web-production-5aa0f.up.railway.app'
LOTE     = 100
# ─────────────────────────────────────────────────────────────────

def conectar():
    if not os.path.exists(BANCO_DB):
        print(f'[ERRO] banco.db não encontrado em: {BANCO_DB}')
        sys.exit(1)
    tamanho = os.path.getsize(BANCO_DB) / 1024
    print(f'banco.db encontrado ({tamanho:.0f} KB)')
    conn = sqlite3.connect(BANCO_DB)
    conn.row_factory = sqlite3.Row
    return conn

def limpar(val, default=0.0):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    try:
        return float(val)
    except:
        return default

def post(endpoint, payload):
    url = BASE_URL.rstrip('/') + endpoint
    try:
        r = requests.post(url, json=payload, timeout=60)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f'  [ERRO] {endpoint}: {e}')
        return None

def get_status():
    try:
        r = requests.get(f'{BASE_URL}/api/db/status', timeout=10)
        d = r.json()
        print(f'\n── Status Railway ───────────────────────')
        print(f'  Produtos:  {d.get("produtos","?")} registros')
        print(f'  Boletos:   {d.get("boletos","?")} registros')
        print(f'  Pedidos:   {d.get("pedidos","?")} registros')
        print(f'  Bling OK:  {d.get("bling_ok",False)}')
        print(f'  ML OK:     {d.get("ml_ok",False)}')
        print(f'─────────────────────────────────────────')
    except Exception as e:
        print(f'  [AVISO] Railway não respondeu: {e}')

def exportar_anuncios(conn):
    print('\n[1/2] Exportando anúncios ML...')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM anuncios_ml")
    total = cur.fetchone()[0]
    print(f'  Total no banco.db: {total}')

    cur.execute("""
        SELECT mlb, sku, preco, taxa_pct, frete_rs, tipo, status
        FROM anuncios_ml
    """)
    rows = cur.fetchall()

    listings = []
    ignorados = 0
    for row in rows:
        mlb = str(row['mlb'] or '').strip()
        if not mlb.startswith('MLB'):
            ignorados += 1
            continue
        tipo_raw = str(row['tipo'] or '').lower()
        free_ship = 1 if ('premium' in tipo_raw or 'gold_pro' in tipo_raw) else 0
        listings.append({
            'id':            mlb,
            'sku':           str(row['sku'] or '').strip(),
            'titulo':        '',
            'preco':         limpar(row['preco']),
            'sale_fee':      limpar(row['taxa_pct']),
            'listing_type':  tipo_raw or 'gold_special',
            'free_shipping': free_ship,
            'status':        str(row['status'] or 'active').lower(),
            'frete_medio':   limpar(row['frete_rs']),
            'margem_minima': 0,
            'cmv':           0,
        })

    print(f'  Válidos (MLB...): {len(listings)} | Ignorados: {ignorados}')
    print(f'  Enviando em lotes de {LOTE}...')

    ok_total = err_total = 0
    for i in range(0, len(listings), LOTE):
        lote = listings[i:i+LOTE]
        res = post('/api/db/listings-batch', {'listings': lote})
        if res:
            ok  = res.get('ok', 0)
            err = res.get('errors', 0)
            ok_total  += ok
            err_total += err
            print(f'  Lote {i//LOTE+1}: {len(lote)} → OK={ok} ERR={err}')
        else:
            err_total += len(lote)
            print(f'  Lote {i//LOTE+1}: FALHOU')

    print(f'  Resultado: {ok_total} OK | {err_total} erros')
    return ok_total

def exportar_produtos(conn):
    print('\n[2/2] Exportando CMV dos produtos...')
    cur = conn.cursor()
    cur.execute("""
        SELECT sku, produto, cmv_brasil, cmv_parana
        FROM produtos
        WHERE cmv_brasil > 0
    """)
    rows = cur.fetchall()
    print(f'  Produtos com CMV: {len(rows)}')

    payload = {}
    for row in rows:
        sku    = str(row['sku'] or '').strip()
        nome   = str(row['produto'] or '').strip()
        cmv_br = limpar(row['cmv_brasil'])
        cmv_pr = limpar(row['cmv_parana']) or cmv_br
        if not sku or cmv_br <= 0:
            continue
        payload[sku] = {
            'cmv': cmv_br, 'cmvBr': cmv_br, 'cmvPr': cmv_pr, 'nome': nome
        }

    skus = list(payload.keys())
    ok_total = 0
    for i in range(0, len(skus), LOTE):
        lote = {k: payload[k] for k in skus[i:i+LOTE]}
        res = post('/api/cmv-cache', lote)
        if res:
            ok_total += res.get('n', len(lote))
            print(f'  Lote {i//LOTE+1}: {len(lote)} SKUs → OK')
        else:
            print(f'  Lote {i//LOTE+1}: FALHOU')

    print(f'  Resultado: {ok_total} SKUs enviados')
    return ok_total

def main():
    args = sys.argv[1:]
    so_anuncios = '--so-anuncios' in args
    so_produtos  = '--so-produtos' in args

    print('FAVA ECOM — Exportar banco.db → Railway')
    print(f'Banco:   {BANCO_DB}')
    print(f'Railway: {BASE_URL}')

    conn = conectar()
    get_status()

    ok_a = ok_p = 0
    if not so_produtos:
        ok_a = exportar_anuncios(conn)
    if not so_anuncios:
        ok_p = exportar_produtos(conn)

    conn.close()

    print(f'\n{"="*45}')
    print('RESULTADO FINAL')
    print(f'  Anúncios ML enviados: {ok_a}')
    print(f'  Produtos/CMV:         {ok_p}')
    print(f'{"="*45}')
    get_status()
    print('\n✅ Exportação concluída.')
    input('\nPressione Enter para fechar...')

if __name__ == '__main__':
    main()
