"""
FAVA ECOM — exportar_banco_local.py
=====================================
Exporta o banco SQLite local (banco.db) para o Railway PostgreSQL.

Lê as tabelas locais e envia via API:
  ml_listings / anuncios_ml  → /api/db/listings-batch
  produtos                   → /api/cmv-cache
  historico_compras          → /api/db/historico  (opcional)

Uso:
  python exportar_banco_local.py            → exporta tudo
  python exportar_banco_local.py --inspecionar → só mostra o schema
  python exportar_banco_local.py --so-anuncios → só anúncios ML
  python exportar_banco_local.py --so-produtos → só CMV de produtos
"""

import sqlite3
import requests
import sys
import os
import math

# ── CONFIGURAÇÃO ────────────────────────────────────────────────────────────
BANCO_DB  = r'C:\FAVAECOM\banco.db'
BASE_URL  = 'https://web-production-5aa0f.up.railway.app'
LOTE      = 100
# ────────────────────────────────────────────────────────────────────────────

def conectar():
    if not os.path.exists(BANCO_DB):
        print(f'[ERRO] banco.db não encontrado em: {BANCO_DB}')
        sys.exit(1)
    tamanho = os.path.getsize(BANCO_DB) / 1024
    print(f'banco.db encontrado ({tamanho:.0f} KB)')
    conn = sqlite3.connect(BANCO_DB)
    conn.row_factory = sqlite3.Row
    return conn

def inspecionar(conn):
    """Mostra todas as tabelas e colunas do banco.db."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tabelas = [r[0] for r in cur.fetchall()]
    
    print(f'\n── Schema do banco.db ({len(tabelas)} tabelas) ─────────────────')
    for t in tabelas:
        cur.execute(f"SELECT COUNT(*) FROM '{t}'")
        total = cur.fetchone()[0]
        cur.execute(f"PRAGMA table_info('{t}')")
        colunas = [c[1] for c in cur.fetchall()]
        print(f'\n  {t} ({total} registros)')
        print(f'    Colunas: {", ".join(colunas)}')
    print()
    return tabelas

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

def get_status_railway():
    try:
        r = requests.get(f'{BASE_URL}/api/db/status', timeout=10)
        d = r.json()
        print(f'\n── Status Railway ──────────────────────────')
        print(f'  Produtos:  {d.get("produtos","?")}')
        print(f'  Boletos:   {d.get("boletos","?")}')
        print(f'  Pedidos:   {d.get("pedidos","?")}')
        print(f'  Bling OK:  {d.get("bling_ok",False)}')
        print(f'  ML OK:     {d.get("ml_ok",False)}')
        print(f'───────────────────────────────────────────')
    except Exception as e:
        print(f'  [AVISO] Railway não respondeu: {e}')

# ── EXPORTAR ANÚNCIOS ML ─────────────────────────────────────────────────────
def exportar_anuncios(conn, tabelas):
    """
    Tenta as tabelas em ordem de prioridade.
    Adapta para qualquer variação de nome de coluna.
    """
    # Nomes possíveis da tabela de anúncios
    candidatos = ['anuncios_ml', 'ml_listings', 'anuncios', 'listings', 'items']
    tabela = next((t for t in candidatos if t in tabelas), None)
    
    if not tabela:
        print('[AVISO] Nenhuma tabela de anúncios encontrada. Tabelas disponíveis:')
        print('  ', ', '.join(tabelas))
        return 0
    
    print(f'\n[1/2] Exportando anúncios da tabela: {tabela}')
    
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info('{tabela}')")
    colunas_raw = {c[1].lower(): c[1] for c in cur.fetchall()}
    
    # Mapa: nome esperado → variações possíveis no banco local
    mapa = {
        'id':            ['id', 'mlb', 'mlb_id', 'item_id', 'listing_id'],
        'sku':           ['sku', 'codigo', 'seller_sku', 'cod_produto'],
        'titulo':        ['titulo', 'title', 'nome', 'descricao', 'name'],
        'preco':         ['preco', 'price', 'valor', 'preco_atual'],
        'sale_fee':      ['sale_fee', 'taxa', 'comissao', 'fee', 'taxa_ml', 'percentual_comissao'],
        'frete_medio':   ['frete_medio', 'frete', 'frete_liq', 'shipping_cost', 'frete_calculado'],
        'listing_type':  ['listing_type', 'tipo', 'tipo_anuncio', 'listing_type_id'],
        'free_shipping': ['free_shipping', 'frete_gratis', 'gratis'],
        'status':        ['status', 'situacao', 'ativo'],
        'cmv':           ['cmv', 'cmv_br', 'custo', 'custo_br', 'preco_custo'],
    }
    
    def achar_col(campo):
        for variacao in mapa.get(campo, [campo]):
            if variacao in colunas_raw:
                return colunas_raw[variacao]
        return None
    
    # Resolve colunas
    col_id     = achar_col('id')
    col_sku    = achar_col('sku')
    col_titulo = achar_col('titulo')
    col_preco  = achar_col('preco')
    col_taxa   = achar_col('sale_fee')
    col_frete  = achar_col('frete_medio')
    col_tipo   = achar_col('listing_type')
    col_free   = achar_col('free_shipping')
    col_status = achar_col('status')
    col_cmv    = achar_col('cmv')
    
    if not col_id:
        print(f'[ERRO] Coluna de ID (MLB) não encontrada em {tabela}')
        print(f'  Colunas disponíveis: {list(colunas_raw.keys())}')
        return 0
    
    print(f'  Colunas mapeadas:')
    print(f'    id={col_id} | sku={col_sku} | titulo={col_titulo}')
    print(f'    preco={col_preco} | taxa={col_taxa} | frete={col_frete}')
    print(f'    tipo={col_tipo} | cmv={col_cmv}')
    
    # Monta SELECT
    select_cols = [col_id]
    for c in [col_sku, col_titulo, col_preco, col_taxa, col_frete,
              col_tipo, col_free, col_status, col_cmv]:
        if c:
            select_cols.append(c)
    
    cur.execute(f"SELECT COUNT(*) FROM '{tabela}'")
    total = cur.fetchone()[0]
    print(f'\n  Total de anúncios: {total}')
    
    cur.execute(f"SELECT {', '.join(select_cols)} FROM '{tabela}'")
    rows = cur.fetchall()
    
    # Filtra só MLB
    listings = []
    ignorados = 0
    for row in rows:
        d = dict(row)
        mlb = str(d.get(col_id, '') or '').strip()
        if not mlb.startswith('MLB'):
            ignorados += 1
            continue
        
        tipo_raw = str(d.get(col_tipo, '') or '').lower()
        free_ship = 1 if ('premium' in tipo_raw or 'gold_pro' in tipo_raw) else 0
        if col_free and d.get(col_free):
            free_ship = int(d[col_free] or 0)
        
        listings.append({
            'id':            mlb,
            'sku':           str(d.get(col_sku, '') or '').strip(),
            'titulo':        str(d.get(col_titulo, '') or '').strip(),
            'preco':         limpar(d.get(col_preco)),
            'sale_fee':      limpar(d.get(col_taxa)),
            'listing_type':  tipo_raw or 'gold_special',
            'free_shipping': free_ship,
            'status':        str(d.get(col_status, 'active') or 'active').lower(),
            'frete_medio':   limpar(d.get(col_frete) if col_frete else 0),
            'margem_minima': 0,
            'cmv':           limpar(d.get(col_cmv) if col_cmv else 0),
        })
    
    print(f'  Válidos (MLB...): {len(listings)} | Ignorados: {ignorados}')
    print(f'\n  Enviando para Railway em lotes de {LOTE}...')
    
    ok_total = 0
    err_total = 0
    for i in range(0, len(listings), LOTE):
        lote = listings[i:i+LOTE]
        res = post('/api/db/listings-batch', {'listings': lote})
        if res:
            ok = res.get('ok', 0)
            err = res.get('errors', 0)
            ok_total += ok
            err_total += err
            print(f'  Lote {i//LOTE+1}: {len(lote)} → OK={ok} ERR={err}')
        else:
            err_total += len(lote)
            print(f'  Lote {i//LOTE+1}: FALHOU')
    
    print(f'\n  Resultado anúncios: {ok_total} OK | {err_total} erros')
    return ok_total

# ── EXPORTAR PRODUTOS / CMV ──────────────────────────────────────────────────
def exportar_produtos(conn, tabelas):
    """Exporta CMV dos produtos para /api/cmv-cache."""
    candidatos = ['produtos', 'products', 'items', 'catalog']
    tabela = next((t for t in candidatos if t in tabelas), None)
    
    if not tabela:
        print('[AVISO] Tabela de produtos não encontrada. Pulando.')
        return 0
    
    print(f'\n[2/2] Exportando produtos da tabela: {tabela}')
    
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info('{tabela}')")
    colunas_raw = {c[1].lower(): c[1] for c in cur.fetchall()}
    
    mapa_p = {
        'sku':    ['sku', 'codigo', 'cod', 'id'],
        'nome':   ['nome', 'descricao', 'name', 'titulo', 'produto'],
        'cmv_br': ['cmv_br', 'custo_br', 'cmv', 'custo', 'preco_custo', 'custo_brasil'],
        'cmv_pr': ['cmv_pr', 'custo_pr', 'custo_parana'],
    }
    
    def achar(campo):
        for v in mapa_p.get(campo, [campo]):
            if v in colunas_raw:
                return colunas_raw[v]
        return None
    
    col_sku  = achar('sku')
    col_nome = achar('nome')
    col_cmv  = achar('cmv_br')
    col_cmvp = achar('cmv_pr')
    
    if not col_sku or not col_cmv:
        print(f'[AVISO] Colunas SKU/CMV não encontradas em {tabela}.')
        print(f'  Disponíveis: {list(colunas_raw.keys())}')
        return 0
    
    cur.execute(f"SELECT {col_sku}, {col_nome or col_sku}, {col_cmv}{', ' + col_cmvp if col_cmvp else ''} FROM '{tabela}' WHERE {col_cmv} > 0")
    rows = cur.fetchall()
    
    print(f'  Produtos com CMV: {len(rows)}')
    
    payload = {}
    for row in rows:
        d = dict(row)
        sku    = str(d.get(col_sku, '') or '').strip()
        nome   = str(d.get(col_nome, '') or '').strip()
        cmv_br = limpar(d.get(col_cmv))
        cmv_pr = limpar(d.get(col_cmvp)) if col_cmvp else cmv_br
        
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
    
    print(f'  Resultado produtos: {ok_total} SKUs com CMV enviados')
    return ok_total

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]
    modo_inspecionar  = '--inspecionar' in args
    modo_so_anuncios  = '--so-anuncios' in args
    modo_so_produtos  = '--so-produtos' in args
    
    print(f'FAVA ECOM — Exportar banco.db → Railway')
    print(f'Banco:   {BANCO_DB}')
    print(f'Railway: {BASE_URL}\n')
    
    conn = conectar()
    tabelas = inspecionar(conn)
    
    if modo_inspecionar:
        print('Modo inspecionar — encerrando.')
        conn.close()
        return
    
    get_status_railway()
    
    ok_anuncios = 0
    ok_produtos  = 0
    
    if not modo_so_produtos:
        ok_anuncios = exportar_anuncios(conn, tabelas)
    
    if not modo_so_anuncios:
        ok_produtos = exportar_produtos(conn, tabelas)
    
    conn.close()
    
    print(f'\n{"="*50}')
    print('RESULTADO FINAL')
    print(f'  Anúncios enviados: {ok_anuncios}')
    print(f'  Produtos/CMV:      {ok_produtos}')
    print(f'{"="*50}')
    
    print('\n── Status final do Railway ─')
    get_status_railway()
    print('\n✅ Exportação concluída.')

if __name__ == '__main__':
    main()
