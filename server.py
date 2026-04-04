"""
FAVA ECOM — Servidor Railway v3
================================
- Proxy para Bling / ML / MP
- Banco PostgreSQL persistente (ou SQLite como fallback)
- Sincronização automática com Bling a cada 1h
- API /api/db/* para o painel consumir
"""

import http.server
import urllib.request
import urllib.error
from urllib.parse import parse_qs
import urllib.parse
import json
import os
import threading
import time
from datetime import datetime, date, timedelta

PORTA = int(os.environ.get('PORT', 8080))
DATABASE_URL = os.environ.get('DATABASE_URL', '')

BLING_ACCESS  = os.environ.get('BLING_ACCESS', '')
BLING_REFRESH = os.environ.get('BLING_REFRESH', '')
BLING_CLIENT  = os.environ.get('BLING_CLIENT', '19df6720532752f6888d5f0aad392bc8829974d3')
BLING_SECRET  = os.environ.get('BLING_SECRET', '590eed8f0b2fb1998e3f60335cef2a17bf5b2135fc69ec4a5ae925f520a8')
ML_ACCESS     = os.environ.get('ML_ACCESS', '')
ML_REFRESH    = os.environ.get('ML_REFRESH', '')

YAMPI_ALIAS   = os.environ.get('YAMPI_ALIAS',  'fava-ecom')
YAMPI_TOKEN   = os.environ.get('YAMPI_TOKEN',  'pMMopJjv6qB1d9ccwargjmOJQeJcHHsWrdnTw477')
YAMPI_SECRET  = os.environ.get('YAMPI_SECRET', 'sk_pjOvgahJ0QysIhv6i7RXhs3oynRRI5O1WaQXB')

PROXY = {
    '/api/bling/': 'https://www.bling.com.br/Api/v3/',
    '/api/ml/':    'https://api.mercadolibre.com/',
    '/api/mp/':    'https://api.mercadopago.com/',
}

_db = None
_db_lock = threading.Lock()
_bling_token = {'access': BLING_ACCESS, 'refresh': BLING_REFRESH}
_ml_token    = {'access': ML_ACCESS,    'refresh': ML_REFRESH}

# ────────────────────────────────────────────────────────────────
# BANCO DE DADOS
# ────────────────────────────────────────────────────────────────
def get_db():
    global _db
    if _db:
        try:
            cur = _db.cursor()
            cur.execute('SELECT 1')
            return _db
        except:
            _db = None

    if DATABASE_URL:
        try:
            import psycopg2
            _db = psycopg2.connect(DATABASE_URL, sslmode='require')
            _db.autocommit = True
            print('[DB] PostgreSQL conectado')
            return _db
        except Exception as e:
            print(f'[DB] PostgreSQL falhou ({e}) — usando SQLite')

    import sqlite3
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fava.db')
    _db = sqlite3.connect(db_path, check_same_thread=False)
    _db.row_factory = sqlite3.Row
    print(f'[DB] SQLite: {db_path}')
    return _db

IS_PG = bool(DATABASE_URL)

def criar_tabelas():
    db = get_db()
    sqls = []
    if IS_PG:
        sqls = [
            """CREATE TABLE IF NOT EXISTS produtos (
                sku TEXT PRIMARY KEY, nome TEXT, marca TEXT, familia TEXT,
                custo REAL DEFAULT 0, custo_br REAL DEFAULT 0, custo_pr REAL DEFAULT 0,
                estoque INTEGER DEFAULT 0, ipi REAL DEFAULT 0, cred_icms REAL DEFAULT 0,
                fornecedor TEXT, preco_venda REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS historico_compras (
                id SERIAL PRIMARY KEY, nf TEXT, fornecedor TEXT, data_emissao TEXT,
                sku TEXT, nome TEXT, qtd REAL DEFAULT 0, vunit REAL DEFAULT 0,
                vtot REAL DEFAULT 0, ipi_p REAL DEFAULT 0, ipi_un REAL DEFAULT 0,
                icms_p REAL DEFAULT 0, cred_pc REAL DEFAULT 0, custo_r REAL DEFAULT 0,
                cmv_br REAL DEFAULT 0, cmv_pr REAL DEFAULT 0, ncm TEXT, cfop TEXT,
                created_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS nf_entrada (
                chave TEXT PRIMARY KEY, nf TEXT, fornecedor TEXT, cnpj TEXT,
                emissao TEXT, valor REAL DEFAULT 0, created_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS boletos (
                id SERIAL PRIMARY KEY, nf_chave TEXT, fornecedor TEXT, cnpj TEXT,
                nf TEXT, emissao TEXT, valor_nf REAL DEFAULT 0, parcela TEXT,
                vencimento TEXT, valor REAL DEFAULT 0, status TEXT DEFAULT 'A PAGAR',
                created_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS ml_listings (
                id TEXT PRIMARY KEY, sku TEXT, titulo TEXT, preco REAL DEFAULT 0,
                sale_fee REAL DEFAULT 0, listing_type TEXT, free_shipping INTEGER DEFAULT 0,
                status TEXT, margem_minima REAL DEFAULT 0, frete_medio REAL DEFAULT 0,
                desconto REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS pedidos (
                id TEXT PRIMARY KEY, canal TEXT, data TEXT, status TEXT,
                total REAL DEFAULT 0, uf TEXT, frete REAL DEFAULT 0, itens TEXT,
                created_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS sync_log (
                id SERIAL PRIMARY KEY, tipo TEXT, resultado TEXT,
                created_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS cprod_map (
                cprod TEXT PRIMARY KEY, sku TEXT, nome TEXT,
                cmv_br REAL DEFAULT 0, cmv_pr REAL DEFAULT 0)""",
            """CREATE TABLE IF NOT EXISTS pedrinho (
                codigo TEXT PRIMARY KEY,
                descricao TEXT,
                storyselling TEXT,
                fotos TEXT,
                qtd_fotos INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS kits (
                id SERIAL PRIMARY KEY,
                sku TEXT UNIQUE, nome TEXT, itens TEXT,
                justificativa TEXT, peso REAL DEFAULT 0,
                titulo_ml TEXT, descricao TEXT, descricao_completa TEXT,
                categoria TEXT, tarefas TEXT,
                status TEXT DEFAULT 'aprovado',
                created_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS kits_mapa (
                sku_componente TEXT NOT NULL,
                sku_kit TEXT NOT NULL,
                qtd REAL DEFAULT 1,
                fonte TEXT DEFAULT 'auto',
                PRIMARY KEY (sku_componente, sku_kit))""",
            """CREATE TABLE IF NOT EXISTS pedidos_pc (
                id TEXT PRIMARY KEY,
                numero TEXT, data TEXT, canal TEXT, uf TEXT,
                total REAL DEFAULT 0, lucro REAL,
                margem REAL, frete REAL DEFAULT 0,
                sem_imposto INTEGER DEFAULT 0,
                sem_custo INTEGER DEFAULT 0,
                itens TEXT,
                created_at TIMESTAMP DEFAULT NOW())""",
            """CREATE TABLE IF NOT EXISTS produto_cadastro (
                id SERIAL PRIMARY KEY,
                sku TEXT UNIQUE, nome TEXT, fornecedor TEXT,
                codigo_fornecedor TEXT, ncm TEXT, cst TEXT, cfop TEXT,
                ipi REAL DEFAULT 0, tem_st INTEGER DEFAULT 0,
                custo REAL DEFAULT 0, custo_br REAL DEFAULT 0, custo_pr REAL DEFAULT 0,
                peso REAL DEFAULT 0, comprimento REAL DEFAULT 0,
                largura REAL DEFAULT 0, altura REAL DEFAULT 0,
                ean TEXT, categoria TEXT, familia TEXT, fotos TEXT,
                titulo_ml TEXT, titulo_shopee TEXT, titulo_yampi TEXT, titulo_facebook TEXT,
                descricao_ml TEXT,
                preco_ml_classico REAL DEFAULT 0, preco_ml_premium REAL DEFAULT 0,
                preco_shopee REAL DEFAULT 0, preco_yampi REAL DEFAULT 0,
                preco_balcao REAL DEFAULT 0, preco_atacado REAL DEFAULT 0,
                status_cadastro TEXT DEFAULT 'rascunho', tarefas TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW())""",
        ]
    else:
        sqls = [
            """CREATE TABLE IF NOT EXISTS produtos (
                sku TEXT PRIMARY KEY, nome TEXT, marca TEXT, familia TEXT,
                custo REAL DEFAULT 0, custo_br REAL DEFAULT 0, custo_pr REAL DEFAULT 0,
                estoque INTEGER DEFAULT 0, ipi REAL DEFAULT 0, cred_icms REAL DEFAULT 0,
                fornecedor TEXT, preco_venda REAL DEFAULT 0,
                updated_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS historico_compras (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nf TEXT, fornecedor TEXT,
                data_emissao TEXT, sku TEXT, nome TEXT, qtd REAL DEFAULT 0,
                vunit REAL DEFAULT 0, vtot REAL DEFAULT 0, ipi_p REAL DEFAULT 0,
                ipi_un REAL DEFAULT 0, icms_p REAL DEFAULT 0, cred_pc REAL DEFAULT 0,
                custo_r REAL DEFAULT 0, cmv_br REAL DEFAULT 0, cmv_pr REAL DEFAULT 0,
                ncm TEXT, cfop TEXT, created_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS nf_entrada (
                chave TEXT PRIMARY KEY, nf TEXT, fornecedor TEXT, cnpj TEXT,
                emissao TEXT, valor REAL DEFAULT 0,
                created_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS boletos (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nf_chave TEXT, fornecedor TEXT,
                cnpj TEXT, nf TEXT, emissao TEXT, valor_nf REAL DEFAULT 0,
                parcela TEXT, vencimento TEXT, valor REAL DEFAULT 0,
                status TEXT DEFAULT 'A PAGAR',
                created_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS ml_listings (
                id TEXT PRIMARY KEY, sku TEXT, titulo TEXT, preco REAL DEFAULT 0,
                sale_fee REAL DEFAULT 0, listing_type TEXT, free_shipping INTEGER DEFAULT 0,
                status TEXT, margem_minima REAL DEFAULT 0, frete_medio REAL DEFAULT 0,
                desconto REAL DEFAULT 0,
                updated_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS pedidos (
                id TEXT PRIMARY KEY, canal TEXT, data TEXT, status TEXT,
                total REAL DEFAULT 0, uf TEXT, frete REAL DEFAULT 0, itens TEXT,
                created_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, tipo TEXT, resultado TEXT,
                created_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS cprod_map (
                cprod TEXT PRIMARY KEY, sku TEXT, nome TEXT,
                cmv_br REAL DEFAULT 0, cmv_pr REAL DEFAULT 0)""",
            """CREATE TABLE IF NOT EXISTS pedrinho (
                codigo TEXT PRIMARY KEY,
                descricao TEXT,
                storyselling TEXT,
                fotos TEXT,
                qtd_fotos INTEGER DEFAULT 0,
                updated_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS kits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT UNIQUE, nome TEXT, itens TEXT,
                justificativa TEXT, peso REAL DEFAULT 0,
                titulo_ml TEXT, descricao TEXT, descricao_completa TEXT,
                categoria TEXT, tarefas TEXT,
                status TEXT DEFAULT 'aprovado',
                created_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS pedidos_pc (
                id TEXT PRIMARY KEY,
                numero TEXT, data TEXT, canal TEXT, uf TEXT,
                total REAL DEFAULT 0, lucro REAL,
                margem REAL, frete REAL DEFAULT 0,
                sem_imposto INTEGER DEFAULT 0,
                sem_custo INTEGER DEFAULT 0,
                itens TEXT,
                created_at DATETIME DEFAULT (datetime('now')))""",
            """CREATE TABLE IF NOT EXISTS produto_cadastro (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT UNIQUE, nome TEXT, fornecedor TEXT,
                codigo_fornecedor TEXT, ncm TEXT, cst TEXT, cfop TEXT,
                ipi REAL DEFAULT 0, tem_st INTEGER DEFAULT 0,
                custo REAL DEFAULT 0, custo_br REAL DEFAULT 0, custo_pr REAL DEFAULT 0,
                peso REAL DEFAULT 0, comprimento REAL DEFAULT 0,
                largura REAL DEFAULT 0, altura REAL DEFAULT 0,
                ean TEXT, categoria TEXT, familia TEXT, fotos TEXT,
                titulo_ml TEXT, titulo_shopee TEXT, titulo_yampi TEXT, titulo_facebook TEXT,
                descricao_ml TEXT,
                preco_ml_classico REAL DEFAULT 0, preco_ml_premium REAL DEFAULT 0,
                preco_shopee REAL DEFAULT 0, preco_yampi REAL DEFAULT 0,
                preco_balcao REAL DEFAULT 0, preco_atacado REAL DEFAULT 0,
                status_cadastro TEXT DEFAULT 'rascunho', tarefas TEXT,
                created_at DATETIME DEFAULT (datetime('now')),
                updated_at DATETIME DEFAULT (datetime('now')))""",
        ]
    with _db_lock:
        cur = db.cursor()
        for sql in sqls:
            try: cur.execute(sql)
            except Exception as e: print(f'[DB] {e}')
        if not IS_PG: db.commit()
    print('[DB] Tabelas prontas')
    # Migração: adiciona coluna desconto se não existir
    try:
        exe("ALTER TABLE ml_listings ADD COLUMN desconto REAL DEFAULT 0")
    except: pass
    try:
        exe("""CREATE TABLE IF NOT EXISTS campanha_historico (
            id SERIAL PRIMARY KEY, mlb_id TEXT, sku TEXT, titulo TEXT, campanha TEXT,
            desconto REAL DEFAULT 0, preco_original REAL DEFAULT 0,
            preco_final REAL DEFAULT 0, lucro_estimado REAL DEFAULT 0,
            margem_estimada REAL DEFAULT 0, status TEXT,
            data_aplicacao TIMESTAMP DEFAULT NOW()
        )""")
    except: pass
    try:
        exe("CREATE INDEX IF NOT EXISTS idx_ch_mlb ON campanha_historico(mlb_id)")
    except: pass
    try:
        exe("ALTER TABLE ml_listings ADD COLUMN IF NOT EXISTS lucro_estimado REAL DEFAULT 0")
    except: pass
    try:
        exe("ALTER TABLE ml_listings ADD COLUMN IF NOT EXISTS margem_real REAL DEFAULT 0")
    except: pass
    try:
        exe("""CREATE TABLE IF NOT EXISTS pedidos_pc (
            id TEXT PRIMARY KEY, numero TEXT, data TEXT, canal TEXT, uf TEXT,
            total REAL DEFAULT 0, lucro REAL, margem REAL, frete REAL DEFAULT 0,
            sem_imposto INTEGER DEFAULT 0, sem_custo INTEGER DEFAULT 0,
            itens TEXT, created_at TIMESTAMP DEFAULT NOW())""")
        print('[DB] Tabela pedidos_pc criada/verificada')
        print('[DB] Coluna desconto adicionada em ml_listings')
    except: pass

def qmark(n):
    return ','.join(['%s' if IS_PG else '?']*n)

def exe(sql, params=(), fetchall=False, fetchone=False):
    with _db_lock:
        db = get_db()
        try:
            cur = db.cursor()
            cur.execute(sql, params)
            if fetchall:
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
            if fetchone:
                r = cur.fetchone()
                if r is None: return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, r)) if not hasattr(r,'keys') else dict(r)
            if not IS_PG: db.commit()
            return cur.rowcount
        except Exception as e:
            if not IS_PG:
                try: db.rollback()
                except: pass
            raise

def upsert_produto(sku, nome='', marca='', familia='', custo=0, custo_br=0, custo_pr=0,
                   estoque=0, ipi=0, cred_icms=0, fornecedor='', preco_venda=0):
    conflict = ('DO UPDATE SET nome=EXCLUDED.nome,custo=EXCLUDED.custo,custo_br=EXCLUDED.custo_br,'
                'custo_pr=EXCLUDED.custo_pr,estoque=EXCLUDED.estoque,ipi=EXCLUDED.ipi,'
                'fornecedor=EXCLUDED.fornecedor,preco_venda=EXCLUDED.preco_venda') if IS_PG else \
               ('DO UPDATE SET nome=excluded.nome,custo=excluded.custo,custo_br=excluded.custo_br,'
                'custo_pr=excluded.custo_pr,estoque=excluded.estoque,ipi=excluded.ipi,'
                'fornecedor=excluded.fornecedor,preco_venda=excluded.preco_venda')
    sql = (f"INSERT INTO produtos (sku,nome,marca,familia,custo,custo_br,custo_pr,estoque,ipi,cred_icms,fornecedor,preco_venda) "
           f"VALUES ({qmark(12)}) ON CONFLICT(sku) {conflict}")
    exe(sql, (sku,nome,marca,familia,custo,custo_br,custo_pr,estoque,ipi,cred_icms,fornecedor,preco_venda))

# ────────────────────────────────────────────────────────────────
# TOKENS
# ────────────────────────────────────────────────────────────────
def salvar_tokens_db(tipo, access, refresh=None):
    if tipo == 'bling':
        _bling_token['access'] = access
        if refresh: _bling_token['refresh'] = refresh
    elif tipo == 'ml':
        _ml_token['access'] = access
        if refresh: _ml_token['refresh'] = refresh
    import base64
    data = base64.b64encode(json.dumps({'bling':_bling_token,'ml':_ml_token}).encode()).decode()
    p = '%s' if IS_PG else '?'
    try:
        exe(f"DELETE FROM sync_log WHERE tipo={p}", ('_tokens',))
        exe(f"INSERT INTO sync_log (tipo,resultado) VALUES ({p},{p})", ('_tokens', data))
    except: pass

def carregar_tokens_salvos():
    try:
        p = '%s' if IS_PG else '?'
        row = exe(f"SELECT resultado FROM sync_log WHERE tipo={p} ORDER BY created_at DESC LIMIT 1",
                  ('_tokens',), fetchone=True)
        if row:
            import base64
            d = json.loads(base64.b64decode(row['resultado']))
            if d.get('bling',{}).get('access'): _bling_token.update(d['bling'])
            if d.get('ml',{}).get('access'):    _ml_token.update(d['ml'])
            print('[AUTH] Tokens restaurados')
    except Exception as e:
        print(f'[AUTH] Tokens não restaurados: {e}')

# ────────────────────────────────────────────────────────────────
# SYNC BLING
# ────────────────────────────────────────────────────────────────
def renovar_bling():
    rt = _bling_token.get('refresh','')
    if not rt: return False
    import base64
    creds = base64.b64encode(f'{BLING_CLIENT}:{BLING_SECRET}'.encode()).decode()
    try:
        body = f'grant_type=refresh_token&refresh_token={rt}'.encode()
        req = urllib.request.Request(
            'https://www.bling.com.br/Api/v3/oauth/token', data=body,
            headers={'Content-Type':'application/x-www-form-urlencoded',
                     'Authorization':f'Basic {creds}'}, method='POST')
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            if d.get('access_token'):
                salvar_tokens_db('bling', d['access_token'], d.get('refresh_token', rt))
                print('[BLING] Token renovado'); return True
    except Exception as e: print(f'[BLING] Renovar: {e}')
    return False

def bling_get(path, pagina=1):
    token = _bling_token.get('access','')
    if not token: return None
    sep = '&' if '?' in path else '?'
    url = f'https://www.bling.com.br/Api/v3/{path}{sep}pagina={pagina}&limite=100'
    req = urllib.request.Request(url, headers={'Authorization':f'Bearer {token}','Accept':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 401 and renovar_bling():
            return bling_get(path, pagina)
        print(f'[BLING] {path} erro {e.code}'); return None
    except Exception as e:
        print(f'[BLING] {path}: {e}'); return None

def sync_produtos():
    print('[SYNC] Produtos...')
    n=0; pagina=1
    while True:
        d = bling_get('produtos', pagina)
        if not d: break
        items = d.get('data',[])
        if not items: break
        for p in items:
            sku = str(p.get('codigo','') or p.get('id','')).strip()
            if not sku: continue
            nome  = (p.get('descricao','') or '').strip()
            custo = float(p.get('precoCusto') or 0)
            preco = float(p.get('preco') or 0)
            estq  = int((p.get('estoque') or {}).get('saldoVirtualTotal') or 0)
            forn  = ''
            if isinstance(p.get('fornecedor'), dict):
                forn = p['fornecedor'].get('nome','') or ''
            ipi = float((p.get('tributacao') or {}).get('ipi') or 0)
            try: upsert_produto(sku, nome, custo=custo, custo_br=custo, estoque=estq, ipi=ipi, fornecedor=forn, preco_venda=preco)
            except Exception as e: print(f'[SYNC] prod {sku}: {e}')
            n+=1
        if len(items)<100: break
        pagina+=1; time.sleep(0.3)
    try:
        sem_cmv = exe("SELECT sku FROM produtos WHERE custo_br = 0 OR custo_br IS NULL", fetchall=True)
        if sem_cmv:
            for row in sem_cmv:
                s = row['sku']
                p_ph = '%s' if IS_PG else '?'
                hist = exe(f"SELECT AVG(cmv_br) as media FROM historico_compras WHERE sku={p_ph} AND cmv_br>0",
                           (s,), fetchone=True)
                if hist and hist.get('media',0)>0:
                    exe(f"UPDATE produtos SET custo_br={hist['media']}, custo={hist['media']} WHERE sku='{s}'")
        print(f'[SYNC] CMV complementado do historico')
    except Exception as e:
        print(f'[SYNC] CMV complemento erro: {e}')
    print(f'[SYNC] {n} produtos'); return n

def sync_pedidos():
    print('[SYNC] Pedidos...')
    hoje = date.today()
    de = (hoje - timedelta(days=60)).isoformat()
    n=0; pagina=1
    while True:
        d = bling_get(f'pedidos/vendas?dataInicial={de}&dataFinal={hoje.isoformat()}', pagina)
        if not d: break
        items = d.get('data',[])
        if not items: break
        for p in items:
            pid = str(p.get('id',''))
            if not pid: continue
            raw = p.get('itens',[]) or []
            itens = json.dumps([{'sku':i.get('codigo',''),'nome':i.get('descricao',''),
                'qtd':float(i.get('quantidade',1)),'preco':float(i.get('valor',0))} for i in raw])
            canal = (p.get('canal') or {}).get('descricao','Bling') if isinstance(p.get('canal'),dict) else 'Bling'
            status = (p.get('situacao') or {}).get('nome','') if isinstance(p.get('situacao'),dict) else ''
            total = float(p.get('totalProdutos',0) or p.get('total',0) or 0)
            data_p = (p.get('data','') or '')[:10]
            conflict = 'ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status' if IS_PG else 'ON CONFLICT(id) DO UPDATE SET status=excluded.status'
            try:
                exe(f"INSERT INTO pedidos (id,canal,data,status,total,uf,frete,itens) VALUES ({qmark(8)}) {conflict}",
                    (pid,canal,data_p,status,total,'PR',0,itens))
                n+=1
            except Exception as e: print(f'[SYNC] ped {pid}: {e}')
        if len(items)<100: break
        pagina+=1; time.sleep(0.3)
    print(f'[SYNC] {n} pedidos'); return n

def sync_frete_por_anuncio():
    print('[SYNC] Calculando frete médio por anúncio...')
    try:
        pedidos = exe("SELECT itens FROM pedidos WHERE canal LIKE '%ML%' OR canal LIKE '%Mercado%'", fetchall=True)
        frete_map = {}
        for p in pedidos:
            if not p.get('itens'): continue
            try:
                itens_list = p['itens'] if isinstance(p['itens'], list) else json.loads(p['itens'])
                for it in itens_list:
                    mlb = it.get('mlb','')
                    frete = float(it.get('frete',0) or it.get('frete_un',0) or 0)
                    if mlb and frete > 0:
                        if mlb not in frete_map: frete_map[mlb] = []
                        frete_map[mlb].append(frete)
            except: pass
        n = 0
        for mlb, fretes in frete_map.items():
            if fretes:
                media = sum(fretes)/len(fretes)
                try:
                    exe(f"UPDATE ml_listings SET frete_medio={media} WHERE id='{mlb}'")
                    n += 1
                except: pass
        print(f'[SYNC] {n} anúncios com frete médio atualizado')
        return n
    except Exception as e:
        print(f'[SYNC] frete erro: {e}'); return 0

def ml_get(path):
    token = _ml_token.get('access','')
    if not token: return None
    url = f'https://api.mercadolibre.com/{path}'
    req = urllib.request.Request(url, headers={'Authorization':f'Bearer {token}','Accept':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f'[ML] {path[:60]} erro {e.code}'); return None
    except Exception as e:
        print(f'[ML] {path[:60]} erro: {e}'); return None

def sync_ml_listings():
    if not _ml_token.get('access'): return 0
    print('[SYNC] Anúncios ML...')
    all_ids = []
    scroll = None
    for _ in range(30):
        path = f'users/537714337/items/search?status=active&limit=50'
        if scroll: path += f'&scroll_id={scroll}'
        d = ml_get(path)
        if not d: break
        ids = d.get('results',[])
        if not ids: break
        all_ids += ids
        scroll = d.get('scroll_id')
        if len(ids) < 50 or not scroll: break
        time.sleep(0.2)
    if not all_ids:
        print('[ML] Nenhum anúncio encontrado'); return 0
    print(f'[ML] {len(all_ids)} anúncios — buscando detalhes...')
    salvos = 0
    for i in range(0, len(all_ids), 20):
        lote = ','.join(all_ids[i:i+20])
        d = ml_get(f'items?ids={lote}&attributes=id,title,price,seller_sku,listing_type_id,status,sale_fee,shipping')
        if not d: continue
        for x in (d if isinstance(d, list) else []):
            if x.get('code') != 200: continue
            it = x.get('body', {})
            iid = it.get('id','')
            if not iid: continue
            sku       = str(it.get('seller_sku','') or '').strip()
            titulo    = (it.get('title','') or '').strip()
            preco     = float(it.get('price') or 0)
            sale_fee  = float(it.get('sale_fee') or 0)
            ltype     = it.get('listing_type_id','')
            shp       = it.get('shipping',{}) or {}
            free_ship = 1 if (shp.get('free_shipping') or ltype in ('gold_pro','gold_premium')) else 0
            status_it = it.get('status','')
            try:
                c = ('ON CONFLICT(id) DO UPDATE SET sku=EXCLUDED.sku,titulo=EXCLUDED.titulo,'
                     'preco=EXCLUDED.preco,sale_fee=EXCLUDED.sale_fee,listing_type=EXCLUDED.listing_type,'
                     'free_shipping=EXCLUDED.free_shipping,status=EXCLUDED.status') if IS_PG else \
                    ('ON CONFLICT(id) DO UPDATE SET sku=excluded.sku,titulo=excluded.titulo,'
                     'preco=excluded.preco,sale_fee=excluded.sale_fee,listing_type=excluded.listing_type,'
                     'free_shipping=excluded.free_shipping,status=excluded.status')
                exe(f"INSERT INTO ml_listings (id,sku,titulo,preco,sale_fee,listing_type,free_shipping,status) VALUES ({qmark(8)}) {c}",
                    (iid,sku,titulo,preco,sale_fee,ltype,free_ship,status_it))
                salvos += 1
            except Exception as e:
                print(f'[ML] listing {iid}: {e}')
        time.sleep(0.3)
    print(f'[ML] {salvos} anúncios salvos')
    return salvos

def sync_bling_anuncios():
    if not _bling_token.get('access'): return 0
    print('[SYNC] Anúncios Bling...')
    endpoint = None
    for ep in ['anuncios', 'produtos?tipo=V&situacao=Ativo', 'integracoes/marketplace/anuncios']:
        d = bling_get(ep, 1)
        if d and d.get('data') is not None:
            endpoint = ep
            print(f'[BLING] Endpoint anúncios: {ep}')
            break
    if not endpoint:
        print('[BLING] Nenhum endpoint de anúncios funcionou')
        return 0
    n = 0; pagina = 1
    while True:
        d = bling_get(endpoint, pagina)
        if not d: break
        items = d.get('data', [])
        if not items: break
        for it in items:
            try:
                mlb    = str(it.get('idSite') or it.get('idAnuncio') or '').strip()
                sku    = str(it.get('codigo') or it.get('sku') or '').strip()
                titulo = str(it.get('nome') or it.get('titulo') or '').strip()
                preco  = float(it.get('preco') or 0)
                taxa   = float(it.get('percentualComissao') or it.get('taxa') or 0)
                frete  = float(it.get('frete') or 0)
                ltype  = str(it.get('tipoAnuncio') or '').lower()
                free_s = 1 if 'premium' in ltype or 'gold_pro' in ltype else 0
                status = str(it.get('situacao') or 'active').lower()
                cmv    = float(it.get('precoCusto') or 0)
                if not mlb or not mlb.startswith('MLB'): continue
                c = ('ON CONFLICT(id) DO UPDATE SET sku=EXCLUDED.sku,titulo=EXCLUDED.titulo,'
                     'preco=EXCLUDED.preco,sale_fee=EXCLUDED.sale_fee,listing_type=EXCLUDED.listing_type,'
                     'free_shipping=EXCLUDED.free_shipping,status=EXCLUDED.status,frete_medio=EXCLUDED.frete_medio') if IS_PG else \
                    ('ON CONFLICT(id) DO UPDATE SET sku=excluded.sku,titulo=excluded.titulo,'
                     'preco=excluded.preco,sale_fee=excluded.sale_fee,listing_type=excluded.listing_type,'
                     'free_shipping=excluded.free_shipping,status=excluded.status,frete_medio=excluded.frete_medio')
                exe(f"INSERT INTO ml_listings (id,sku,titulo,preco,sale_fee,listing_type,free_shipping,status,frete_medio) VALUES ({qmark(9)}) {c}",
                    (mlb,sku,titulo,preco,taxa,ltype,free_s,status,frete))
                if cmv > 0 and sku:
                    upsert_produto(sku, titulo, custo=cmv, custo_br=cmv)
                n += 1
            except Exception as e:
                print(f'[BLING] anuncio erro: {e}')
        if len(items) < 100: break
        pagina += 1; time.sleep(0.3)
    print(f'[BLING] {n} anúncios salvos')
    return n

def sync_all():
    n1 = sync_produtos()
    n2 = sync_pedidos()
    n3 = sync_ml_listings() if _ml_token.get('access') else 0
    n4 = sync_bling_anuncios() if _bling_token.get('access') else 0
    sync_frete_por_anuncio()
    msg = f'produtos={n1} pedidos={n2} listings_ml={n3} listings_bling={n4}'
    p = '%s' if IS_PG else '?'
    try: exe(f"INSERT INTO sync_log (tipo,resultado) VALUES ({p},{p})", ('sync', msg))
    except: pass
    print(f'[SYNC] {msg}')

def agendar_sync():
    def loop():
        time.sleep(15)
        while True:
            try:
                if _bling_token.get('access'): sync_all()
                else: print('[SYNC] Aguardando token Bling...')
            except Exception as e: print(f'[SYNC] Erro: {e}')
            time.sleep(3600)
    threading.Thread(target=loop, daemon=True).start()
    print('[SYNC] Agendador 1h iniciado')

    # ── Renovação automática do Bling a cada 5h50min ──
    def loop_bling():
        time.sleep(60)  # aguarda 1min para servidor inicializar
        # Primeiro: tentar carregar tokens salvos
        carregar_tokens_salvos()
        if _bling_token.get('access'):
            print('[BLING] Tokens carregados do banco')
        while True:
            try:
                rt = _bling_token.get('refresh', '')
                if rt:
                    ok = renovar_bling()
                    print(f'[BLING] Auto-renovação: {"OK" if ok else "FALHOU"} | access={str(_bling_token.get("access",""))[:15]}...')
                else:
                    print('[BLING] Sem refresh_token — aguardando OAuth manual')
            except Exception as e:
                print(f'[BLING] Erro na renovação: {e}')
            time.sleep(21000)  # 5h50min = 21000s
    threading.Thread(target=loop_bling, daemon=True).start()
    print('[BLING] Auto-renovação a cada 5h50min iniciada')

# ────────────────────────────────────────────────────────────────
# HTTP SERVER
# ────────────────────────────────────────────────────────────────
class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if '/api/' in self.path:
            print(f"[{args[1] if len(args)>1 else '?'}] {self.path[:80]}")

    def do_OPTIONS(self):
        self.send_response(200); self._cors(); self.end_headers()

    def do_GET(self):
        clean = self.path.split('?')[0]
        routes = {
            '/api/db/produtos':          self._get_produtos,
            '/api/db/historico':         self._get_historico,
            '/api/db/pedidos':           self._get_pedidos,
            '/api/db/boletos':           self._get_boletos,
            '/api/db/nfs':               self._get_nfs,
            '/api/db/listings':          self._get_listings,
            '/api/db/listings-performance': self._get_listings_performance,
            '/api/db/campanha':            self._get_campanha,
            '/api/db/kits-mapa':         self._get_kits_mapa,
            '/api/db/cprod-map':         self._get_cprod_map,
            '/api/db/cprod-lookup':      self._get_cprod_lookup,
            '/api/db/proximo-sku':       self._get_proximo_sku,
            '/api/db/pedrinho':          self._get_pedrinho,
            '/api/db/kits':              self._get_kits,
            '/api/db/cadastros':         self._get_cadastros,
            '/api/db/status':            self._get_status,
            '/api/db/pedidos-pc':         self._get_pedidos_pc,
            '/api/cmv-cache':            self._get_cmv_compat,
            '/api/sync/now':             self._sync_now,
            '/api/sync/bling-anuncios':  self._sync_bling_anuncios,
            # ── NOVO: Bling OAuth ──────────────────────────────
            '/api/bling/renovar':        self._bling_renovar,
            '/api/bling/autorizar':      self._bling_autorizar,
            '/api/bling/callback':       self._bling_callback,
            '/api/bling/trocar':         self._bling_trocar,
        }
        if clean in routes: routes[clean]()
        elif clean.startswith('/api/yampi/'): self._yampi_proxy('GET')
        elif clean.startswith('/api/'): self._proxy('GET')
        else: super().do_GET()

    def do_POST(self):
        clean = self.path.split('?')[0]
        routes = {
            '/api/db/produto':            self._post_produto,
            '/api/db/historico':          self._post_historico,
            '/api/db/nf':                 self._post_nf,
            '/api/db/listing':            self._post_listing,
            '/api/sync/lucro':            self._sync_lucro,
            '/api/db/campanha':           self._post_campanha,
            '/api/db/cprod-map':          self._post_cprod_map,
            '/api/db/listings-batch':     self._post_listings_batch,
            '/api/db/kits-mapa':          self._post_kits_mapa,
            '/api/db/pedrinho/importar':  self._post_pedrinho_importar,
            '/api/db/kit':                self._post_kit,
            '/api/db/cadastro':           self._post_cadastro,
            '/api/db/pedidos-pc':         self._post_pedidos_pc,
            '/api/auth/tokens':           self._post_tokens,
            '/api/bling/set-token':         self._bling_set_token,
            '/api/cmv-cache':             self._post_cmv,
        }
        if clean in routes: routes[clean]()
        elif clean.startswith('/api/'): self._proxy('POST')
        else: self.send_error(405)

    def do_PUT(self):
        if self.path.startswith('/api/yampi/'): self._yampi_proxy('PUT')
        elif self.path.startswith('/api/'): self._proxy('PUT')
        else: self.send_error(405)

    def _body(self):
        n = int(self.headers.get('Content-Length',0))
        return json.loads(self.rfile.read(n)) if n else {}

    # ────────────────────────────────────────────────────────────
    # BLING OAUTH — NOVO
    # ────────────────────────────────────────────────────────────
    def _bling_set_token(self):
        try:
            body=json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))))
            access=body.get("access_token","").strip()
            refresh=body.get("refresh_token","").strip()
            if not access: self._ok({"ok":False,"error":"access_token obrigatorio"},400); return
            salvar_tokens_db("bling", access, refresh or None)
            self._ok({"ok":True,"msg":"Token Bling salvo"})
        except Exception as e: self._ok({"ok":False,"error":str(e)},500)

    def _bling_renovar(self):
        """GET /api/bling/renovar — usa refresh_token para obter novo access_token"""
        rt = _bling_token.get("refresh","")
        if not rt:
            self._ok({"ok":False,"error":"refresh_token nao disponivel. Faca OAuth manual via /api/bling/autorizar"}); return
        ok = renovar_bling()
        if ok:
            self._ok({"ok":True,"access_token": _bling_token.get("access","")[:20]+"...","msg":"Token renovado com sucesso"})
        else:
            self._ok({"ok":False,"error":"Falha ao renovar. refresh_token pode ter expirado. Acesse /api/bling/autorizar"})

    def _bling_autorizar(self):
        """GET /api/bling/autorizar — redireciona para autorização Bling"""
        redirect_uri = 'https://web-production-5aa0f.up.railway.app/api/bling/callback'
        url = (f'https://www.bling.com.br/Api/v3/oauth/authorize'
               f'?response_type=code&client_id={BLING_CLIENT}'
               f'&redirect_uri={urllib.parse.quote(redirect_uri, safe="")}&state=fava')
        self.send_response(302)
        self._cors()
        self.send_header('Location', url)
        self.end_headers()

    def _bling_callback(self):
        """GET /api/bling/callback — recebe code do Bling e troca por token"""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        code = qs.get('code', [''])[0]
        redirect_uri = 'https://web-production-5aa0f.up.railway.app/api/bling/callback'
        if not code:
            self._html_resp('<html><body><h2>❌ Código não encontrado na URL</h2></body></html>')
            return
        ok = self._trocar_code_bling(code, redirect_uri)
        if ok:
            self._html_resp("""<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#f0fff4">
            <h1 style="color:#22c55e">✅ Bling conectado com sucesso!</h1>
            <p>Tokens salvos. Sincronização iniciada.</p>
            <p>Pode fechar esta aba e voltar ao painel.</p>
            <script>setTimeout(()=>window.close(),4000)</script>
            </body></html>""")
        else:
            self._html_resp("""<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#fff0f0">
            <h2>❌ Erro ao trocar código</h2>
            <p>O código pode ter expirado (válido por 60 segundos).</p>
            <p><a href="/api/bling/autorizar">Clique aqui para tentar novamente</a></p>
            </body></html>""")

    def _bling_trocar(self):
        """GET /api/bling/trocar?code=XXX&redir=URL — troca code manual"""
        from urllib.parse import urlparse, parse_qs, unquote
        qs = parse_qs(urlparse(self.path).query)
        code  = qs.get('code',  [''])[0]
        redir = qs.get('redir', ['https://www.favaecom.com.br'])[0]
        if not code:
            self._html_resp('<html><body><h2>❌ Parâmetro ?code= não informado</h2></body></html>')
            return
        ok = self._trocar_code_bling(code, unquote(redir))
        if ok:
            self._html_resp("""<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#f0fff4">
            <h1 style="color:#22c55e">✅ Bling reautorizado!</h1>
            <p>Tokens salvos. Pode fechar esta aba.</p>
            <script>setTimeout(()=>window.close(),3000)</script>
            </body></html>""")
        else:
            self._html_resp("""<html><body style="font-family:sans-serif;text-align:center;padding:60px;background:#fff0f0">
            <h2>❌ Código expirado ou inválido</h2>
            <p><a href="/api/bling/autorizar">Clique aqui para autorizar novamente</a></p>
            </body></html>""")

    def _trocar_code_bling(self, code, redirect_uri):
        """Troca authorization_code por access+refresh token no Bling"""
        import base64
        creds = base64.b64encode(f'{BLING_CLIENT}:{BLING_SECRET}'.encode()).decode()
        body  = f'grant_type=authorization_code&code={code}&redirect_uri={urllib.parse.quote(redirect_uri, safe="")}'.encode()
        try:
            req = urllib.request.Request(
                'https://www.bling.com.br/Api/v3/oauth/token', data=body,
                headers={'Content-Type': 'application/x-www-form-urlencoded',
                         'Authorization': f'Basic {creds}'}, method='POST')
            with urllib.request.urlopen(req, timeout=15) as r:
                d = json.loads(r.read())
            if d.get('access_token'):
                salvar_tokens_db('bling', d['access_token'], d.get('refresh_token', ''))
                print(f'[BLING] ✅ Token obtido via authorization_code')
                threading.Thread(target=sync_all, daemon=True).start()
                return True
            print(f'[BLING] Sem access_token: {d}')
        except Exception as e:
            print(f'[BLING] Erro troca code: {e}')
        return False

    def _html_resp(self, html):
        body = html.encode('utf-8')
        self.send_response(200); self._cors()
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ────────────────────────────────────────────────────────────
    # GET routes
    # ────────────────────────────────────────────────────────────
    def _get_produtos(self):
        try: self._ok(exe("SELECT * FROM produtos ORDER BY sku", fetchall=True))
        except Exception as e: self._err(500, str(e))

    def _get_proximo_sku(self):
        try:
            with get_db() as db:
                row=db.fetchone("SELECT MAX(sku::integer) as m FROM produtos WHERE sku ~ '^[0-9]+$'")
                mx=int(row["m"] or 2933) if row and row["m"] else 2933
            self._ok({"proximo_sku": max(mx+1, 2934), "max_db": mx})
        except Exception as e: self._ok({"proximo_sku":2934,"error":str(e)})

    def _get_cprod_lookup(self):
        p=parse_qs(self.path.split("?")[1] if "?" in self.path else "")
        cprod=p.get("cprod",[""])[0]; cprods=p.get("cprods",[""])[0]
        try:
            with get_db() as db:
                if cprods:
                    lista=[c.strip() for c in cprods.split(",") if c.strip()]
                    ph=",".join(["%s"]*len(lista))
                    rows=db.fetchall(f"SELECT cprod,sku,nome,cmv_br FROM cprod_map WHERE cprod IN ({ph})",lista)
                elif cprod:
                    rows=db.fetchall("SELECT cprod,sku,nome,cmv_br FROM cprod_map WHERE cprod=%s",(cprod,))
                else: self._ok({"ok":False},400); return
                result={}
                for r in rows:
                    prod=db.fetchone("SELECT cmv_br,cmv_pr,peso,st,st_imposto,ipi,ncm FROM produtos WHERE sku=%s",(str(r["sku"]),)) or {}
                    result[str(r["cprod"])]={"sku":r["sku"],"nome":r["nome"],"cmv_br":prod.get("cmv_br") or r.get("cmv_br",0),"cmv_pr":prod.get("cmv_pr",0),"peso":prod.get("peso",0),"st":prod.get("st",0),"st_imposto":prod.get("st_imposto",0),"ipi":prod.get("ipi",0),"ncm":prod.get("ncm","")}
            self._ok(result)
        except Exception as e: self._ok({"ok":False,"error":str(e)},500)

    def _get_cprod_map(self):
        try:
            rows = exe("SELECT cprod, sku, nome, cmv_br, cmv_pr FROM cprod_map ORDER BY sku", fetchall=True)
            result = {r['cprod']: {'sku': r['sku'], 'nome': r['nome'], 'cmv_br': r['cmv_br'], 'cmv_pr': r['cmv_pr']} for r in rows}
            self._ok(result)
        except Exception as e: self._err(500, str(e))

    def _get_cmv_compat(self):
        try:
            rows = exe("SELECT sku, custo_br as cmv, custo_pr, nome FROM produtos WHERE custo_br > 0", fetchall=True)
            self._ok({r['sku']:{'cmv':r['cmv'],'cmvPr':r['custo_pr'],'nome':r['nome']} for r in rows})
        except Exception as e: self._err(500, str(e))

    def _get_historico(self):
        try: self._ok(exe("SELECT * FROM historico_compras ORDER BY data_emissao DESC LIMIT 1000", fetchall=True))
        except Exception as e: self._err(500, str(e))

    def _get_pedidos(self):
        try:
            rows = exe("SELECT * FROM pedidos ORDER BY data DESC LIMIT 500", fetchall=True)
            for r in rows:
                if isinstance(r.get('itens'),str):
                    try: r['itens'] = json.loads(r['itens'])
                    except: r['itens'] = []
            self._ok(rows)
        except Exception as e: self._err(500, str(e))

    def _get_boletos(self):
        try: self._ok(exe("SELECT * FROM boletos ORDER BY vencimento ASC LIMIT 300", fetchall=True))
        except Exception as e: self._err(500, str(e))

    def _get_nfs(self):
        try: self._ok(exe("SELECT * FROM nf_entrada ORDER BY emissao DESC LIMIT 300", fetchall=True))
        except Exception as e: self._err(500, str(e))

    def _get_listings(self):
        try:
            rows = exe("""
                SELECT l.id, l.sku, COALESCE(NULLIF(l.titulo,''), p.nome, l.sku) as titulo, l.preco,
                       l.sale_fee, l.listing_type, l.free_shipping, l.status,
                       l.margem_minima, l.frete_medio, l.desconto,
                       COALESCE(p.custo_br, cm.cmv_br, 0) as cmv,
                       COALESCE(p.custo_pr, cm.cmv_pr, 0) as cmv_pr
                FROM ml_listings l
                LEFT JOIN produtos p ON p.sku = l.sku
                LEFT JOIN cprod_map cm ON cm.sku = l.sku
                WHERE l.id IS NOT NULL
                ORDER BY l.titulo
            """, fetchall=True)
            self._ok(rows)
        except Exception as e: self._err(500, str(e))

    def _get_status(self):
        try:
            def cnt(t): return exe(f"SELECT COUNT(*) as n FROM {t}", fetchone=True)['n']
            p = '%s' if IS_PG else '?'
            last = exe(f"SELECT resultado, created_at FROM sync_log WHERE tipo={p} ORDER BY created_at DESC LIMIT 1",
                       ('sync',), fetchone=True)
            self._ok({'produtos':cnt('produtos'),'historico':cnt('historico_compras'),
                      'pedidos':cnt('pedidos'),'boletos':cnt('boletos'),
                      'ultimo_sync':last,'bling_ok':bool(_bling_token.get('access')),
                      'ml_ok':bool(_ml_token.get('access'))})
        except Exception as e: self._err(500, str(e))

    def _get_listings_performance(self):
        try:
            rows = exe("SELECT id,sku,titulo,preco,frete_medio,lucro_estimado,margem_real,desconto,status FROM ml_listings WHERE lucro_estimado IS NOT NULL ORDER BY margem_real DESC LIMIT 500", fetchall=True)
            self._ok(rows)
        except Exception as e: self._err(500, str(e))

    def _get_campanha(self):
        try:
            p = parse_qs(self.path.split("?")[1] if "?" in self.path else "")
            mlb = p.get("mlb_id",[""])[0]; camp = p.get("campanha",[""])[0]
            if mlb:
                rows = exe("SELECT * FROM campanha_historico WHERE mlb_id=%s ORDER BY data_aplicacao DESC LIMIT 50",(mlb,),fetchall=True)
            elif camp:
                rows = exe("SELECT * FROM campanha_historico WHERE campanha=%s ORDER BY mlb_id,data_aplicacao DESC",(camp,),fetchall=True)
            else:
                rows = exe("SELECT mlb_id,sku,titulo,campanha,desconto,preco_original,preco_final,lucro_estimado,margem_estimada,status,MAX(data_aplicacao) as data_aplicacao FROM campanha_historico GROUP BY mlb_id,sku,titulo,campanha,desconto,preco_original,preco_final,lucro_estimado,margem_estimada,status ORDER BY mlb_id LIMIT 2000",fetchall=True)
            self._ok(rows)
        except Exception as e: self._err(500, str(e))

    def _post_campanha(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))))
            rows = body.get("rows",[]); camp = body.get("campanha","fava_crescendo")
            saved = 0
            for r in rows:
                exe("INSERT INTO campanha_historico(mlb_id,sku,titulo,campanha,desconto,preco_original,preco_final,lucro_estimado,margem_estimada,status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (str(r.get("mlb_id","")),str(r.get("sku","")),str(r.get("titulo",""))[:200],camp,
                     float(r.get("desconto",0)),float(r.get("preco_original",0)),float(r.get("preco_final",0)),
                     float(r.get("lucro_estimado",0)),float(r.get("margem_estimada",0)),str(r.get("status",""))))
                saved += 1
            self._ok({"ok":True,"saved":saved})
        except Exception as e: self._err(500, str(e))

    def _sync_lucro(self):
        try:
            TAXAS = 0.0165+0.076+0.12+0.0381+0.02
            rows = exe("SELECT id,preco,sale_fee,frete_medio,desconto FROM ml_listings WHERE preco>0", fetchall=True)
            for r in rows:
                pf = float(r["preco"])*(1-float(r["desconto"] or 0)/100)
                rec = pf*(1-float(r["sale_fee"] or 0.17)-TAXAS)
                lucro = rec-float(r["frete_medio"] or 13)
                marg = (lucro/pf*100) if pf>0 else 0
                exe("UPDATE ml_listings SET lucro_estimado=%s,margem_real=%s WHERE id=%s",(round(lucro,2),round(marg,2),r["id"]))
            self._ok({"ok":True,"updated":len(rows)})
        except Exception as e: self._err(500, str(e))

    def _sync_bling_anuncios(self):
        threading.Thread(target=sync_bling_anuncios, daemon=True).start()
        self._ok({'ok':True,'msg':'Sync Bling anuncios iniciado'})

    def _sync_now(self):
        threading.Thread(target=sync_all, daemon=True).start()
        self._ok({'ok':True,'msg':'Sync iniciado'})


    def _yampi_proxy(self, method):
        """Proxy para Yampi API com auth automática."""
        subpath = self.path[len('/api/yampi'):]  # ex: /catalog/skus/292926764
        url = f'https://api.dooki.com.br/v2/{YAMPI_ALIAS}{subpath}'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Token': YAMPI_TOKEN,
            'User-Secret-Key': YAMPI_SECRET,
        }
        body = None
        if method in ('POST', 'PUT', 'PATCH'):
            n = int(self.headers.get('Content-Length', 0))
            if n: body = self.rfile.read(n)
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=20) as r:
                data = r.read()
                self.send_response(r.status); self._cors()
                self.send_header('Content-Type', 'application/json'); self.end_headers()
                self.wfile.write(data)
        except urllib.error.HTTPError as e:
            self.send_response(e.code); self._cors()
            self.send_header('Content-Type', 'application/json'); self.end_headers()
            self.wfile.write(e.read())
        except Exception as e:
            self._err(500, str(e))

    def _get_pedrinho(self):
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        cod = qs.get('codigo',[''])[0]
        q   = qs.get('q',[''])[0]
        db  = get_db()
        with _db_lock:
            cur = db.cursor()
            if cod:
                p_ph = '%s' if IS_PG else '?'
                cur.execute(f"SELECT * FROM pedrinho WHERE codigo={p_ph}", (cod,))
                row = cur.fetchone()
                if not row: self._ok({'error':'not_found'}); return
                cols = [d[0] for d in cur.description]
                self._ok(dict(zip(cols,row)) if not hasattr(row,'keys') else dict(row))
            elif q:
                p_ph = '%s' if IS_PG else '?'
                op   = 'ILIKE' if IS_PG else 'LIKE'
                cur.execute(f"SELECT codigo,descricao,qtd_fotos FROM pedrinho WHERE descricao {op} {p_ph} LIMIT 20", (f'%{q}%',))
                cols = [d[0] for d in cur.description]
                self._ok([dict(zip(cols,r)) if not hasattr(r,'keys') else dict(r) for r in cur.fetchall()])
            else:
                cur.execute("SELECT COUNT(*) as n FROM pedrinho")
                row = cur.fetchone()
                n = row[0] if not hasattr(row,'keys') else row['n']
                self._ok({'total': n})

    def _get_kits(self):
        try: self._ok(exe("SELECT * FROM kits ORDER BY created_at DESC", fetchall=True))
        except Exception as e: self._err(500, str(e))

    def _get_kits_mapa(self):
        try:
            sku = self.path.split('sku=')[-1].split('&')[0] if 'sku=' in self.path else ''
            p = '%s' if IS_PG else '?'
            if sku:
                rows = exe(f"SELECT sku_kit, qtd, fonte FROM kits_mapa WHERE sku_componente={p}", (sku,), fetchall=True)
            else:
                rows = exe("SELECT sku_componente, sku_kit, qtd FROM kits_mapa ORDER BY sku_componente", fetchall=True)
            self._ok({'data': rows})
        except Exception as e: self._err(500, str(e))

    def _get_cadastros(self):
        try: self._ok(exe("SELECT * FROM produto_cadastro ORDER BY created_at DESC LIMIT 200", fetchall=True))
        except Exception as e: self._err(500, str(e))

    # ────────────────────────────────────────────────────────────
    # POST routes
    # ────────────────────────────────────────────────────────────

    def _get_pedidos_pc(self):
        try:
            rows = exe("SELECT id,numero,data,canal,uf,total,lucro,margem,frete,sem_imposto,sem_custo FROM pedidos_pc ORDER BY data DESC LIMIT 1000", fetchall=True)
            self._ok({'total': len(rows), 'rows': rows})
        except Exception as e: self._err(500, str(e))

    def _post_pedidos_pc(self):
        """POST /api/db/pedidos-pc — bulk insert pedidos do Preço Certo"""
        try:
            payload = self._body()
            pedidos = payload if isinstance(payload, list) else payload.get('pedidos', [])
            ok = 0; errs = 0
            ignore = 'ON CONFLICT DO NOTHING' if IS_PG else ''
            for p in pedidos:
                try:
                    pid = str(p.get('id',''))
                    if not pid: continue
                    itens = json.dumps(p.get('itens', []))
                    sql = (f"INSERT {'OR IGNORE ' if not IS_PG else ''}INTO pedidos_pc "
                           f"(id,numero,data,canal,uf,total,lucro,margem,frete,sem_imposto,sem_custo,itens) "
                           f"VALUES ({qmark(12)}) {ignore if IS_PG else ''}")
                    exe(sql, (
                        pid, str(p.get('numero','')), str(p.get('data',''))[:10],
                        str(p.get('canal','')), str(p.get('uf','')),
                        float(p.get('total',0) or 0),
                        float(p.get('lucro',0)) if p.get('lucro') is not None else None,
                        float(p.get('margem',0)) if p.get('margem') is not None else None,
                        float(p.get('frete',0) or 0),
                        1 if p.get('sem_imposto') else 0,
                        1 if p.get('sem_custo') else 0,
                        itens
                    ))
                    ok += 1
                except Exception as e2:
                    errs += 1
            self._ok({'ok': ok, 'errors': errs})
            print(f'[PC] {ok} pedidos salvos | {errs} erros')
        except Exception as e: self._err(500, str(e))

    def _post_tokens(self):
        try:
            d = self._body()
            if d.get('bling_access'): salvar_tokens_db('bling', d['bling_access'], d.get('bling_refresh'))
            if d.get('ml_access'):    salvar_tokens_db('ml', d['ml_access'], d.get('ml_refresh'))
            if d.get('bling_access') or d.get('ml_access'):
                threading.Thread(target=sync_all, daemon=True).start()
            self._ok({'ok':True})
            print('[AUTH] Tokens atualizados — sync iniciado')
        except Exception as e: self._err(500, str(e))

    def _post_cmv(self):
        try:
            d = self._body()
            n=0
            for sku, v in d.items():
                cmv = float(v.get('cmv', v.get('cmvBr', 0)))
                if cmv<=0: continue
                try: upsert_produto(sku, v.get('nome',sku), custo=cmv, custo_br=cmv, custo_pr=float(v.get('cmvPr',cmv)))
                except: pass
                n+=1
            self._ok({'ok':True,'n':n})
            print(f'[CMV] {n} SKUs recebidos')
        except Exception as e: self._err(500, str(e))

    def _post_produto(self):
        try:
            d = self._body()
            sku = d.get('sku','').strip()
            if not sku: self._err(400,'sku obrigatorio'); return
            upsert_produto(sku, d.get('nome',''), d.get('marca',''), d.get('familia',''),
                float(d.get('custo',0)), float(d.get('custo_br',0)), float(d.get('custo_pr',0)),
                int(d.get('estoque',0)), float(d.get('ipi',0)), float(d.get('cred_icms',0)),
                d.get('fornecedor',''), float(d.get('preco_venda',0)))
            self._ok({'ok':True,'sku':sku})
        except Exception as e: self._err(500, str(e))

    def _post_historico(self):
        try:
            payload = self._body()
            if isinstance(payload, dict): payload = [payload]
            n=0
            ignore = 'ON CONFLICT DO NOTHING' if IS_PG else ''
            for row in payload:
                try:
                    sql = (f"INSERT {'OR IGNORE ' if not IS_PG else ''}INTO historico_compras "
                           f"(nf,fornecedor,data_emissao,sku,nome,qtd,vunit,vtot,ipi_p,ipi_un,icms_p,cred_pc,custo_r,cmv_br,cmv_pr,ncm,cfop) "
                           f"VALUES ({qmark(17)}) {ignore if IS_PG else ''}")
                    exe(sql, (row.get('nf'), row.get('fornecedor'), row.get('data_emissao'),
                         row.get('sku',''), row.get('nome',''),
                         float(row.get('qtd',0)), float(row.get('vunit',0)), float(row.get('vtot',0)),
                         float(row.get('ipi_p',0)), float(row.get('ipi_un',0)),
                         float(row.get('icms_p',0)), float(row.get('cred_pc',0)),
                         float(row.get('custo_r',0)), float(row.get('cmv_br',0)),
                         float(row.get('cmv_pr',0)), row.get('ncm',''), row.get('cfop','')))
                    sku = row.get('sku','').strip()
                    cmv_br = float(row.get('cmv_br',0))
                    if sku and cmv_br>0:
                        prod_atual = exe(f"SELECT custo_br FROM produtos WHERE sku={'%s' if IS_PG else '?'}",
                                        (sku,), fetchone=True)
                        if not prod_atual or not prod_atual.get('custo_br'):
                            upsert_produto(sku, row.get('nome',''), custo=cmv_br, custo_br=cmv_br,
                                           custo_pr=float(row.get('cmv_pr',cmv_br)))
                    n+=1
                except Exception as e: print(f'[HIST] {e}')
            self._ok({'ok':True,'inseridos':n})
        except Exception as e: self._err(500, str(e))

    def _post_nf(self):
        try:
            d = self._body()
            chave = d.get('chave','')
            if not chave: self._err(400,'chave obrigatoria'); return
            ignore = 'ON CONFLICT DO NOTHING' if IS_PG else ''
            exe(f"INSERT {'OR IGNORE ' if not IS_PG else ''}INTO nf_entrada (chave,nf,fornecedor,cnpj,emissao,valor) VALUES ({qmark(6)}) {ignore if IS_PG else ''}",
                (chave, str(d.get('nf','')), d.get('forn',''), d.get('cnpj',''), str(d.get('emissao','')), float(d.get('vNF',0))))
            for p_ in (d.get('parcelas') or []):
                exe(f"INSERT INTO boletos (nf_chave,fornecedor,cnpj,nf,emissao,valor_nf,parcela,vencimento,valor) VALUES ({qmark(9)})",
                    (chave, d.get('forn',''), d.get('cnpj_raw',''), str(d.get('nf','')),
                     str(d.get('emissao','')), float(d.get('vNF',0)),
                     p_.get('num',''), str(p_.get('venc','')), float(p_.get('valor',0))))
            self._ok({'ok':True})
        except Exception as e: self._err(500, str(e))

    def _post_listing(self):
        try:
            d = self._body()
            mlb = d.get('id','')
            if not mlb: self._err(400,'id obrigatorio'); return
            sku       = str(d.get('sku','') or '').strip()
            titulo    = str(d.get('titulo','') or '').strip()
            preco     = float(d.get('preco',0) or 0)
            sale_fee  = float(d.get('sale_fee',0) or 0)
            ltype     = str(d.get('listing_type','') or '')
            free_ship = int(d.get('free_shipping',0) or 0)
            status_it = str(d.get('status','active') or 'active')
            frete     = float(d.get('frete_medio',0) or 0)
            mg_min    = float(d.get('margem_minima',0) or 0)
            desconto  = float(d.get('desconto',0) or 0)
            cmv       = float(d.get('cmv',0) or 0)
            c = ('ON CONFLICT(id) DO UPDATE SET sku=EXCLUDED.sku,titulo=EXCLUDED.titulo,preco=EXCLUDED.preco,'
                 'sale_fee=EXCLUDED.sale_fee,listing_type=EXCLUDED.listing_type,free_shipping=EXCLUDED.free_shipping,'
                 'status=EXCLUDED.status,frete_medio=EXCLUDED.frete_medio,margem_minima=EXCLUDED.margem_minima,'
                 'desconto=EXCLUDED.desconto') if IS_PG else \
                ('ON CONFLICT(id) DO UPDATE SET sku=excluded.sku,titulo=excluded.titulo,preco=excluded.preco,'
                 'sale_fee=excluded.sale_fee,listing_type=excluded.listing_type,free_shipping=excluded.free_shipping,'
                 'status=excluded.status,frete_medio=excluded.frete_medio,margem_minima=excluded.margem_minima,'
                 'desconto=excluded.desconto')
            exe(f"INSERT INTO ml_listings (id,sku,titulo,preco,sale_fee,listing_type,free_shipping,status,frete_medio,margem_minima,desconto) VALUES ({qmark(11)}) {c}",
                (mlb,sku,titulo,preco,sale_fee,ltype,free_ship,status_it,frete,mg_min,desconto))
            if cmv > 0 and sku:
                upsert_produto(sku, titulo, custo=cmv, custo_br=cmv)
            self._ok({'ok':True,'id':mlb})
        except Exception as e: self._err(500, str(e))

    def _post_cprod_map(self):
        try:
            d = self._body()
            n = 0
            for cprod, info in d.items():
                sku    = str(info.get('sku','') or '')
                nome   = str(info.get('nome','') or '')
                cmv_br = float(info.get('cmv_br',0) or 0)
                cmv_pr = float(info.get('cmv_pr',0) or 0)
                c = ('ON CONFLICT(cprod) DO UPDATE SET sku=EXCLUDED.sku,nome=EXCLUDED.nome,'
                     'cmv_br=EXCLUDED.cmv_br,cmv_pr=EXCLUDED.cmv_pr') if IS_PG else \
                    ('ON CONFLICT(cprod) DO UPDATE SET sku=excluded.sku,nome=excluded.nome,'
                     'cmv_br=excluded.cmv_br,cmv_pr=excluded.cmv_pr')
                exe(f"INSERT INTO cprod_map (cprod,sku,nome,cmv_br,cmv_pr) VALUES ({qmark(5)}) {c}",
                    (cprod,sku,nome,cmv_br,cmv_pr))
                if sku and (cmv_br>0 or cmv_pr>0):
                    upsert_produto(sku, nome, custo=cmv_br or cmv_pr, custo_br=cmv_br or cmv_pr, custo_pr=cmv_pr or cmv_br)
                n += 1
            self._ok({'ok':n})
        except Exception as e: self._err(500, str(e))

    def _post_listings_batch(self):
        try:
            d = self._body()
            listings = d.get('listings',[])
            ok, errs = 0, 0
            for item in listings:
                try:
                    mlb   = str(item.get('id','') or '').strip()
                    if not mlb: continue
                    sku   = str(item.get('sku','') or '').strip()
                    titulo= str(item.get('titulo','') or '').strip()
                    preco = float(item.get('preco',0) or 0)
                    sf    = float(item.get('sale_fee',0) or 0)
                    ltype = str(item.get('listing_type','') or '')
                    fs    = int(item.get('free_shipping',0) or 0)
                    st    = str(item.get('status','active') or 'active')
                    frete = float(item.get('frete_medio',0) or 0)
                    mg    = float(item.get('margem_minima',0) or 0)
                    cmv   = float(item.get('cmv',0) or 0)
                    c = ('ON CONFLICT(id) DO UPDATE SET sku=EXCLUDED.sku,titulo=EXCLUDED.titulo,preco=EXCLUDED.preco,'
                         'sale_fee=EXCLUDED.sale_fee,listing_type=EXCLUDED.listing_type,free_shipping=EXCLUDED.free_shipping,'
                         'status=EXCLUDED.status,frete_medio=EXCLUDED.frete_medio,margem_minima=EXCLUDED.margem_minima') if IS_PG else \
                        ('ON CONFLICT(id) DO UPDATE SET sku=excluded.sku,titulo=excluded.titulo,preco=excluded.preco,'
                         'sale_fee=excluded.sale_fee,listing_type=excluded.listing_type,free_shipping=excluded.free_shipping,'
                         'status=excluded.status,frete_medio=excluded.frete_medio,margem_minima=excluded.margem_minima')
                    exe(f"INSERT INTO ml_listings (id,sku,titulo,preco,sale_fee,listing_type,free_shipping,status,frete_medio,margem_minima) VALUES ({qmark(10)}) {c}",
                        (mlb,sku,titulo,preco,sf,ltype,fs,st,frete,mg))
                    if cmv > 0 and sku:
                        upsert_produto(sku, titulo, custo=cmv, custo_br=cmv)
                    ok += 1
                except Exception as e2:
                    errs += 1
            self._ok({'ok':ok,'errors':errs})
        except Exception as e: self._err(500, str(e))

    def _post_kits_mapa(self):
        try:
            d = self._body()
            kits = d.get('kits', [])
            c_pk = ('ON CONFLICT(sku_componente,sku_kit) DO UPDATE SET qtd=EXCLUDED.qtd,fonte=EXCLUDED.fonte') if IS_PG else \
                   ('ON CONFLICT(sku_componente,sku_kit) DO UPDATE SET qtd=excluded.qtd,fonte=excluded.fonte')
            ok = 0
            for kit in kits:
                sku_kit = str(kit.get('sku_kit','') or '').strip()
                fonte   = str(kit.get('fonte','auto'))
                for comp in kit.get('componentes',[]):
                    sku_c = str(comp.get('sku','') or '').strip()
                    qtd   = float(comp.get('qtd', 1) or 1)
                    if sku_c and sku_kit:
                        exe(f"INSERT INTO kits_mapa (sku_componente,sku_kit,qtd,fonte) VALUES ({qmark(4)}) {c_pk}",
                            (sku_c, sku_kit, qtd, fonte))
                        ok += 1
            self._ok({'ok': ok, 'kits': len(kits)})
        except Exception as e: self._err(500, str(e))

    def _post_pedrinho_importar(self):
        body = self._body()
        prods = body.get('produtos', [])
        inseridos = 0
        db = get_db()
        with _db_lock:
            cur = db.cursor()
            for p in prods:
                fotos_json = json.dumps(p.get('fotos',[]))
                try:
                    if IS_PG:
                        cur.execute("""INSERT INTO pedrinho (codigo,descricao,storyselling,fotos,qtd_fotos)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT(codigo) DO UPDATE SET
                            descricao=EXCLUDED.descricao,storyselling=EXCLUDED.storyselling,
                            fotos=EXCLUDED.fotos,qtd_fotos=EXCLUDED.qtd_fotos""",
                            (p['codigo'],p.get('descricao',''),p.get('storyselling',''),fotos_json,p.get('qtd_fotos',0)))
                    else:
                        cur.execute("""INSERT OR REPLACE INTO pedrinho
                            (codigo,descricao,storyselling,fotos,qtd_fotos) VALUES (?,?,?,?,?)""",
                            (p['codigo'],p.get('descricao',''),p.get('storyselling',''),fotos_json,p.get('qtd_fotos',0)))
                    inseridos += 1
                except Exception as e:
                    print(f'[Pedrinho] {e}')
            if not IS_PG: db.commit()
        self._ok({'inseridos': inseridos, 'total': len(prods)})

    def _post_kit(self):
        b = self._body()
        try:
            itens = json.dumps(b.get('itens',[]))
            taref = json.dumps(b.get('tarefas_status', b.get('tarefas',[])))
            if IS_PG:
                exe("""INSERT INTO kits (sku,nome,itens,justificativa,peso,titulo_ml,
                    descricao,descricao_completa,categoria,tarefas,status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(sku) DO UPDATE SET nome=EXCLUDED.nome,
                    tarefas=EXCLUDED.tarefas,status=EXCLUDED.status""",
                    (b.get('sku'),b.get('nome'),itens,b.get('justificativa'),
                     b.get('peso',0),b.get('titulo'),b.get('descricao'),
                     b.get('descricao_completa'),b.get('categoria'),taref,b.get('status','aprovado')))
            else:
                exe("""INSERT OR REPLACE INTO kits
                    (sku,nome,itens,justificativa,peso,titulo_ml,descricao,descricao_completa,categoria,tarefas,status)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (b.get('sku'),b.get('nome'),itens,b.get('justificativa'),
                     b.get('peso',0),b.get('titulo'),b.get('descricao'),
                     b.get('descricao_completa'),b.get('categoria'),taref,b.get('status','aprovado')))
            self._ok({'ok': True})
        except Exception as e:
            self._err(500, str(e))

    def _post_cadastro(self):
        b = self._body()
        fotos = json.dumps(b.get('fotos',[]))
        taref = json.dumps(b.get('tarefas',[]))
        try:
            campos = ['sku','nome','fornecedor','codigo_fornecedor','ncm','cst','cfop',
                'ipi','tem_st','custo','custo_br','custo_pr','peso','comprimento','largura',
                'altura','ean','categoria','familia','fotos','titulo_ml','titulo_shopee',
                'titulo_yampi','titulo_facebook','descricao_ml','preco_ml_classico',
                'preco_ml_premium','preco_shopee','preco_yampi','preco_balcao',
                'preco_atacado','status_cadastro','tarefas']
            vals = [b.get('sku'),b.get('nome'),b.get('fornecedor'),b.get('codigo_fornecedor'),
                b.get('ncm'),b.get('cst'),b.get('cfop'),b.get('ipi',0),
                1 if b.get('tem_st') else 0,
                b.get('custo',0),b.get('custo_br',0),b.get('custo_pr',0),
                b.get('peso',0),b.get('comprimento',0),b.get('largura',0),b.get('altura',0),
                b.get('ean'),b.get('categoria'),b.get('familia'),fotos,
                b.get('titulo_ml'),b.get('titulo_shopee'),b.get('titulo_yampi'),b.get('titulo_facebook'),
                b.get('descricao_ml'),b.get('preco_ml_classico',0),b.get('preco_ml_premium',0),
                b.get('preco_shopee',0),b.get('preco_yampi',0),b.get('preco_balcao',0),
                b.get('preco_atacado',0),b.get('status_cadastro','rascunho'),taref]
            ph = ['%s' if IS_PG else '?'] * len(campos)
            sql = f"INSERT INTO produto_cadastro ({','.join(campos)}) VALUES ({','.join(ph)})"
            if IS_PG:
                sql += " ON CONFLICT(sku) DO UPDATE SET " + ",".join(f"{c}=EXCLUDED.{c}" for c in campos if c!='sku')
            else:
                sql = sql.replace('INSERT INTO','INSERT OR REPLACE INTO')
            exe(sql, vals)
            self._ok({'ok': True, 'sku': b.get('sku')})
        except Exception as e:
            self._err(500, str(e))

    # ────────────────────────────────────────────────────────────
    # PROXY
    # ────────────────────────────────────────────────────────────
    def _proxy(self, method):
        url = None
        for prefix, base in PROXY.items():
            if self.path.startswith(prefix):
                url = base + self.path[len(prefix):]; break
        if not url: self.send_error(404); return
        headers = {h: self.headers.get(h) for h in ['Content-Type','Accept'] if self.headers.get(h)}
        if 'Accept' not in headers: headers['Accept'] = 'application/json'
        # Injetar token correto automaticamente
        if self.path.startswith('/api/bling/'):
            tk = _bling_token.get('access','')
            if tk: headers['Authorization'] = f'Bearer {tk}'
        elif self.path.startswith('/api/ml/') or self.path.startswith('/api/mp/'):
            tk = _ml_token.get('access','')
            if tk: headers['Authorization'] = f'Bearer {tk}'
        body = None
        if method in ('POST','PUT'):
            n = int(self.headers.get('Content-Length',0))
            if n: body = self.rfile.read(n)
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
                self.send_response(r.status); self._cors()
                self.send_header('Content-Type','application/json'); self.end_headers()
                self.wfile.write(data)
        except urllib.error.HTTPError as e:
            self.send_response(e.code); self._cors()
            self.send_header('Content-Type','application/json'); self.end_headers()
            self.wfile.write(e.read())
        except Exception as e:
            self._err(500, str(e))

    def _cors(self):
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Authorization,Content-Type,Accept')

    def _ok(self, data):
        body = json.dumps(data, default=str).encode('utf-8')
        self.send_response(200); self._cors()
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body))); self.end_headers()
        self.wfile.write(body)

    def _err(self, code, msg):
        body = json.dumps({'error':msg}).encode()
        self.send_response(code); self._cors()
        self.send_header('Content-Type','application/json'); self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print(f'Fava Ecom v3 — porta {PORTA} | BD: {"PostgreSQL" if DATABASE_URL else "SQLite"}')
    criar_tabelas()
    carregar_tokens_salvos()
    agendar_sync()
    http.server.HTTPServer(('0.0.0.0', PORTA), Handler).serve_forever()
