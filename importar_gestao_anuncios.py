"""
FAVA ECOM — Script 2: Importar anúncios ML da GESTAO_FAVA_ECOM
===============================================================
Fonte: GESTAO_FAVA_ECOM.xlsx → aba GESTAO ML
Destino: POST /api/db/listings-batch

Colunas mapeadas:
  MLB        → id
  SKU        → sku
  PRODUTO    → titulo
  PREÇO ATUAL → preco
  TAXA       → sale_fee (percentual, ex: 0.12)
  FRETE      → frete_medio (valor R$)
  TIPO       → listing_type
  MARGEM %   → margem_minima (margem real atual)
"""

import pandas as pd
import requests
import math

# ── CONFIGURAÇÃO ────────────────────────────────────────────────────────────
ARQUIVO   = r'C:\FAVAECOM\GESTAO_FAVA_ECOM.xlsx'
BASE_URL  = 'https://web-production-5aa0f.up.railway.app'
LOTE      = 100  # listings por request
# ────────────────────────────────────────────────────────────────────────────

def limpar(val, default=0.0):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    try:
        return float(val)
    except:
        return default

def tipo_ml(tipo_str):
    """Normaliza tipo para o padrão ML."""
    t = str(tipo_str or '').lower()
    if 'premium' in t or 'gold_pro' in t:
        return 'gold_pro'
    if 'classico' in t or 'clássico' in t or 'classic' in t:
        return 'gold_special'
    return t.strip() or 'gold_special'

def post(endpoint, payload):
    url = BASE_URL.rstrip('/') + endpoint
    try:
        r = requests.post(url, json=payload, timeout=60)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f'  [ERRO] {endpoint}: {e}')
        return None

def main():
    print(f'Lendo GESTAO ML de:\n  {ARQUIVO}\n')
    
    # Header real está na linha 3 (índice 3)
    df = pd.read_excel(ARQUIVO, sheet_name='GESTAO ML', header=3)
    
    # Filtra só linhas com MLB válido
    df = df[df['MLB'].notna()].copy()
    df['MLB'] = df['MLB'].astype(str).str.strip()
    df = df[df['MLB'].str.startswith('MLB')]
    
    total = len(df)
    print(f'Anúncios encontrados: {total}')
    
    # Monta lista de listings
    listings = []
    sem_sku = 0
    
    for _, row in df.iterrows():
        mlb   = row['MLB']
        sku   = str(row.get('SKU', '') or '').strip()
        titulo = str(row.get('PRODUTO', '') or '').strip()
        preco  = limpar(row.get('PREÇO ATUAL'))
        taxa   = limpar(row.get('TAXA'))         # ex: 0.12
        frete  = limpar(row.get('FRETE'))        # R$ frete médio
        tipo   = tipo_ml(row.get('TIPO'))
        margem = limpar(row.get('MARGEM %'))     # ex: 0.40
        
        if not sku:
            sem_sku += 1
        
        free_ship = 1 if tipo in ('gold_pro', 'gold_premium') else 0
        
        listings.append({
            'id':            mlb,
            'sku':           sku,
            'titulo':        titulo,
            'preco':         preco,
            'sale_fee':      taxa,
            'listing_type':  tipo,
            'free_shipping': free_ship,
            'status':        'active',
            'frete_medio':   frete,
            'margem_minima': margem,
            'cmv':           0,  # CMV vem da tabela produtos via JOIN no servidor
        })
    
    print(f'  Anúncios sem SKU: {sem_sku} (serão salvos, CMV cruzado depois)')
    print(f'\nEnviando para /api/db/listings-batch em lotes de {LOTE}...')
    
    ok_total = 0
    err_total = 0
    
    for i in range(0, len(listings), LOTE):
        lote = listings[i:i+LOTE]
        res = post('/api/db/listings-batch', {'listings': lote})
        if res:
            ok_total  += res.get('ok', 0)
            err_total += res.get('errors', 0)
            print(f'  Lote {i//LOTE + 1}: {len(lote)} → OK={res.get("ok",0)} ERR={res.get("errors",0)}')
        else:
            err_total += len(lote)
            print(f'  Lote {i//LOTE + 1}: FALHOU')
    
    print(f'\nResultado: {ok_total} inseridos | {err_total} erros')
    print('✅ Script 2 concluído.')

if __name__ == '__main__':
    main()
