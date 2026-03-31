"""
FAVA ECOM — Script 2: Importar anúncios ML da GESTAO_FAVA_ECOM
"""

import pandas as pd
import requests
import math

ARQUIVO  = r'C:\FAVAECOM\scripts\GESTAO_FAVA_ECOM.xlsx'
BASE_URL = 'https://web-production-5aa0f.up.railway.app'
LOTE     = 100

def limpar(val, default=0.0):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    try:
        return float(val)
    except:
        return default

def tipo_ml(tipo_str):
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
    df = pd.read_excel(ARQUIVO, sheet_name='GESTAO ML', header=3)
    df = df[df['MLB'].notna()].copy()
    df['MLB'] = df['MLB'].astype(str).str.strip()
    df = df[df['MLB'].str.startswith('MLB')]
    print(f'Anúncios encontrados: {len(df)}')

    listings = []
    sem_sku = 0
    for _, row in df.iterrows():
        mlb    = row['MLB']
        sku    = str(row.get('SKU', '') or '').strip()
        titulo = str(row.get('PRODUTO', '') or '').strip()
        preco  = limpar(row.get('PREÇO ATUAL'))
        taxa   = limpar(row.get('TAXA'))
        frete  = limpar(row.get('FRETE'))
        tipo   = tipo_ml(row.get('TIPO'))
        margem = limpar(row.get('MARGEM %'))
        if not sku:
            sem_sku += 1
        free_ship = 1 if tipo in ('gold_pro', 'gold_premium') else 0
        listings.append({
            'id': mlb, 'sku': sku, 'titulo': titulo, 'preco': preco,
            'sale_fee': taxa, 'listing_type': tipo, 'free_shipping': free_ship,
            'status': 'active', 'frete_medio': frete, 'margem_minima': margem, 'cmv': 0,
        })

    print(f'  Sem SKU: {sem_sku}')
    print(f'Enviando em lotes de {LOTE}...')

    ok_total = err_total = 0
    for i in range(0, len(listings), LOTE):
        lote = listings[i:i+LOTE]
        res = post('/api/db/listings-batch', {'listings': lote})
        if res:
            ok_total  += res.get('ok', 0)
            err_total += res.get('errors', 0)
            print(f'  Lote {i//LOTE+1}: {len(lote)} → OK={res.get("ok",0)} ERR={res.get("errors",0)}')
        else:
            err_total += len(lote)
            print(f'  Lote {i//LOTE+1}: FALHOU')

    print(f'\nResultado: {ok_total} inseridos | {err_total} erros')
    print('✅ Script 2 concluído.')

if __name__ == '__main__':
    main()
