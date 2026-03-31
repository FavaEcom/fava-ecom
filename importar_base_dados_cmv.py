"""
FAVA ECOM — Script 1: Importar CMV + mapa cProd da BASE_DADOS_V2
"""

import pandas as pd
import requests
import math

ARQUIVO  = r'C:\FAVAECOM\scripts\PROJETO FAVA ECOM V3.1 - ultiima.xlsm'
BASE_URL = 'https://web-production-5aa0f.up.railway.app'
LOTE     = 200

def limpar(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return 0.0
    try:
        return float(val)
    except:
        return 0.0

def post(endpoint, payload):
    url = BASE_URL.rstrip('/') + endpoint
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f'  [ERRO] {endpoint}: {e}')
        return None

def main():
    print(f'Lendo BASE_DADOS_V2 de:\n  {ARQUIVO}\n')
    df = pd.read_excel(ARQUIVO, sheet_name='BASE_DADOS_V2', header=2)
    df = df[df['SKU'].notna() & (df['SKU'] != '')].copy()
    df['SKU']    = df['SKU'].astype(str).str.strip()
    df['CÓDIGO'] = df['CÓDIGO'].astype(str).str.strip()
    print(f'Registros encontrados: {len(df)}')

    # CMV CACHE
    print('\n[1/2] Enviando CMV para /api/cmv-cache ...')
    cmv_payload = {}
    sem_cmv = 0
    for _, row in df.iterrows():
        sku    = row['SKU']
        cmv_br = limpar(row.get('CMV BRASIL'))
        cmv_pr = limpar(row.get('CMV PARANÁ'))
        nome   = str(row.get('PRODUTO', '') or '').strip()
        if cmv_br <= 0 and cmv_pr <= 0:
            sem_cmv += 1
            continue
        cmv_payload[sku] = {'cmv': cmv_br or cmv_pr, 'cmvBr': cmv_br, 'cmvPr': cmv_pr, 'nome': nome}

    skus = list(cmv_payload.keys())
    ok_cmv = 0
    for i in range(0, len(skus), LOTE):
        lote = {k: cmv_payload[k] for k in skus[i:i+LOTE]}
        res = post('/api/cmv-cache', lote)
        if res:
            ok_cmv += res.get('n', len(lote))
            print(f'  Lote {i//LOTE+1}: {len(lote)} SKUs → OK')
        else:
            print(f'  Lote {i//LOTE+1}: FALHOU')
    print(f'  Total: {ok_cmv} SKUs com CMV | {sem_cmv} sem CMV (ignorados)')

    # CPROD MAP
    print('\n[2/2] Enviando mapa cProd para /api/db/cprod-map ...')
    cprod_payload = {}
    sem_codigo = 0
    for _, row in df.iterrows():
        sku   = row['SKU']
        cprod = row['CÓDIGO']
        nome  = str(row.get('PRODUTO', '') or '').strip()
        cmv_br = limpar(row.get('CMV BRASIL'))
        cmv_pr = limpar(row.get('CMV PARANÁ'))
        if not cprod or cprod in ('nan', '0', 'None'):
            sem_codigo += 1
            continue
        cprod_payload[cprod] = {'sku': sku, 'nome': nome, 'cmv_br': cmv_br, 'cmv_pr': cmv_pr}

    cprods = list(cprod_payload.keys())
    ok_cprod = 0
    for i in range(0, len(cprods), LOTE):
        lote = {k: cprod_payload[k] for k in cprods[i:i+LOTE]}
        res = post('/api/db/cprod-map', lote)
        if res:
            ok_cprod += res.get('ok', len(lote))
            print(f'  Lote {i//LOTE+1}: {len(lote)} cProds → OK')
        else:
            print(f'  Lote {i//LOTE+1}: FALHOU')
    print(f'  Total: {ok_cprod} cProds mapeados | {sem_codigo} sem código (ignorados)')
    print('\n✅ Script 1 concluído.')

if __name__ == '__main__':
    main()
