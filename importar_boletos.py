"""
FAVA ECOM — Script 3: Importar boletos da FAVA_ESTOQUE_V5
"""

import pandas as pd
import requests
import math
from collections import defaultdict

ARQUIVO  = r'C:\FAVAECOM\scripts\FAVA_ESTOQUE_V5.xlsx'
BASE_URL = 'https://web-production-5aa0f.up.railway.app'

def limpar_float(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return 0.0
    try:
        return float(val)
    except:
        return 0.0

def limpar_str(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ''
    return str(val).strip()

def formatar_data(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ''
    try:
        if hasattr(val, 'strftime'):
            return val.strftime('%Y-%m-%d')
        return str(val).strip()[:10]
    except:
        return str(val).strip()

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
    print(f'Lendo BOLETOS de:\n  {ARQUIVO}\n')
    df = pd.read_excel(ARQUIVO, sheet_name='BOLETOS', header=0)

    if 'FORNECEDOR' not in df.columns:
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

    df = df[df['CHAVE NF-e'].notna()].copy()
    df = df[df['CHAVE NF-e'].astype(str).str.len() > 10]
    print(f'Parcelas encontradas: {len(df)}')

    nfs = defaultdict(lambda: {'meta': {}, 'parcelas': []})
    for _, row in df.iterrows():
        chave    = limpar_str(row['CHAVE NF-e'])
        forn     = limpar_str(row.get('FORNECEDOR', ''))
        cnpj     = limpar_str(row.get('CNPJ', ''))
        nf_num   = limpar_str(row.get('Nº NF', ''))
        emissao  = formatar_data(row.get('EMISSÃO', ''))
        valor_nf = limpar_float(row.get('VALOR NF', 0))
        parcela  = limpar_str(row.get('PARCELA', ''))
        vencto   = formatar_data(row.get('VENCIMENTO', ''))
        valor_p  = limpar_float(row.get('VALOR PARCELA', 0))

        if not nfs[chave]['meta']:
            nfs[chave]['meta'] = {
                'chave': chave, 'forn': forn, 'cnpj': cnpj,
                'nf': nf_num, 'emissao': emissao, 'vNF': valor_nf,
            }
        nfs[chave]['parcelas'].append({'num': parcela, 'venc': vencto, 'valor': valor_p})

    print(f'NFs únicas: {len(nfs)}')
    print('Enviando para /api/db/nf...')

    ok = erros = 0
    for chave, dados in nfs.items():
        payload = {**dados['meta'], 'parcelas': dados['parcelas']}
        res = post('/api/db/nf', payload)
        if res and res.get('ok'):
            ok += 1
        else:
            erros += 1

    print(f'\nResultado: {ok} NFs importadas | {erros} erros')
    print('✅ Script 3 concluído.')

if __name__ == '__main__':
    main()
