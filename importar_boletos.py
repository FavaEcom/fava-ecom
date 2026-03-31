"""
FAVA ECOM — Script 3: Importar boletos da FAVA_ESTOQUE_V5
==========================================================
Fonte: FAVA_ESTOQUE_V5.xlsx → aba BOLETOS
Destino: POST /api/db/nf

Lógica: agrupa parcelas por CHAVE NF-e → envia cada NF com suas parcelas.
Colunas:
  FORNECEDOR, CNPJ, Nº NF, CHAVE NF-e, EMISSÃO, VALOR NF,
  PARCELA, VENCIMENTO, VALOR PARCELA, STATUS
"""

import pandas as pd
import requests
import math
from collections import defaultdict

# ── CONFIGURAÇÃO ────────────────────────────────────────────────────────────
ARQUIVO   = r'C:\FAVAECOM\FAVA_ESTOQUE_V5.xlsx'
BASE_URL  = 'https://web-production-5aa0f.up.railway.app'
# ────────────────────────────────────────────────────────────────────────────

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
        s = str(val).strip()
        return s[:10] if len(s) >= 10 else s
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
    
    # Header real está na linha 0 mas como segunda linha do arquivo
    # (primeira linha é título "💰 CONTROLE DE PARCELAS...")
    df = pd.read_excel(ARQUIVO, sheet_name='BOLETOS', header=0)
    
    # A linha 0 real dos dados é a linha 0 do df que tem os nomes reais
    # Verifica se precisa pular linha de título
    if 'FORNECEDOR' not in df.columns:
        # Usa linha 0 como cabeçalho
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)
    
    # Filtra linhas com CHAVE NF-e válida
    df = df[df['CHAVE NF-e'].notna()].copy()
    df = df[df['CHAVE NF-e'].astype(str).str.len() > 10]
    
    total_parcelas = len(df)
    print(f'Parcelas encontradas: {total_parcelas}')
    
    # Agrupa por chave NF
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
        
        # Salva meta da NF (só na primeira parcela)
        if not nfs[chave]['meta']:
            nfs[chave]['meta'] = {
                'chave':   chave,
                'forn':    forn,
                'cnpj':    cnpj,
                'nf':      nf_num,
                'emissao': emissao,
                'vNF':     valor_nf,
            }
        
        nfs[chave]['parcelas'].append({
            'num':   parcela,
            'venc':  vencto,
            'valor': valor_p,
        })
    
    total_nfs = len(nfs)
    print(f'NFs únicas: {total_nfs}')
    print(f'\nEnviando para /api/db/nf...')
    
    ok = 0
    erros = 0
    
    for chave, dados in nfs.items():
        payload = {**dados['meta'], 'parcelas': dados['parcelas']}
        res = post('/api/db/nf', payload)
        if res and res.get('ok'):
            ok += 1
        else:
            erros += 1
            print(f'  [ERRO] NF {dados["meta"].get("nf","")} — {chave[:20]}...')
    
    print(f'\nResultado: {ok} NFs importadas | {erros} erros')
    print(f'Total de parcelas importadas: {total_parcelas}')
    print('✅ Script 3 concluído.')

if __name__ == '__main__':
    main()
