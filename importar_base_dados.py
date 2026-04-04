"""
FAVA ECOM - Importar BASE_DADOS_V2 para o banco
================================================
Atualiza: CMV BR, CMV PR, ST, ST_IMPOSTO, NCM, CEST,
          IPI, MONOFASICO, ORIGEM, CST, BASE_ICMS, ALIQ_ICMS

Uso:
  cd C:\FAVAECOM\scripts
  python importar_base_dados.py "PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm"
"""

import sys, json, openpyxl, urllib.request, urllib.error

SERVER = 'https://web-production-5aa0f.up.railway.app'
ARQUIVO = sys.argv[1] if len(sys.argv) > 1 else 'PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm'

def sf(v):
    try: return float(v)
    except: return None

def post(endpoint, dados):
    payload = json.dumps(dados).encode()
    req = urllib.request.Request(
        SERVER + endpoint, data=payload,
        headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {'ok': False, 'error': f'HTTP {e.code}: {e.read().decode()[:100]}'}
    except Exception as e:
        return {'ok': False, 'error': str(e)}

print('=' * 55)
print(f'  Lendo {ARQUIVO}...')
print('=' * 55)

wb = openpyxl.load_workbook(ARQUIVO, read_only=True, data_only=True)
ws = wb['BASE_DADOS_V2']

produtos = []
for row in ws.iter_rows(min_row=4, values_only=True):
    sku = row[2]
    if not sku: continue
    try: sku = int(float(str(sku)))
    except: continue

    st_val = str(row[45]).strip() if row[45] else 'Não'
    cmv_br = sf(row[48])
    if not cmv_br: continue  # só atualiza quem tem CMV

    produtos.append({
        'sku': sku,
        'st': 1 if st_val == 'Sim' else 0,
        'st_imposto': sf(row[46]) or 0.0,
        'cmv_br': cmv_br,
        'cmv_pr': sf(row[49]) or cmv_br,
        'ncm': str(row[32]).strip() if row[32] else None,
        'cest': str(row[33]).strip() if row[33] else None,
        'origem': str(row[27]).strip() if row[27] else None,
        'cst': str(row[28]).strip() if row[28] else None,
        'ipi': sf(row[39]) or 0.0,
        'monofasico': 1 if row[44] else 0,
        'base_icms': sf(row[35]) or 0.0,
        'aliq_icms': sf(row[37]) or 0.0,
        'aliq_eff': sf(row[38]) or 0.0,
    })

print(f'  {len(produtos)} produtos com CMV encontrados')
print(f'  Com ST: {sum(1 for p in produtos if p["st"])}')
print(f'  Sem ST: {sum(1 for p in produtos if not p["st"])}')
print(f'\n  Enviando para o servidor...')

BATCH = 200
ok_total = 0
for i in range(0, len(produtos), BATCH):
    lote = produtos[i:i+BATCH]
    res = post('/api/db/produtos/update-fiscal', {'produtos': lote})
    salvos = res.get('ok') or res.get('updated') or res.get('saved') or 0
    if isinstance(salvos, bool): salvos = len(lote) if salvos else 0
    ok_total += salvos
    print(f'  Lote {i//BATCH+1}: {salvos} atualizados | res={res}')

print(f'\n  Concluido: {ok_total} produtos atualizados no banco')
print('=' * 55)
input('\nPressione Enter para fechar...')
