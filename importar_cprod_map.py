"""
FAVA ECOM — Importar BASE_DADOS_V2 completo
============================================
Importa da planilha:
  1. cprod_map: CÓDIGO (col D) → SKU (col C) — para lookup de NFs
  2. produtos: CMV, ST, IPI, Mono, Peso, Largura, Altura, Profundidade, Família

Uso:
  cd C:\FAVAECOM\scripts
  python importar_cprod_map.py "PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm"
"""
import sys, json, urllib.request, openpyxl

SERVER = 'https://web-production-5aa0f.up.railway.app'
ARQUIVO = sys.argv[1] if len(sys.argv) > 1 else None

if not ARQUIVO:
    print('Uso: python importar_cprod_map.py "PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm"')
    input('\nPressione Enter para fechar...'); sys.exit(1)

print('=' * 60)
print(f'  Lendo {ARQUIVO}...')

wb = openpyxl.load_workbook(ARQUIVO, read_only=True, data_only=True)
ws = wb['BASE_DADOS_V2']

def sf(v):
    try: return float(v)
    except: return None

mapeamentos = []  # cprod_map
produtos = []     # atualizar produtos

for row in ws.iter_rows(min_row=4, values_only=True):
    # Colunas (0-based): SKU=2, CÓDIGO=3, PRODUTO=4, FAMÍLIA=6
    # ST=45, ST_IMP=46, CMV_BR=48, CMV_PR=49
    # PESO=51, LARGURA=52, ALTURA=53, PROF=54
    # IPI=39, MONOFASICO=44
    try:
        sku  = row[2];  cod  = row[3];  nome = row[4]
        fam  = row[6];  ipi  = row[39]; mono = row[44]
        st   = row[45]; si   = row[46]
        cmvbr= row[48]; cmvpr= row[49]
        peso = row[51]; larg = row[52]; alt  = row[53]; prof = row[54]
    except IndexError:
        continue

    if not sku: continue
    try: sku_int = int(float(str(sku)))
    except: continue

    cod_str = str(cod).strip() if cod else ''

    # cprod_map: só se tiver código do fornecedor
    if cod_str and cod_str not in ('None','0','nan'):
        mapeamentos.append({
            'cprod': cod_str,
            'sku': str(sku_int),
            'nome': str(nome or '').strip()[:200]
        })

    # produtos: atualizar campos físicos e fiscais
    if sf(cmvbr):
        produtos.append({
            'sku': str(sku_int),
            'familia': str(fam or '').strip()[:100],
            'cmv_br': sf(cmvbr), 'cmv_pr': sf(cmvpr) or sf(cmvbr),
            'custo_br': sf(cmvbr), 'custo_pr': sf(cmvpr) or sf(cmvbr),
            'st': 1 if str(st).strip()=='Sim' else 0,
            'st_imposto': sf(si) or 0.0,
            'ipi': sf(ipi) or 0.0,
            'monofasico': 1 if mono else 0,
            'peso': sf(peso) or 0.0,
            'largura': sf(larg) or 0.0,
            'altura': sf(alt) or 0.0,
            'profundidade': sf(prof) or 0.0,
        })

print(f'  {len(mapeamentos)} códigos para cprod_map')
print(f'  {len(produtos)} produtos para atualizar')

if mapeamentos:
    print(f'\n  Amostra cprod_map:')
    for m in mapeamentos[:3]:
        print(f'    {m["cprod"]} → SKU {m["sku"]} | {m["nome"][:40]}')

if produtos:
    print(f'\n  Amostra produtos:')
    for p in produtos[:3]:
        print(f'    SKU {p["sku"]} | CMV {p["cmv_br"]:.2f} | Peso {p["peso"]}kg | ST:{p["st"]} | Família:{p["familia"]}')

if not mapeamentos and not produtos:
    print('\n  ERRO: Nenhum dado encontrado.')
    input('\nPressione Enter...'); sys.exit(1)

confirma = input(f'\n  Importar? (s/n): ')
if confirma.lower() not in ('s','sim','y','yes'):
    print('  Cancelado.'); input('\nPressione Enter...'); sys.exit(0)

def post(endpoint, dados):
    payload = json.dumps(dados).encode()
    req = urllib.request.Request(
        SERVER + endpoint, data=payload,
        headers={'Content-Type':'application/json'}, method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        return {'ok': False, 'error': str(e)}

# ── 1. Importar cprod_map ─────────────────────────────────────────
if mapeamentos:
    print('\n  Importando cprod_map...')
    ok = 0
    BATCH = 200
    for i in range(0, len(mapeamentos), BATCH):
        lote = mapeamentos[i:i+BATCH]
        res = post('/api/db/cprod-map-import', {'mapeamentos': lote})
        salvos = res.get('saved', len(lote) if res.get('ok') else 0)
        ok += salvos
        print(f'    Lote {i//BATCH+1}: {salvos} salvos')
    print(f'  ✅ cprod_map: {ok} mapeamentos')

# ── 2. Atualizar produtos ─────────────────────────────────────────
if produtos:
    print('\n  Atualizando produtos...')
    ok2 = 0
    for i in range(0, len(produtos), 200):
        lote = produtos[i:i+200]
        res = post('/api/db/produtos/update-fiscal', {'produtos': lote})
        salvos = res.get('updated', res.get('saved', len(lote) if res.get('ok') else 0))
        if isinstance(salvos, bool): salvos = len(lote) if salvos else 0
        ok2 += salvos
        print(f'    Lote {i//200+1}: {salvos} atualizados')
    print(f'  ✅ produtos: {ok2} atualizados')

print('\n' + '=' * 60)
print('  CONCLUÍDO! Dados importados com sucesso.')
print('  Agora o produto_novo vai identificar os produtos automaticamente.')
print('=' * 60)
input('\nPressione Enter para fechar...')
