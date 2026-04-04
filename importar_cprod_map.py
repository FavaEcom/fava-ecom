"""
FAVA ECOM — Importar mapeamento Código → SKU para o banco
==========================================================
Lê a planilha principal (BASE_DADOS_V2) e importa o mapeamento
  CÓDIGO (col D) → SKU (col C)

Uso:
  cd C:\FAVAECOM\scripts
  python importar_cprod_map.py "PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm"
"""
import sys, json, urllib.request, openpyxl

SERVER = 'https://web-production-5aa0f.up.railway.app'
ARQUIVO = sys.argv[1] if len(sys.argv) > 1 else None

if not ARQUIVO:
    print("Uso: python importar_cprod_map.py \"PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm\"")
    input("\nPressione Enter para fechar...")
    sys.exit(1)

print("=" * 60)
print(f"  Lendo {ARQUIVO}...")

wb = openpyxl.load_workbook(ARQUIVO, read_only=True, data_only=True)
print(f"  Abas disponíveis: {wb.sheetnames}")

# Tentar ler BASE_DADOS_V2
aba = 'BASE_DADOS_V2' if 'BASE_DADOS_V2' in wb.sheetnames else wb.sheetnames[0]
ws = wb[aba]
print(f"  Lendo aba: {aba}")

# Detectar colunas automaticamente pela linha de cabeçalho (linha 3)
headers = {}
for row in ws.iter_rows(min_row=1, max_row=5, values_only=True):
    for i, v in enumerate(row):
        v = str(v or '').strip().upper()
        if v in ('SKU', 'SKU FAVA', 'SKU INTERNO'): headers['sku'] = i
        if v in ('CÓDIGO', 'CODIGO', 'CÓD', 'COD', 'CÓDIGO BLING', 'CODIGO BLING'): headers['cod'] = i
        if v in ('PRODUTO', 'NOME', 'DESCRIÇÃO', 'DESCRICAO'): headers['nome'] = i
    if 'sku' in headers and 'cod' in headers:
        break

# Fallback: usar posições padrão da planilha Fava (col C=SKU, col D=CÓDIGO)
if 'sku' not in headers: headers['sku'] = 2   # coluna C (índice 2)
if 'cod' not in headers: headers['cod'] = 3   # coluna D (índice 3)
if 'nome' not in headers: headers['nome'] = 4 # coluna E (índice 4)

print(f"  Colunas: SKU=col {headers['sku']+1}, CÓDIGO=col {headers['cod']+1}, NOME=col {headers['nome']+1}")

mapeamentos = []
for row in ws.iter_rows(min_row=4, values_only=True):
    try:
        sku  = row[headers['sku']]
        cod  = row[headers['cod']]
        nome = row[headers.get('nome', 4)] if len(row) > headers.get('nome', 4) else ''
    except IndexError:
        continue

    if not sku or not cod: continue
    try:
        sku_int = int(float(str(sku)))
    except:
        continue
    cod_str = str(cod).strip()
    if not cod_str or cod_str in ('None', '0'): continue

    mapeamentos.append({
        'cprod': cod_str,
        'sku': str(sku_int),
        'nome': str(nome or '').strip()[:200]
    })

print(f"\n  {len(mapeamentos)} mapeamentos encontrados")
if not mapeamentos:
    print("  ERRO: Nenhum mapeamento encontrado.")
    input("\nPressione Enter para fechar...")
    sys.exit(1)

print(f"\n  Amostra (primeiros 5):")
for m in mapeamentos[:5]:
    print(f"    {m['cprod']} → SKU {m['sku']}  |  {m['nome'][:40]}")

confirma = input(f"\n  Importar {len(mapeamentos)} mapeamentos? (s/n): ")
if confirma.lower() not in ('s', 'sim', 'y', 'yes'):
    print("  Cancelado.")
    input("\nPressione Enter para fechar...")
    sys.exit(0)

# Enviar em lotes
BATCH = 200
ok = 0
for i in range(0, len(mapeamentos), BATCH):
    lote = mapeamentos[i:i+BATCH]
    payload = json.dumps({'mapeamentos': lote}).encode()
    req = urllib.request.Request(
        SERVER + '/api/db/cprod-map-import',
        data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            res = json.loads(r.read())
            salvos = res.get('saved', len(lote) if res.get('ok') else 0)
            ok += salvos
            print(f"  Lote {i//BATCH+1}: {salvos} salvos")
    except Exception as e:
        print(f"  ERRO lote {i//BATCH+1}: {e}")

print(f"\n  ✅ Concluído: {ok} mapeamentos salvos no banco")
print("=" * 60)
input("\nPressione Enter para fechar...")
