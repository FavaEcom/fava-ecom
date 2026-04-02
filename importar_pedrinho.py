"""
importar_pedrinho.py — Importa planilha Pedrinho para Railway
Uso: python importar_pedrinho.py PERGUNTE_AO_PEDRINHO_-_V1__2_.xlsm
"""
import sys, json, time
import openpyxl
import urllib.request, urllib.error

RAILWAY = 'https://web-production-5aa0f.up.railway.app'

def post(path, data):
    body = json.dumps(data).encode()
    req  = urllib.request.Request(
        RAILWAY + path,
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {'error': e.code, 'msg': e.read().decode()}

def importar(arquivo):
    print(f"\n📂 Lendo {arquivo}...")
    wb = openpyxl.load_workbook(arquivo, read_only=True, data_only=True)

    # --- DADOS (storyselling) ---
    ws_dados  = wb['DADOS']
    ws_fotos  = wb['LINK FOTOS']

    # Monta dicionário de fotos por código
    fotos = {}
    for row in ws_fotos.iter_rows(min_row=2, values_only=True):
        cod, link = row[0], row[1]
        if cod and link:
            # Remove sufixo (1), (2) etc para ter o código base
            cod_base = str(cod).split('(')[0].strip()
            if cod_base not in fotos:
                fotos[cod_base] = []
            fotos[cod_base].append(str(link))

    # Monta lista de produtos
    produtos = []
    for row in ws_dados.iter_rows(min_row=2, values_only=True):
        cod, desc, story = row[0], row[1], row[2]
        if not cod or not desc:
            continue
        cod_str = str(cod).strip()
        produtos.append({
            'codigo':      cod_str,
            'descricao':   str(desc).strip(),
            'storyselling': str(story).strip() if story else '',
            'fotos':       fotos.get(cod_str, []),
            'qtd_fotos':   len(fotos.get(cod_str, [])),
        })

    print(f"✅ {len(produtos)} produtos lidos | {len(fotos)} com fotos")
    print(f"📤 Enviando para Railway...")

    # Envia em lotes de 100
    ok, erros = 0, 0
    lote = 100
    for i in range(0, len(produtos), lote):
        batch = produtos[i:i+lote]
        r = post('/api/db/pedrinho/importar', {'produtos': batch})
        if 'error' in r:
            erros += len(batch)
            print(f"  ❌ Lote {i//lote+1}: {r}")
        else:
            ok += r.get('inseridos', len(batch))
            print(f"  ✅ Lote {i//lote+1}: {ok} produtos salvos")
        time.sleep(0.3)

    print(f"\n{'='*50}")
    print(f"✅ Importados: {ok}")
    print(f"❌ Erros:      {erros}")
    print(f"📊 Total:      {len(produtos)}")
    print(f"🔗 Fotos linkadas: {sum(1 for p in produtos if p['fotos'])}")

if __name__ == '__main__':
    arquivo = sys.argv[1] if len(sys.argv) > 1 else 'PERGUNTE_AO_PEDRINHO_-_V1__2_.xlsm'
    importar(arquivo)
