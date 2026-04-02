"""
importar_base_dados.py — Importa BASE_DADOS_V2 para Railway
Complementa produtos existentes com: família, categoria, ST, CMV PR, peso, medidas
Uso: python importar_base_dados.py "PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm"
"""
import sys, json, time
import openpyxl
import urllib.request, urllib.error

RAILWAY = 'https://web-production-5aa0f.up.railway.app'

def post(path, data):
    body = json.dumps(data, ensure_ascii=False, default=str).encode('utf-8')
    req  = urllib.request.Request(
        RAILWAY + path, data=body,
        headers={'Content-Type': 'application/json; charset=utf-8'},
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {'error': e.code, 'msg': e.read().decode()[:200]}
    except Exception as e:
        return {'error': str(e)}

def get(path):
    req = urllib.request.Request(RAILWAY + path)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except Exception as e:
        return {}

def flt(v):
    try: return float(v) if v is not None else 0.0
    except: return 0.0

def s(v):
    return str(v).strip() if v is not None else ''

def importar(arquivo):
    print(f'\n📂 Lendo {arquivo}...')
    wb = openpyxl.load_workbook(arquivo, read_only=True, data_only=True)
    ws = wb['BASE_DADOS_V2']

    # Lê todos os dados a partir da linha 4 (dados começam na linha 4)
    produtos = []
    skus_vistos = set()

    for row in ws.iter_rows(min_row=4, values_only=True):
        # Colunas (1-indexed):
        # 2=STATUS, 3=SKU, 4=CODIGO, 5=PRODUTO, 6=MARCA, 7=FAMÍLIA, 9=SUBCATEGORIA
        # 10=TIPO_PRODUTO, 22=FORNECEDOR, 28=ORIGEM, 29=CST, 31=CFOP, 33=NCM, 35=EAN
        # 38=ICMS_CHEIA, 39=ICMS_EFET, 40=IPI, 41=PIS, 42=COFINS, 46=ST
        # 49=CMV_BR, 50=CMV_PR, 52=PESO, 53=LARGURA, 54=ALTURA, 55=PROFUNDIDADE
        # 45=MONOFASICO, 48=TIPO_FISICO

        status  = s(row[1])   # col 2
        sku_raw = row[2]      # col 3
        if not sku_raw: continue
        # Aceita SKU numérico ou formato kit/variação (ex: 10-1700, 2-1562)
        try:
            sku_num = int(float(str(sku_raw)))
            sku = str(sku_num)
        except:
            sku = str(sku_raw).strip()
        if not sku or sku in skus_vistos: continue
        skus_vistos.add(sku)

        nome     = s(row[4])   # col 5
        marca    = s(row[5])   # col 6
        familia  = s(row[6])   # col 7
        categ_h  = s(row[7])   # col 8 (CATEGORIA header visto como True na leitura)
        subcat   = s(row[8])   # col 9 SUBCATEGORIA
        tipo_p   = s(row[9])   # col 10 TIPO PRODUTO
        forn     = s(row[21])  # col 22 FORNECEDOR
        origem   = s(row[27])  # col 28
        cst      = s(row[28])  # col 29
        cfop     = s(row[30])  # col 31
        ncm      = s(row[32])  # col 33
        ean      = s(row[34])  # col 35
        icms_cheia= flt(row[37]) # col 38
        icms_efet = flt(row[38]) # col 39
        ipi       = flt(row[39]) # col 40
        pis       = flt(row[40]) # col 41
        cofins    = flt(row[41]) # col 42
        monof     = s(row[44])  # col 45
        st_raw    = s(row[45])  # col 46 ST
        tipo_fis  = s(row[47]) # col 48
        cmv_br    = flt(row[48]) # col 49
        cmv_pr    = flt(row[49]) # col 50
        peso      = flt(row[51]) # col 52
        largura   = flt(row[52]) # col 53
        altura    = flt(row[53]) # col 54
        prof      = flt(row[54]) # col 55

        tem_st = 1 if st_raw and 'sim' in st_raw.lower() else 0

        if not nome or cmv_br <= 0: continue

        produtos.append({
            'sku': sku,
            'nome': nome,
            'marca': marca,
            'familia': familia,
            'categoria': subcat or categ_h,
            'subcategoria': subcat,
            'tipo_produto': tipo_p,
            'fornecedor': forn,
            'origem': origem,
            'cst': cst,
            'cfop': cfop,
            'ncm': ncm,
            'ean': ean,
            'icms_cheia': icms_cheia,
            'icms_efetiva': icms_efet,
            'ipi': ipi,
            'pis': pis,
            'cofins': cofins,
            'monofasico': 1 if 'sim' in monof.lower() else 0,
            'tem_st': tem_st,
            'tipo_fisico': tipo_fis,
            'custo_br': cmv_br,
            'custo_pr': cmv_pr,
            'peso': peso,
            'largura': largura,
            'altura': altura,
            'profundidade': prof,
            'status': status,
        })

    print(f'✅ {len(produtos)} produtos lidos da BASE_DADOS_V2')

    # Envia para Railway via cmv-cache (atualiza custo_br e custo_pr)
    # E depois atualiza os campos complementares via produto endpoint
    print(f'📤 Enviando dados...')

    # Passo 1: Atualiza CMV (que já existe)
    lote_cmv = 200
    ok_cmv = 0
    for i in range(0, len(produtos), lote_cmv):
        batch = produtos[i:i+lote_cmv]
        payload = {p['sku']: {'cmv': p['custo_br'], 'cmvPr': p['custo_pr'], 'nome': p['nome']} for p in batch}
        r = post('/api/cmv-cache', payload)
        if 'error' not in r:
            ok_cmv += len(batch)
        print(f'  CMV lote {i//lote_cmv+1}: {ok_cmv} atualizados')
        time.sleep(0.2)

    # Passo 2: Atualiza campos complementares via /api/db/produto
    lote = 50
    ok = 0
    erros = 0
    for i in range(0, len(produtos), lote):
        batch = produtos[i:i+lote]
        for p in batch:
            r = post('/api/db/produto', {
                'sku':      p['sku'],
                'nome':     p['nome'],
                'marca':    p['marca'],
                'familia':  p['familia'],
                'custo':    p['custo_br'],
                'custo_br': p['custo_br'],
                'custo_pr': p['custo_pr'],
                'ipi':      p['ipi'],
                'fornecedor': p['fornecedor'],
            })
            if 'error' not in r: ok += 1
            else: erros += 1

        pct = round((i+len(batch))/len(produtos)*100)
        print(f'  Produtos {i+len(batch)}/{len(produtos)} ({pct}%) — ✅{ok} ❌{erros}')
        time.sleep(0.3)

    # Passo 3: Salva cadastro completo com todos os campos
    print(f'\n📦 Salvando cadastro completo...')
    ok2 = 0
    for i in range(0, len(produtos), lote):
        batch = produtos[i:i+lote]
        for p in batch:
            r = post('/api/db/cadastro', {
                'sku':          p['sku'],
                'nome':         p['nome'],
                'fornecedor':   p['fornecedor'],
                'ncm':          p['ncm'],
                'cst':          p['cst'],
                'cfop':         p['cfop'],
                'ipi':          p['ipi'],
                'tem_st':       p['tem_st'],
                'custo':        p['custo_br'],
                'custo_br':     p['custo_br'],
                'custo_pr':     p['custo_pr'],
                'peso':         p['peso'],
                'comprimento':  p['profundidade'],
                'largura':      p['largura'],
                'altura':       p['altura'],
                'ean':          p['ean'],
                'categoria':    p['categoria'],
                'familia':      p['familia'],
                'status_cadastro': 'importado',
            })
            if 'error' not in r: ok2 += 1
        print(f'  Cadastro {i+len(batch)}/{len(produtos)}')
        time.sleep(0.3)

    # Resultado final
    status_banco = get('/api/db/status')
    print(f'\n{"="*55}')
    print(f'✅ CMV atualizados:      {ok_cmv}')
    print(f'✅ Produtos atualizados: {ok}')
    print(f'✅ Cadastros salvos:     {ok2}')
    print(f'❌ Erros:                {erros}')
    print(f'📊 Total no banco:       {status_banco.get("produtos","?")}')

    # Salva lista de famílias e categorias para usar no sistema
    familias = sorted(set(p['familia'] for p in produtos if p['familia']))
    categorias = sorted(set(p['categoria'] for p in produtos if p['categoria']))
    with open('familias_categorias.json','w',encoding='utf-8') as f:
        json.dump({'familias': familias, 'categorias': categorias}, f, ensure_ascii=False, indent=2)
    print(f'📋 {len(familias)} famílias e {len(categorias)} categorias salvas em familias_categorias.json')

if __name__ == '__main__':
    arquivo = sys.argv[1] if len(sys.argv) > 1 else 'PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm'
    importar(arquivo)
