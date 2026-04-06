"""
FAVA ECOM — Importador em Massa de NF-e XML v4
===============================================
Lê todos os XMLs de NF-e da pasta de rede e salva no banco Railway:
  - Histórico de compras (por produto)
  - CMV atualizado por SKU
  - NFs no histórico para consulta
  - nf_rascunho → permite retomar cadastro pelo número da NF no painel

Uso: python importar_todas_nfe_v4.py
"""

import os, glob, json, xml.etree.ElementTree as ET, urllib.request, urllib.error
from datetime import datetime
from pathlib import Path

# ── CONFIGURAÇÃO ──────────────────────────────────────────────────────────────
XML_FOLDER  = r'\\192.168.0.103\Trabalho\NOTAS XLS'
RAILWAY_URL = 'https://web-production-5aa0f.up.railway.app'
CONFIG_FILE = r'C:\FAVAECOM\scripts\config.json'

PIS    = 0.0165
COFINS = 0.076

NS = 'http://www.portalfiscal.inf.br/nfe'

# ── HELPERS ───────────────────────────────────────────────────────────────────
def tag(n): return f'{{{NS}}}{n}'

def findtext(el, *path, default=''):
    cur = el
    for p in path:
        cur = cur.find(tag(p))
        if cur is None: return default
    return cur.text or default

def fval(txt, d=0.0):
    try: return float(txt) if txt else d
    except: return d

def parse_date(txt):
    if not txt: return ''
    try: return txt[:10]
    except: return ''

ICMS_GROUPS = ['ICMS00','ICMS10','ICMS20','ICMS30','ICMS40','ICMS51',
               'ICMS60','ICMS70','ICMS90','ICMSST','ICMSSN101','ICMSSN102',
               'ICMSSN201','ICMSSN202','ICMSSN500','ICMSSN900']

def parse_icms(imp):
    el = imp.find(tag('ICMS'))
    if el is None: return 0.0, 0.0
    for g in ICMS_GROUPS:
        grp = el.find(tag(g))
        if grp is not None:
            pICMS = fval(findtext(grp, 'pICMS'))
            vICMS = fval(findtext(grp, 'vICMS'))
            return pICMS, vICMS
    return 0.0, 0.0

def parse_ipi(imp):
    ipi = imp.find(tag('IPI'))
    if ipi is None: return 0.0, 0.0
    for g in ['IPITrib','IPINT']:
        grp = ipi.find(tag(g))
        if grp is not None:
            return fval(findtext(grp,'pIPI')), fval(findtext(grp,'vIPI'))
    return 0.0, 0.0

def parse_xml(path):
    """Lê um XML de NF-e e retorna dict com dados da nota + lista de itens."""
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        nfe  = root.find(f'.//{tag("NFe")}') or root
        inf  = nfe.find(tag('infNFe')) or nfe

        emit = inf.find(tag('emit')) or inf
        ide  = inf.find(tag('ide'))  or inf

        nNF      = findtext(ide, 'nNF')
        dhEmi    = parse_date(findtext(ide, 'dhEmi') or findtext(ide, 'dEmi'))
        fornecedor = findtext(emit, 'xNome')
        cnpj     = findtext(emit, 'CNPJ')

        total_el = inf.find(tag('total'))
        icmsTot  = total_el.find(tag('ICMSTot')) if total_el else None
        vNF      = fval(findtext(icmsTot, 'vNF') if icmsTot else '0')
        vIPI_tot = fval(findtext(icmsTot, 'vIPI') if icmsTot else '0')
        vST_tot  = fval(findtext(icmsTot, 'vST')  if icmsTot else '0')

        # Chave de acesso
        chave = ''
        prot = root.find(f'.//{tag("infProt")}')
        if prot is not None:
            chave = findtext(prot, 'chNFe')
        if not chave:
            chave = (inf.get('Id') or '').replace('NFe','')

        itens = []
        for det in inf.findall(tag('det')):
            prod = det.find(tag('prod'))
            imp  = det.find(tag('imposto'))
            if prod is None: continue

            cProd  = findtext(prod, 'cProd')
            xProd  = findtext(prod, 'xProd')
            qCom   = fval(findtext(prod, 'qCom'), 1)
            vUnCom = fval(findtext(prod, 'vUnCom'))
            vProd  = fval(findtext(prod, 'vProd'))
            ncm    = findtext(prod, 'NCM')
            cfop   = findtext(prod, 'CFOP')

            pIPI, vIPIit = parse_ipi(imp) if imp else (0.0, 0.0)
            pICMS, vICMSit = parse_icms(imp) if imp else (0.0, 0.0)

            # ST
            vSTit = 0.0
            if imp:
                icms_el = imp.find(tag('ICMS'))
                if icms_el:
                    for g in ICMS_GROUPS:
                        grp = icms_el.find(tag(g))
                        if grp is not None:
                            vSTit = fval(findtext(grp,'vICMSST'))
                            break

            # Custo unitário com IPI e ST
            custo_unit = (vProd + vIPIit + vSTit) / max(qCom, 1)

            # Créditos fiscais
            icms_pct   = pICMS / 100.0
            cred_icms  = custo_unit * icms_pct
            cred_pis   = custo_unit * PIS
            cred_cof   = custo_unit * COFINS
            cmv_br     = custo_unit - cred_icms - cred_pis - cred_cof

            itens.append({
                'cprod':      cProd,
                'xprod':      xProd,
                'qcom':       qCom,
                'vunit':      vUnCom,
                'vprod':      vProd,
                'ipi_p':      pIPI,
                'ipi_un':     vIPIit / max(qCom,1),
                'icms_p':     pICMS,
                'vst':        vSTit,
                'custo_unit': custo_unit,
                'cred_icms':  cred_icms,
                'cred_pis':   cred_pis,
                'cred_cof':   cred_cof,
                'cmv_br':     cmv_br,
                'ncm':        ncm,
                'cfop':       cfop,
            })

        return {
            'chave':      chave or f'{nNF}-{cnpj}',
            'nf':         nNF,
            'fornecedor': fornecedor,
            'cnpj':       cnpj,
            'emissao':    dhEmi,
            'valor':      vNF,
            'vIPI':       vIPI_tot,
            'vST':        vST_tot,
            'itens':      itens,
        }
    except Exception as e:
        print(f'  [ERRO XML] {path}: {e}')
        return None

def post_railway(endpoint, payload):
    try:
        data = json.dumps(payload, default=str).encode('utf-8')
        req  = urllib.request.Request(
            f'{RAILWAY_URL}{endpoint}',
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        resp = urllib.request.urlopen(req, timeout=30)
        return json.loads(resp.read())
    except Exception as e:
        print(f'  [ERRO Railway] {endpoint}: {e}')
        return None

def carregar_cprod_map():
    """Carrega mapeamento cProd → SKU do banco."""
    try:
        resp = urllib.request.urlopen(f'{RAILWAY_URL}/api/db/cprod-map', timeout=15)
        data = json.loads(resp.read())
        # Retorna dict {cprod: {sku, nome}} ou {cprod: sku}
        result = {}
        for cprod, v in data.items():
            if isinstance(v, dict):
                result[cprod] = v.get('sku', '')
            else:
                result[cprod] = str(v)
        return result
    except:
        return {}

def main():
    print('='*60)
    print('  FAVA ECOM — Importar NF-e XML → Railway v3')
    print(f'  Pasta: {XML_FOLDER}')
    print('='*60)

    # Verifica pasta
    if not os.path.exists(XML_FOLDER):
        print(f'\n❌ Pasta não encontrada: {XML_FOLDER}')
        input('\nEnter para fechar...')
        return

    # Lista XMLs
    xmls = glob.glob(os.path.join(XML_FOLDER, '**', '*.xml'), recursive=True)
    xmls += glob.glob(os.path.join(XML_FOLDER, '*.xml'))
    xmls = list(set(xmls))
    print(f'\n{len(xmls)} XMLs encontrados\n')

    if not xmls:
        print('Nenhum XML encontrado na pasta.')
        input('\nEnter...')
        return

    # Carrega mapa cProd → SKU
    print('Carregando mapa cProd → SKU do banco...')
    cprod_map = carregar_cprod_map()
    print(f'  {len(cprod_map)} mapeamentos carregados')

    # Processa XMLs
    nfs_ok, nfs_err = 0, 0
    historico = []
    nfs_salvar = []
    parsed_nfs = []   # v4: guarda NFs completas para salvar rascunho
    cmv_dict   = {}

    for i, xml_path in enumerate(sorted(xmls)):
        nf = parse_xml(xml_path)
        if not nf:
            nfs_err += 1
            continue

        print(f'  [{i+1}/{len(xmls)}] NF {nf["nf"]} | {nf["fornecedor"][:30]} | {len(nf["itens"])} itens')
        parsed_nfs.append(nf)   # v4: guarda para rascunho

        # Salva NF no histórico
        nfs_salvar.append({
            'chave':      nf['chave'],
            'nf':         nf['nf'],
            'forn':       nf['fornecedor'],
            'cnpj':       nf['cnpj'],
            'emissao':    nf['emissao'],
            'vNF':        nf['valor'],
        })

        # Monta itens do histórico
        for it in nf['itens']:
            sku = cprod_map.get(it['cprod'], '')
            historico.append({
                'nf':          nf['nf'],
                'fornecedor':  nf['fornecedor'],
                'data_emissao': nf['emissao'],
                'sku':         sku,
                'nome':        it['xprod'],
                'qtd':         it['qcom'],
                'vunit':       it['vunit'],
                'vtot':        it['vprod'],
                'ipi_p':       it['ipi_p'],
                'ipi_un':      it['ipi_un'],
                'icms_p':      it['icms_p'],
                'custo_r':     it['custo_unit'],
                'cmv_br':      it['cmv_br'],
                'cmv_pr':      it['cmv_br'],
                'ncm':         it['ncm'],
                'cfop':        it['cfop'],
                'cprod':       it['cprod'],
            })

            # CMV por SKU
            if sku and it['cmv_br'] > 0:
                cmv_dict[sku] = {
                    'cmv':   it['cmv_br'],
                    'cmvBr': it['cmv_br'],
                    'cmvPr': it['cmv_br'],
                    'nome':  it['xprod'],
                }

        nfs_ok += 1

    print(f'\n✅ {nfs_ok} NFs processadas | ❌ {nfs_err} erros')
    print(f'   {len(historico)} itens de histórico')
    print(f'   {len(cmv_dict)} CMVs com SKU mapeado')

    confirma = input('\nEnviar para o banco Railway? (s/n): ').strip().lower()
    if confirma != 's':
        input('\nEnter...'); return

    # Envia NFs
    print('\nSalvando NFs...')
    for nf_d in nfs_salvar:
        post_railway('/api/db/nf', {**nf_d, 'parcelas': []})

    # Envia histórico em lotes
    print('Salvando histórico de compras...')
    LOTE = 200
    hist_ok = 0
    for i in range(0, len(historico), LOTE):
        res = post_railway('/api/db/historico', historico[i:i+LOTE])
        if res:
            hist_ok += res.get('inseridos', res.get('ok', LOTE))
            print(f'  Lote {i//LOTE+1}: OK')

    # Envia CMV
    print(f'Salvando {len(cmv_dict)} CMVs...')
    LOTE_CMV = 200
    cmv_ok = 0
    skus = list(cmv_dict.keys())
    for i in range(0, len(skus), LOTE_CMV):
        lote = {s: cmv_dict[s] for s in skus[i:i+LOTE_CMV]}
        res  = post_railway('/api/cmv-cache', lote)
        if res:
            cmv_ok += res.get('n', len(lote))
            print(f'  Lote CMV {i//LOTE_CMV+1}: OK')

    # ── NOVO v4: Salva rascunhos para retomar no painel ─────────────
    print('\nSalvando rascunhos de NF no painel...')
    rascunhos_ok = 0
    for nf in parsed_nfs:
        fichas_rascunho = []
        for it in nf['itens']:
            sku = cprod_map.get(it['cprod'], '')
            fichas_rascunho.append({
                'codigo':       it['cprod'],
                'nome':         it['xprod'],
                'ncm':          it['ncm'],
                'cfop':         it['cfop'],
                'qtd':          it['qcom'],
                'sku':          sku,
                'existe':       bool(sku),
                'custoNF':      it['vunit'],
                'custoEntrada': it['custo_unit'],
                'ipiUn':        it['ipi_un'],
                'ipiPct':       it['ipi_p'] / 100 if it['ipi_p'] > 1 else it['ipi_p'],
                'stUn':         it['vst'] / max(it['qcom'], 1) if it.get('vst') else 0,
                'temST':        it.get('vst', 0) > 0,
                'cmvBr':        it['cmv_br'],
                'cmvPr':        it['cmv_br'],
                'monofasico':   False,
                'familia':      '',
                'peso':         0,
                'titulo_ml':    '',
                'titulo_shopee': '',
                'sel':          not bool(sku),
            })
        res = post_railway('/api/db/nf-rascunho', {
            'nf_num':    nf['nf'],
            'fornecedor': nf['fornecedor'],
            'cnpj':      nf['cnpj'],
            'data_nf':   nf['emissao'],
            'status':    'historico',
            'itens':     fichas_rascunho,
        })
        if res and res.get('ok'): rascunhos_ok += 1
    print(f'  ✅ {rascunhos_ok} rascunhos salvos')

    print(f'\n{"="*60}')
    print(f'  ✅ {hist_ok} itens histórico salvos')
    print(f'  ✅ {cmv_ok} CMVs atualizados no banco')
    print(f'  ✅ {rascunhos_ok} rascunhos salvos (retome no painel por nº NF)')
    print(f'{"="*60}')
    input('\nEnter para fechar...')

if __name__ == '__main__':
    main()
