"""
FAVA ECOM — Importador Automático NF-e XML
==========================================
Versão sem confirmação — para rodar agendado.
Só importa XMLs NOVOS (não importados antes).

Agendar no Windows:
  schtasks /create /tn "FavaEcom_NFe" /tr "cmd /c cd C:\FAVAECOM\scripts && python importar_nfe_auto.py >> C:\FAVAECOM\scripts\log_nfe.txt 2>&1" /sc daily /st 06:30
"""

import os, glob, json, xml.etree.ElementTree as ET, urllib.request
from datetime import datetime

XML_FOLDER  = r'\\192.168.0.103\Trabalho\NOTAS XLS'
RAILWAY_URL = 'https://web-production-5aa0f.up.railway.app'
IMPORTADAS_FILE = r'C:\FAVAECOM\scripts\nfe_importadas.json'

PIS = 0.0165; COFINS = 0.076
NS  = 'http://www.portalfiscal.inf.br/nfe'

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

ICMS_GROUPS = ['ICMS00','ICMS10','ICMS20','ICMS30','ICMS40','ICMS51',
               'ICMS60','ICMS70','ICMS90','ICMSST','ICMSSN101','ICMSSN102',
               'ICMSSN201','ICMSSN202','ICMSSN500','ICMSSN900']

def parse_icms(imp):
    el = imp.find(tag('ICMS'))
    if el is None: return 0.0, 0.0
    for g in ICMS_GROUPS:
        grp = el.find(tag(g))
        if grp is not None:
            return fval(findtext(grp,'pICMS')), fval(findtext(grp,'vICMS'))
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
    try:
        root = ET.parse(path).getroot()
        inf  = root.find(f'.//{tag("infNFe")}')
        emit = inf.find(tag('emit'))
        ide  = inf.find(tag('ide'))
        total_el = inf.find(tag('total'))
        icmsTot  = total_el.find(tag('ICMSTot')) if total_el is not None else None
        nNF = findtext(ide,'nNF')
        dhEmi = (findtext(ide,'dhEmi') or findtext(ide,'dEmi'))[:10]
        fornecedor = findtext(emit,'xNome')
        cnpj = findtext(emit,'CNPJ')
        prot = root.find(f'.//{tag("infProt")}')
        chave = findtext(prot,'chNFe') if prot is not None else ''
        if not chave:
            chave = (inf.get('Id') or '').replace('NFe','')
        itens = []
        for det in inf.findall(tag('det')):
            prod = det.find(tag('prod'))
            imp  = det.find(tag('imposto'))
            if prod is None: continue
            cProd  = findtext(prod,'cProd')
            xProd  = findtext(prod,'xProd')
            qCom   = fval(findtext(prod,'qCom'), 1)
            vUnCom = fval(findtext(prod,'vUnCom'))
            vProd  = fval(findtext(prod,'vProd'))
            ncm    = findtext(prod,'NCM')
            cfop   = findtext(prod,'CFOP')
            pIPI, vIPIit = (parse_ipi(imp) if imp is not None else (0.0,0.0))
            pICMS, vICMSit = (parse_icms(imp) if imp is not None else (0.0,0.0))
            vSTit = 0.0
            if imp is not None:
                icms_el = imp.find(tag('ICMS'))
                if icms_el is not None:
                    for g in ICMS_GROUPS:
                        grp = icms_el.find(tag(g))
                        if grp is not None:
                            vSTit = fval(findtext(grp,'vICMSST')); break
            custo_unit = (vProd + vIPIit + vSTit) / max(qCom,1)
            icms_pct = pICMS/100.0
            cmv_br = custo_unit - custo_unit*icms_pct - custo_unit*PIS - custo_unit*COFINS
            itens.append({'cprod':cProd,'xprod':xProd,'qcom':qCom,'vunit':vUnCom,
                         'vprod':vProd,'ipi_p':pIPI,'ipi_un':vIPIit/max(qCom,1),
                         'icms_p':pICMS,'vst':vSTit,'custo_unit':custo_unit,
                         'cmv_br':cmv_br,'ncm':ncm,'cfop':cfop})
        return {'chave':chave or f'{nNF}-{cnpj}','nf':nNF,'fornecedor':fornecedor,
                'cnpj':cnpj,'emissao':dhEmi,'valor':fval(findtext(icmsTot,'vNF') if icmsTot is not None else '0'),
                'itens':itens}
    except Exception as e:
        print(f'  [ERRO XML] {os.path.basename(path)}: {e}')
        return None

def post(endpoint, payload):
    try:
        data = json.dumps(payload, default=str).encode('utf-8')
        req  = urllib.request.Request(f'{RAILWAY_URL}{endpoint}', data=data,
                                      headers={'Content-Type':'application/json'}, method='POST')
        return json.loads(urllib.request.urlopen(req, timeout=30).read())
    except Exception as e:
        return None

def main():
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'\n[{ts}] Iniciando importação automática NF-e')

    if not os.path.exists(XML_FOLDER):
        print(f'  Pasta não encontrada: {XML_FOLDER}'); return

    # Carregar lista de NFs já importadas
    importadas = set()
    if os.path.exists(IMPORTADAS_FILE):
        try: importadas = set(json.load(open(IMPORTADAS_FILE)))
        except: pass

    # Carregar cprod_map
    try:
        resp = urllib.request.urlopen(f'{RAILWAY_URL}/api/db/cprod-map', timeout=15)
        raw = json.loads(resp.read())
        cprod_map = {k: (v.get('sku','') if isinstance(v,dict) else str(v)) for k,v in raw.items()}
    except:
        cprod_map = {}
    print(f'  cprod_map: {len(cprod_map)} mapeamentos')

    # Listar XMLs
    xmls = list(set(glob.glob(os.path.join(XML_FOLDER,'**','*.xml'), recursive=True) +
                    glob.glob(os.path.join(XML_FOLDER,'*.xml'))))

    # Filtrar novos
    novos = [x for x in xmls if os.path.basename(x) not in importadas]
    print(f'  {len(xmls)} XMLs | {len(novos)} novos')
    if not novos: print('  Nada a importar.'); return

    historico, cmv_dict, nfs_salvar, nfs_importadas = [], {}, [], []

    for xml_path in sorted(novos):
        nf = parse_xml(xml_path)
        if not nf: continue
        print(f'  NF {nf["nf"]} | {nf["fornecedor"][:25]} | {len(nf["itens"])} itens')
        nfs_salvar.append({'chave':nf['chave'],'nf':nf['nf'],'forn':nf['fornecedor'],
                           'cnpj':nf['cnpj'],'emissao':nf['emissao'],'vNF':nf['valor']})
        for it in nf['itens']:
            sku = cprod_map.get(it['cprod'],'')
            historico.append({'nf':nf['nf'],'fornecedor':nf['fornecedor'],
                              'data_emissao':nf['emissao'],'sku':sku,'nome':it['xprod'],
                              'qtd':it['qcom'],'vunit':it['vunit'],'vtot':it['vprod'],
                              'ipi_p':it['ipi_p'],'ipi_un':it['ipi_un'],'icms_p':it['icms_p'],
                              'custo_r':it['custo_unit'],'cmv_br':it['cmv_br'],'cmv_pr':it['cmv_br'],
                              'ncm':it['ncm'],'cfop':it['cfop'],'cprod':it['cprod']})
            if sku and it['cmv_br'] > 0:
                cmv_dict[sku] = {'cmv':it['cmv_br'],'cmvBr':it['cmv_br'],'cmvPr':it['cmv_br'],'nome':it['xprod']}
        nfs_importadas.append(os.path.basename(xml_path))

    # Enviar
    for nf_d in nfs_salvar:
        post('/api/db/nf', {**nf_d, 'parcelas':[]})
    
    hist_ok = 0
    for i in range(0, len(historico), 200):
        res = post('/api/db/historico', historico[i:i+200])
        if res: hist_ok += res.get('inseridos', res.get('ok', 0))

    cmv_ok = 0
    skus = list(cmv_dict.keys())
    for i in range(0, len(skus), 200):
        res = post('/api/cmv-cache', {s: cmv_dict[s] for s in skus[i:i+200]})
        if res: cmv_ok += res.get('n', 0)

    # Salvar lista de importadas
    importadas.update(nfs_importadas)
    with open(IMPORTADAS_FILE, 'w') as f:
        json.dump(list(importadas), f)

    print(f'  ✅ {len(nfs_importadas)} NFs | {hist_ok} histórico | {cmv_ok} CMVs')
    print(f'  Total acumulado: {len(importadas)} NFs importadas')

if __name__ == '__main__':
    main()
