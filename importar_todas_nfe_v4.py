"""
FAVA ECOM — Importar NF-e XML v4.3 DEFINITIVO
==============================================
- cprod (código do fornecedor) salvo corretamente
- cst, cest, tem_st, orig salvos de cada item
- Crédito ICMS = vICMS / qtd (real da nota, nunca fixo)
- Itens SEM mapeamento cprod → importados com sku='' (aparecem laranja na entrada_nf)
- Resumo no final: NFs com itens sem SKU para conferência
"""

import os, glob, json, xml.etree.ElementTree as ET, urllib.request
from datetime import datetime
from collections import defaultdict

XML_FOLDER  = r'\\192.168.0.103\Trabalho\NOTAS XLS'
RAILWAY_URL = 'https://web-production-5aa0f.up.railway.app'

PIS    = 0.0165
COFINS = 0.076
CNPJ_RAIZ = '48328969'  # grupo Fava Ecom
NS     = 'http://www.portalfiscal.inf.br/nfe'

def tag(n): return f'{{{NS}}}{n}'
def ft(el, *ps, d=''):
    cur = el
    for p in ps:
        if cur is None: return d
        cur = cur.find(tag(p))
    return (cur.text or d) if cur is not None else d
def fv(txt, d=0.0):
    try: return float(txt) if txt else d
    except: return d

ICMS_TAGS = ['ICMS00','ICMS10','ICMS20','ICMS30','ICMS40','ICMS51',
             'ICMS60','ICMS70','ICMS90','ICMSST','ICMSSN101','ICMSSN102',
             'ICMSSN201','ICMSSN202','ICMSSN500','ICMSSN900']

def parse_icms(imp):
    el = imp.find(tag('ICMS')) if imp is not None else None
    if el is None: return {}
    for g in ICMS_TAGS:
        grp = el.find(tag(g))
        if grp is not None:
            return {
                'pICMS':   fv(ft(grp,'pICMS')),
                'vICMS':   fv(ft(grp,'vICMS')),
                'vBC':     fv(ft(grp,'vBC')),
                'cst':     ft(grp,'CST') or ft(grp,'CSOSN') or '',
                'orig':    ft(grp,'orig') or '0',
                'vICMSST': fv(ft(grp,'vICMSST')),
                'pDif':    fv(ft(grp,'pDif')),
                'vICMSOp': fv(ft(grp,'vICMSOp')),
                'pRedBC':  fv(ft(grp,'pRedBC')),
            }
    return {}

def parse_ipi(imp):
    ipi = imp.find(tag('IPI')) if imp is not None else None
    if ipi is None: return 0.0, 0.0
    for g in ['IPITrib','IPINT']:
        grp = ipi.find(tag(g))
        if grp is not None:
            return fv(ft(grp,'pIPI')), fv(ft(grp,'vIPI'))
    return 0.0, 0.0

def find_safe(el, t):
    return el.find(f'.//{tag(t)}')

def parse_xml(path):
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        nfe = find_safe(root, 'NFe')
        if nfe is None: nfe = root
        inf = nfe.find(tag('infNFe'))
        if inf is None: inf = nfe
        emit = inf.find(tag('emit')) if len(inf.find(tag('emit')) if inf.find(tag('emit')) is not None else []) >= 0 else inf
        ide  = inf.find(tag('ide'))  if inf.find(tag('ide'))  is not None else inf

        # Usar find_safe para evitar DeprecationWarning
        emit = inf.find(tag('emit'))
        ide  = inf.find(tag('ide'))
        if emit is None: emit = inf
        if ide  is None: ide  = inf

        nNF   = ft(ide,'nNF')
        dhEmi = (ft(ide,'dhEmi') or ft(ide,'dEmi'))[:10] if (ft(ide,'dhEmi') or ft(ide,'dEmi')) else ''
        forn  = ft(emit,'xNome')
        cnpj  = ft(emit,'CNPJ')
        cnpj_limpo = cnpj.replace('.','').replace('/','').replace('-','')
        tipo_nf = 'transferencia' if cnpj_limpo.startswith(CNPJ_RAIZ) else 'compra'

        prot  = find_safe(root,'infProt')
        chave = ft(prot,'chNFe') if prot is not None else ''
        if not chave: chave = (inf.get('Id') or '').replace('NFe','')

        tot_el = inf.find(tag('total'))
        icms_t = tot_el.find(tag('ICMSTot')) if tot_el is not None else None
        vNF    = fv(ft(icms_t,'vNF')) if icms_t is not None else 0.0

        # Extrair boletos (cobr/dup)
        cobr = inf.find(tag('cobr'))
        dups = []
        if cobr is not None:
            dup_els = cobr.findall(tag('dup'))
            for i, dup in enumerate(dup_els, 1):
                dups.append({
                    'parcela': i,
                    'total': len(dup_els),
                    'ndup':  ft(dup,'nDup') or str(i),
                    'dvenc': ft(dup,'dVenc') or '',
                    'valor': fv(ft(dup,'vDup')),
                })

        itens = []
        for i, det in enumerate(inf.findall(tag('det'))):
            prod = det.find(tag('prod'))
            imp  = det.find(tag('imposto'))
            if prod is None: continue

            cprod  = ft(prod,'cProd')
            xProd  = ft(prod,'xProd')
            qCom   = fv(ft(prod,'qCom'), 1)
            vUnCom = fv(ft(prod,'vUnCom'))
            vProd  = fv(ft(prod,'vProd'))
            ncm    = ft(prod,'NCM')
            cfop   = ft(prod,'CFOP')
            cest   = ft(prod,'CEST') or ''

            ic   = parse_icms(imp)
            pIPI, vIPIit = parse_ipi(imp)

            cst     = ic.get('cst','')
            orig    = int(ic.get('orig','0') or 0)
            vSTit   = ic.get('vICMSST', 0.0)
            vICMSit = ic.get('vICMS', 0.0)
            vBC     = ic.get('vBC', 0.0)
            pICMS   = ic.get('pICMS', 0.0)
            pRedBC  = ic.get('pRedBC', 0.0)

            tem_st       = (cst in ['10','30','60','70']) or bool(cest) or (vSTit > 0)
            base_reduzida = (pRedBC > 0) or (vBC > 0 and vBC < vProd * 0.99)

            ipi_un = vIPIit / max(qCom, 1)
            st_un  = vSTit  / max(qCom, 1)
            custo  = vUnCom + ipi_un + st_un

            cred_icms_unit = vICMSit / max(qCom, 1)
            cred_pis  = custo * PIS
            cred_cof  = custo * COFINS

            if tipo_nf == 'transferencia':
                # Transferencia interna: sem credito PIS/COFINS
                cmv_br = custo - cred_icms_unit
                cmv_pr = custo - cred_icms_unit
            else:
                cmv_br = custo - cred_icms_unit - cred_pis - cred_cof
                cmv_pr = (custo - cred_pis - cred_cof) if tem_st else cmv_br

            itens.append({
                'cprod': cprod, 'xprod': xProd,
                'qcom': qCom, 'vunit': vUnCom, 'vprod': vProd,
                'ncm': ncm, 'cfop': cfop, 'cest': cest,
                'cst': cst, 'orig': orig, 'tem_st': int(tem_st),
                'base_red': int(base_reduzida),
                'ipi_p': pIPI, 'ipi_un': ipi_un, 'icms_p': pICMS,
                'vst': vSTit, 'st_un': st_un, 'custo': custo,
                'cred_icms': cred_icms_unit,
                'cmv_br': cmv_br, 'cmv_pr': cmv_pr,
                'det_num': i + 1,
            })

        return {
            'chave': chave or f'{nNF}-{cnpj}',
            'nf': nNF, 'fornecedor': forn, 'tipo': tipo_nf,
            'cnpj': cnpj, 'cnpj_emit': cnpj_limpo, 'emissao': dhEmi,
            'valor': vNF, 'itens': itens, 'dups': dups,
        }
    except Exception as e:
        print(f'  [ERRO XML] {path}: {e}')
        return None

def api(endpoint, payload=None, method='POST'):
    try:
        url = f'{RAILWAY_URL}{endpoint}'
        if payload is None and method=='GET':
            r = urllib.request.urlopen(url, timeout=20)
        else:
            data = json.dumps(payload or {}, default=str).encode()
            req  = urllib.request.Request(url, data=data,
                   headers={'Content-Type':'application/json'}, method=method)
            r = urllib.request.urlopen(req, timeout=30)
        return json.loads(r.read())
    except Exception as e:
        print(f'  [ERRO] {endpoint}: {e}')
        return None

def carregar_cprod_map():
    try:
        r = urllib.request.urlopen(f'{RAILWAY_URL}/api/db/cprod-map', timeout=15)
        d = json.loads(r.read())
        return {k: (v.get('sku','') if isinstance(v,dict) else str(v)) for k,v in d.items()}
    except: return {}

def nfs_no_banco():
    """Retorna set de NFs que ja existem no banco."""
    try:
        r = api('/api/db/nfs-existentes', method='GET')
        if r and isinstance(r, list): return set(str(x) for x in r)
    except: pass
    return set()

def main():
    print('='*62)
    print('  FAVA ECOM — Importar NF-e v4.3 DEFINITIVO')
    print('  cprod + cst + crédito ICMS real de cada item')
    print(f'  Pasta: {XML_FOLDER}')
    print('='*62)
    print()
    print('  [1] Importar apenas NFs NOVAS (automático, sem apagar)')
    print('  [2] Reimportar TUDO do zero (apagar tudo e reimportar)')
    print()
    modo = input('Escolha [1/2] (padrão 1): ').strip() or '1'
    apagar = 's' if modo == '2' else 'n'

    xmls = list(set(
        glob.glob(os.path.join(XML_FOLDER,'**','*.xml'), recursive=True) +
        glob.glob(os.path.join(XML_FOLDER,'*.xml'))
    ))
    print(f'\n{len(xmls)} XMLs encontrados')
    if not xmls: input('\nEnter...'); return

    print('Carregando cprod_map...')
    cprod_map = carregar_cprod_map()
    print(f'  {len(cprod_map)} mapeamentos\n')

    # Carregar NFs já existentes no banco (para modo automático)
    banco_nfs = set()
    if apagar == 'n':
        banco_nfs = nfs_no_banco()
        if banco_nfs:
            print(f'  {len(banco_nfs)} NFs já no banco — processando apenas novas\n')

    parsed = []
    alertas_sem_mapa = []  # NFs com itens sem mapeamento

    for i, xml_path in enumerate(sorted(xmls)):
        nf = parse_xml(xml_path)
        if not nf: continue
        # Modo automático: pular NFs que já existem no banco
        if apagar == 'n' and banco_nfs and str(nf['nf']) in banco_nfs:
            continue

        tipo_icon = '🔄' if nf.get('tipo')=='transferencia' else '🛒'
        st_n  = sum(1 for it in nf['itens'] if it['tem_st'])
        br_n  = sum(1 for it in nf['itens'] if it['base_red'])

        # Verificar itens sem mapeamento
        sem_mapa = [it for it in nf['itens'] if not cprod_map.get(it['cprod'])]
        flag_sem  = f' | ⚠️ {len(sem_mapa)} SEM MAPA' if sem_mapa else ''

        print(f'  [{i+1}/{len(xmls)}] NF {nf["nf"]:<10} | {nf["fornecedor"][:26]:<26} | {len(nf["itens"]):>3} itens | ST:{st_n} BR:{br_n}{flag_sem}')

        if sem_mapa:
            alertas_sem_mapa.append({
                'nf': nf['nf'],
                'forn': nf['fornecedor'][:30],
                'itens_sem_mapa': [{'cprod': it['cprod'], 'nome': it['xprod'][:40]} for it in sem_mapa]
            })

        parsed.append(nf)

    st_total  = sum(sum(1 for it in nf['itens'] if it['tem_st']) for nf in parsed)
    all_itens = sum(len(nf['itens']) for nf in parsed)
    sem_mapa_total = sum(len(a['itens_sem_mapa']) for a in alertas_sem_mapa)

    print(f'\n  {len(parsed)} NFs | {all_itens} itens | {st_total} com ST')
    if alertas_sem_mapa:
        print(f'  ⚠️  {sem_mapa_total} itens SEM mapeamento cprod→SKU em {len(alertas_sem_mapa)} NFs')
        print(f'     → Esses itens entram com SKU vazio (laranja na entrada_nf)')
        print(f'     → Vincule o SKU UMA VEZ na tela entrada_nf e ficará automático')

    if not parsed:
        print('\n  ✅ Nenhuma NF nova encontrada. Banco já atualizado!')
        input('\nEnter...')
        return

    # Apagar se solicitado
    if apagar == 's':
        print('\nApagando dados existentes...')
        for nf in parsed:
            r = api(f'/api/db/apagar-nf?nf={nf["nf"]}', method='GET')
            if r and r.get('apagados',0) > 0:
                print(f'  Apagado NF {nf["nf"]}: {r["apagados"]} registros')

    # Montar payload
    historico = []
    cmv_dict  = {}
    boletos_novos = {}  # nf → lista de dups

    for nf in parsed:
        api('/api/db/nf', {'chave':nf['chave'],'nf':nf['nf'],'forn':nf['fornecedor'],
                           'cnpj':nf['cnpj'],'emissao':nf['emissao'],'vNF':nf['valor'],'parcelas':[]})

        # Boletos
        if nf['dups']:
            boletos_novos[nf['nf']] = {
                'nf': nf['nf'],
                'fornecedor': nf['fornecedor'],
                'boletos': nf['dups']
            }

        for it in nf['itens']:
            sku = cprod_map.get(it['cprod'], '')
            historico.append({
                'nf':           nf['nf'],
                'fornecedor':   nf['fornecedor'],
                'data_emissao': nf['emissao'],
                'sku':          sku,
                'nome':         it['xprod'],
                'qtd':          it['qcom'],
                'vunit':        it['vunit'],
                'vtot':         it['vprod'],
                'ipi_p':        it['ipi_p'],
                'ipi_un':       it['ipi_un'],
                'icms_p':       it['icms_p'],
                'custo_r':      it['custo'],
                'cmv_br':       it['cmv_br'],
                'cmv_pr':       it['cmv_pr'],
                'ncm':          it['ncm'],
                'cfop':         it['cfop'],
                'cprod':        it['cprod'],
                'cst':          it['cst'],
                'cest':         it['cest'],
                'tipo':         nf['tipo'],
                'cnpj_emit':    nf.get('cnpj_emit',''),
                'tem_st':       it['tem_st'],
                'orig':         it['orig'],
                'v_st':         it['vst'],
                'cred_icms':    it['cred_icms'],
                'det_num':      it['det_num'],
            })
            if sku and it['cmv_br'] > 0:
                cmv_dict[sku] = {
                    'cmv':it['cmv_br'],'cmvBr':it['cmv_br'],
                    'cmvPr':it['cmv_pr'],'nome':it['xprod'],
                }

    # Salvar histórico
    print('\nLimpando registros anteriores...')
    nfs_unicas = list(set(str(it['nf']) for it in historico))
    for i in range(0, len(nfs_unicas), 100):
        api('/api/db/historico-apagar', {'nfs': nfs_unicas[i:i+100]})
    print(f'  {len(nfs_unicas)} NFs limpas')

    print('Salvando historico...')
    ok_hist = 0
    for i in range(0, len(historico), 200):
        r = api('/api/db/historico-inserir', historico[i:i+200])
        if r:
            salvos = r.get('inseridos', r.get('ok', 0))
            ok_hist += salvos if isinstance(salvos, int) else 0
        print(f'  Lote {i//200+1}: OK')

    # Salvar CMVs
    print(f'Salvando {len(cmv_dict)} CMVs...')
    skus = list(cmv_dict.keys())
    for i in range(0, len(skus), 200):
        api('/api/cmv-cache', {s:cmv_dict[s] for s in skus[i:i+200]})
        print(f'  CMV lote {i//200+1}: OK')

    # Salvar boletos (só NFs que não têm boletos ainda)
    if boletos_novos:
        from datetime import date, timedelta
        HOJE  = date.today()
        LIMITE = HOJE - timedelta(days=7)
        print(f'\nImportando boletos de {len(boletos_novos)} NFs...')
        ok_bol = pula_bol = 0
        for nf_num, bd in boletos_novos.items():
            try:
                check = api(f'/api/db/boletos?nf={nf_num}', method='GET')
                if check:
                    pula_bol += 1
                    continue
                # Marcar vencidos como pagos
                bols = []
                for b in bd['boletos']:
                    pago = 0
                    if b['dvenc']:
                        try:
                            vd = date.fromisoformat(b['dvenc'])
                            if vd < LIMITE: pago = 1
                        except: pass
                    bols.append({
                        'nf': nf_num, 'fornecedor': bd['fornecedor'],
                        'parcela': b['parcela'], 'total_parcelas': b['total'],
                        'vencimento': b['dvenc'] or None,
                        'valor': round(b['valor'], 2),
                        'num_boleto': b['ndup'], 'pago': pago,
                    })
                r = api('/api/db/boletos-salvar', {'nf': nf_num, 'boletos': bols})
                if r and r.get('ok'): ok_bol += 1
            except: pass
        print(f'  ✅ {ok_bol} NFs com boletos importados | ⏭️ {pula_bol} já existiam')

    print(f'\n{"="*62}')
    print(f'  ✅ {ok_hist} itens salvos com cprod + cst + ICMS real')
    print(f'  ✅ {len(cmv_dict)} CMVs atualizados')
    if alertas_sem_mapa:
        print(f'\n  ⚠️  ITENS SEM MAPEAMENTO — abra entrada_nf.html para vincular:')
        for a in alertas_sem_mapa[:10]:  # Mostrar até 10
            cprods = ', '.join(it['cprod'] for it in a['itens_sem_mapa'][:5])
            print(f'     NF {a["nf"]} ({a["forn"]}): {cprods}')
        if len(alertas_sem_mapa) > 10:
            print(f'     ... e mais {len(alertas_sem_mapa)-10} NFs')
    print(f'{"="*62}')
    input('\nEnter...')

if __name__ == '__main__':
    main()
