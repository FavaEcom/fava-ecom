"""
FAVA ECOM — Importar boletos das NF-e (cobr/dup)
Roda separado após importar_todas_nfe_v4.py
"""
import os, glob, json, requests
import xml.etree.ElementTree as ET

SERVER = 'https://web-production-5aa0f.up.railway.app'
PASTA  = r'\\192.168.0.103\Trabalho\NOTAS XLS'

def tag(n): return f'{{http://www.portalfiscal.inf.br/nfe}}{n}'

print("=" * 60)
print("  FAVA ECOM — Importar Boletos das NF-e")
print("=" * 60)

xmls = glob.glob(os.path.join(PASTA, '**', '*.xml'), recursive=True) + \
       glob.glob(os.path.join(PASTA, '*.xml'))

print(f"{len(xmls)} XMLs encontrados\n")

boletos_total = []
nfs_com_boleto = 0

for f in xmls:
    try:
        tree = ET.parse(f)
        root = tree.getroot()
        nfe  = root.find(tag('NFe')) or root
        inf  = nfe.find(tag('infNFe')) or nfe
        ide  = inf.find(tag('ide'))
        emit = inf.find(tag('emit'))
        cobr = inf.find(tag('cobr'))
        
        if cobr is None:
            continue  # Nota sem boleto (dinheiro/cartão/etc)
        
        nf_num = (ide.findtext(tag('nNF')) or '').strip() if ide is not None else ''
        forn   = (emit.findtext(tag('xNome')) or '').strip()[:80] if emit is not None else ''
        data_e = (ide.findtext(tag('dEmi')) or '').strip() if ide is not None else ''
        
        dups = cobr.findall(tag('dup'))
        if not dups:
            continue
        
        nfs_com_boleto += 1
        for i, dup in enumerate(dups, 1):
            ndup  = (dup.findtext(tag('nDup')) or str(i)).strip()
            dvenc = (dup.findtext(tag('dVenc')) or '').strip()
            valor = float(dup.findtext(tag('vDup')) or 0)
            boletos_total.append({
                'nf': nf_num,
                'fornecedor': forn,
                'parcela': i,
                'total_parcelas': len(dups),
                'vencimento': dvenc or None,
                'valor': valor,
                'num_boleto': ndup,
                'pago': 0,
            })
    except Exception as e:
        pass

print(f"{nfs_com_boleto} NFs com boletos | {len(boletos_total)} parcelas no total\n")

if not boletos_total:
    print("Nenhum boleto encontrado nos XMLs.")
    input("Enter...")
    exit()

# Enviar em lotes por NF (não apagar existentes que já foram marcados como pagos)
from collections import defaultdict
por_nf = defaultdict(list)
for b in boletos_total:
    por_nf[b['nf']].append(b)

print(f"Enviando {len(por_nf)} NFs para Railway...")
ok = erro = 0
for nf, bols in por_nf.items():
    try:
        # Só importa se ainda não tem boletos salvos nessa NF
        check = requests.get(f"{SERVER}/api/db/boletos?nf={nf}", timeout=10).json()
        if check:
            continue  # Já tem boletos salvos (podem estar pagos/editados)
        r = requests.post(f"{SERVER}/api/db/boletos-salvar",
            json={'nf': nf, 'boletos': bols}, timeout=15)
        if r.ok and r.json().get('ok'):
            ok += 1
        else:
            erro += 1
    except Exception as e:
        erro += 1

print(f"\n✅ {ok} NFs importadas | ❌ {erro} erros")
print("Boletos com pago=True não são sobrescritos.")
input("\nEnter...")
