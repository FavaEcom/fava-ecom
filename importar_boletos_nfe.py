"""
FAVA ECOM — Importar boletos das NF-e (cobr/dup)
Roda após subir o server.py novo no Railway.
- Boletos vencidos há mais de 7 dias → marcados como pago=1 automaticamente
- Boletos dos últimos 7 dias e futuros → pago=0 (a pagar)
"""
import os, glob, requests
import xml.etree.ElementTree as ET
from datetime import date, timedelta

SERVER = 'https://web-production-5aa0f.up.railway.app'
PASTA  = r'\\192.168.0.103\Trabalho\NOTAS XLS'
HOJE   = date.today()
LIMITE = HOJE - timedelta(days=7)  # boletos com venc antes disso → pago=1

def tag(n): return f'{{http://www.portalfiscal.inf.br/nfe}}{n}'

print("=" * 60)
print("  FAVA ECOM — Importar Boletos das NF-e")
print(f"  Hoje: {HOJE} | Vencidos antes de {LIMITE} → marcados PAGO")
print("=" * 60)

# Verificar servidor
try:
    r = requests.get(f'{SERVER}/api/db/status', timeout=10)
    if not r.ok:
        print("ERRO: servidor não responde. Suba o server.py primeiro!")
        input("Enter...")
        exit(1)
    print("✅ Servidor OK\n")
except Exception as e:
    print(f"ERRO conexão: {e}")
    input("Enter...")
    exit(1)

xmls = glob.glob(os.path.join(PASTA, '**', '*.xml'), recursive=True) + \
       glob.glob(os.path.join(PASTA, '*.xml'))
print(f"{len(xmls)} XMLs encontrados")

boletos_por_nf = {}
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
        if cobr is None: continue

        nf_num = (ide.findtext(tag('nNF')) or '').strip() if ide else ''
        forn   = (emit.findtext(tag('xNome')) or '').strip()[:80] if emit else ''
        dups   = cobr.findall(tag('dup'))
        if not dups or not nf_num: continue

        nfs_com_boleto += 1
        parcelas = []
        for i, dup in enumerate(dups, 1):
            ndup  = (dup.findtext(tag('nDup')) or str(i)).strip()
            dvenc = (dup.findtext(tag('dVenc')) or '').strip()
            valor = float(dup.findtext(tag('vDup')) or 0)
            # Definir se está pago baseado na data de vencimento
            pago = 0
            if dvenc:
                try:
                    venc_date = date.fromisoformat(dvenc)
                    if venc_date < LIMITE:
                        pago = 1  # Vencido há mais de 7 dias → assumir pago
                except: pass
            parcelas.append({
                'nf': nf_num, 'fornecedor': forn,
                'parcela': i, 'total_parcelas': len(dups),
                'vencimento': dvenc or None,
                'valor': round(valor, 2),
                'num_boleto': ndup,
                'pago': pago,
            })
        boletos_por_nf[nf_num] = parcelas
    except: pass

print(f"{nfs_com_boleto} NFs com boletos\n")

# Enviar para Railway — só NFs sem boletos já salvos
print(f"Enviando {len(boletos_por_nf)} NFs...")
ok = pula = erro = 0
for nf, bols in boletos_por_nf.items():
    try:
        # Checar se já existe
        check = requests.get(f"{SERVER}/api/db/boletos?nf={nf}", timeout=10).json()
        if check:
            pula += 1
            continue  # Já tem boletos — não sobrescrever (podem estar editados)
        r = requests.post(f"{SERVER}/api/db/boletos-salvar",
            json={'nf': nf, 'boletos': bols}, timeout=15)
        if r.ok and r.json().get('ok'):
            ok += 1
        else:
            erro += 1
            if erro <= 3:
                print(f"  ERRO NF {nf}: {r.text[:100]}")
    except Exception as e:
        erro += 1

pagos_auto = sum(1 for bols in boletos_por_nf.values() for b in bols if b['pago'])
print(f"\n✅ {ok} NFs importadas | ⏭️ {pula} já existiam | ❌ {erro} erros")
print(f"📅 {pagos_auto} parcelas marcadas automaticamente como PAGAS (vencidas há +7 dias)")
input("\nEnter...")
