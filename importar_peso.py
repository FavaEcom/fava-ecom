"""
importar_peso.py — Importa peso, dimensões e fiscal da planilha para o banco Railway.
Uso: python importar_peso.py "PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm"
"""
import sys, os, pandas as pd, psycopg2, psycopg2.extras

FILE = sys.argv[1] if len(sys.argv) > 1 else "PROJETO_FAVA_ECOM_V20 - 01-04-26 - 17HRS.xlsm"
DB   = os.environ.get("DATABASE_URL", "")

if not DB:
    # Tentar .env local
    try:
        for line in open(".env"):
            if line.startswith("DATABASE_URL"):
                DB = line.split("=",1)[1].strip()
    except: pass

if not DB:
    print("DATABASE_URL não configurado. Defina a variável de ambiente.")
    sys.exit(1)

conn = psycopg2.connect(DB)
cur  = conn.cursor()

# ── Garantir colunas existem na tabela produtos ──────────────────────────────
print("Verificando colunas da tabela produtos...")
for col, tipo in [
    ("peso",         "REAL DEFAULT 0"),
    ("largura",      "REAL DEFAULT 0"),
    ("altura",       "REAL DEFAULT 0"),
    ("comprimento",  "REAL DEFAULT 0"),
    ("st",           "INTEGER DEFAULT 0"),
    ("st_imposto",   "REAL DEFAULT 0"),
    ("monofasico",   "INTEGER DEFAULT 0"),
]:
    try:
        cur.execute(f"ALTER TABLE produtos ADD COLUMN IF NOT EXISTS {col} {tipo}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  Coluna {col}: {e}")

# ── Ler BASE_DADOS_V2 ─────────────────────────────────────────────────────────
print(f"Lendo BASE_DADOS_V2 de {FILE}...")
df = pd.read_excel(FILE, sheet_name="BASE_DADOS_V2", header=2, dtype=str)
df.columns = [str(c).strip() for c in df.columns]

# Mapear colunas pelo índice (mais robusto)
cols = list(df.columns)
# Encontrar colunas por nome parcial
def find_col(df, *nomes):
    for n in nomes:
        for c in df.columns:
            if n.upper() in str(c).upper():
                return c
    return None

col_sku    = find_col(df, "SKU")
col_peso   = find_col(df, "PESO (kg)", "PESO (K")
col_larg   = find_col(df, "LARGURA")
col_alt    = find_col(df, "ALTURA")
col_prof   = find_col(df, "PROFUNDIDADE")
col_st     = find_col(df, "ST", "SUBSTITUICAO")
col_stimp  = find_col(df, "ST IMPOSTO")
col_mono   = find_col(df, "MONOF")

print(f"  SKU: {col_sku} | Peso: {col_peso} | Largura: {col_larg} | Altura: {col_alt}")
print(f"  Profund: {col_prof} | ST: {col_st} | ST Imp: {col_stimp} | Mono: {col_mono}")

def to_float(v, default=0.0):
    try: return float(str(v).replace(",","."))
    except: return default

def is_sim(v):
    return str(v).strip().upper() in ("SIM", "S", "1", "TRUE", "YES")

ok = err = 0
produtos_peso = {}  # sku -> {peso, largura, altura, comprimento, st, st_imposto, monofasico}

for _, row in df.iterrows():
    sku = str(row.get(col_sku, "")).strip()
    if not sku or sku in ("nan", "SKU", ""):
        continue
    try:
        int(sku)  # SKU deve ser numérico
    except:
        continue

    peso   = to_float(row.get(col_peso, 0))
    larg   = to_float(row.get(col_larg, 0))
    alt    = to_float(row.get(col_alt, 0))
    prof   = to_float(row.get(col_prof, 0))
    st     = 1 if is_sim(row.get(col_st, "")) else 0
    stimp  = to_float(row.get(col_stimp, 0))
    mono   = 1 if is_sim(row.get(col_mono, "")) else 0

    produtos_peso[sku] = dict(peso=peso, largura=larg, altura=alt, comprimento=prof,
                               st=st, st_imposto=stimp, monofasico=mono)

print(f"  {len(produtos_peso)} produtos com dados na planilha")

# ── Ler kits com peso de COMPOSICAO_KITS ────────────────────────────────────
print("Lendo COMPOSICAO_KITS...")
try:
    dk = pd.read_excel(FILE, sheet_name="COMPOSICAO_KITS", header=8, dtype=str)
    # Colunas: SKU_KIT, CMV KIT, PESO KIT
    kits_peso = {}
    for _, row in dk.iterrows():
        sku_kit = str(row.get("SKU_KIT", "")).strip()
        peso_kit = to_float(row.get("PESO KIT", 0))
        if sku_kit and sku_kit != "nan" and peso_kit > 0:
            kits_peso[sku_kit] = peso_kit
    print(f"  {len(kits_peso)} kits com peso total na planilha")
except Exception as e:
    print(f"  COMPOSICAO_KITS erro: {e}")
    kits_peso = {}

# ── UPDATE produtos — individuais ────────────────────────────────────────────
print("Atualizando produtos individuais no banco...")
for sku, d in produtos_peso.items():
    try:
        cur.execute("""
            UPDATE produtos SET
                peso=%s, largura=%s, altura=%s, comprimento=%s,
                st=%s, st_imposto=%s, monofasico=%s
            WHERE sku=%s
        """, (d["peso"], d["largura"], d["altura"], d["comprimento"],
               d["st"], d["st_imposto"], d["monofasico"], sku))
        ok += cur.rowcount
    except Exception as e:
        conn.rollback(); err += 1
        print(f"  ERRO sku {sku}: {e}")
        continue
    conn.commit()

print(f"  {ok} produtos atualizados | {err} erros")

# ── UPDATE ml_listings — peso para anúncios individuais ─────────────────────
print("Propagando peso para ml_listings...")
ml_ok = 0
for sku, d in produtos_peso.items():
    if d["peso"] <= 0:
        continue
    try:
        cur.execute("""
            UPDATE ml_listings SET
                peso=%s, largura=%s, altura=%s, comprimento=%s,
                st=%s, st_imposto=%s, monofasico=%s
            WHERE sku=%s
        """, (d["peso"], d["largura"], d["altura"], d["comprimento"],
               d["st"], d["st_imposto"], d["monofasico"], sku))
        ml_ok += cur.rowcount
        conn.commit()
    except Exception as e:
        conn.rollback()

print(f"  {ml_ok} anúncios ML atualizados com peso")

# ── UPDATE ml_listings — kits: soma automática de componentes ────────────────
print("Calculando peso de kits via kits_mapa...")
try:
    cur.execute("""
        SELECT km.sku_kit, SUM(p.peso * km.qtd) as peso_total
        FROM kits_mapa km
        JOIN produtos p ON p.sku = km.sku_componente
        WHERE p.peso > 0
        GROUP BY km.sku_kit
    """)
    kits_db = {r[0]: float(r[1]) for r in cur.fetchall()}
    print(f"  {len(kits_db)} kits calculados pelo banco (kits_mapa)")
    # Mesclar com kits da planilha (planilha tem prioridade)
    kits_final = {**kits_db, **kits_peso}
except Exception as e:
    print(f"  kits_mapa erro: {e}")
    conn.rollback()
    kits_final = kits_peso

# Atualizar ml_listings para kits
kits_ml = 0
for sku_kit, peso_kit in kits_final.items():
    try:
        cur.execute("UPDATE ml_listings SET peso=%s WHERE sku=%s AND peso=0",
                    (peso_kit, sku_kit))
        kits_ml += cur.rowcount
        conn.commit()
    except Exception as e:
        conn.rollback()

print(f"  {kits_ml} anúncios de kit atualizados com peso")

# ── Resultado final ───────────────────────────────────────────────────────────
cur.execute("SELECT COUNT(*) FROM ml_listings WHERE peso > 0")
total_com_peso = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM ml_listings")
total = cur.fetchone()[0]
print(f"\n✅ Concluído! ml_listings com peso: {total_com_peso}/{total}")

cur.close()
conn.close()
