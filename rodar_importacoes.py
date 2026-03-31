"""
FAVA ECOM — Runner: Executa importações em ordem
=================================================
Ordem correta:
  1. importar_base_dados_cmv.py    → CMV + mapa cProd
  2. importar_gestao_anuncios.py   → Anúncios ML
  3. importar_boletos.py           → Boletos/NFs

Uso:
  python rodar_importacoes.py          → roda tudo
  python rodar_importacoes.py 1        → só script 1
  python rodar_importacoes.py 1 2      → scripts 1 e 2
"""

import subprocess
import sys
import os
import time
import requests

BASE_URL = 'https://web-production-5aa0f.up.railway.app'
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = {
    1: ('importar_base_dados_cmv.py',    'CMV + mapa cProd da BASE_DADOS_V2'),
    2: ('importar_gestao_anuncios.py',   'Anúncios ML da GESTAO_FAVA_ECOM'),
    3: ('importar_boletos.py',           'Boletos da FAVA_ESTOQUE_V5'),
}

def verificar_banco():
    """Verifica status do banco antes de rodar."""
    try:
        r = requests.get(f'{BASE_URL}/api/db/status', timeout=10)
        d = r.json()
        print('── Status do banco ──────────────────────────')
        print(f'  Produtos:   {d.get("produtos", "?")} registros')
        print(f'  Historico:  {d.get("historico", "?")} registros')
        print(f'  Pedidos:    {d.get("pedidos", "?")} registros')
        print(f'  Boletos:    {d.get("boletos", "?")} registros')
        print(f'  Bling OK:   {d.get("bling_ok", False)}')
        print(f'  ML OK:      {d.get("ml_ok", False)}')
        print('─────────────────────────────────────────────')
        return True
    except Exception as e:
        print(f'[AVISO] Não foi possível verificar banco: {e}')
        print('  Verifique se o Railway está online.')
        return False

def rodar_script(num):
    nome, descricao = SCRIPTS[num]
    caminho = os.path.join(SCRIPTS_DIR, nome)
    
    if not os.path.exists(caminho):
        print(f'  [ERRO] Arquivo não encontrado: {caminho}')
        return False
    
    print(f'\n{"="*55}')
    print(f'SCRIPT {num}: {descricao}')
    print(f'{"="*55}')
    
    inicio = time.time()
    result = subprocess.run([sys.executable, caminho], capture_output=False)
    duracao = time.time() - inicio
    
    if result.returncode == 0:
        print(f'\n  ✅ Concluído em {duracao:.1f}s')
        return True
    else:
        print(f'\n  ❌ Falhou (código {result.returncode})')
        return False

def main():
    # Define quais scripts rodar
    args = [int(a) for a in sys.argv[1:] if a.isdigit()]
    scripts_para_rodar = args if args else [1, 2, 3]
    
    print('FAVA ECOM — Importação do Banco PostgreSQL Railway')
    print(f'URL: {BASE_URL}')
    print(f'Scripts: {scripts_para_rodar}\n')
    
    # Verifica banco
    banco_ok = verificar_banco()
    if not banco_ok:
        resp = input('\nContinuar mesmo assim? (s/N): ')
        if resp.lower() != 's':
            print('Abortado.')
            return
    
    print()
    resultados = {}
    
    for num in scripts_para_rodar:
        if num not in SCRIPTS:
            print(f'Script {num} não existe. Ignorando.')
            continue
        resultados[num] = rodar_script(num)
        time.sleep(1)  # Pausa entre scripts
    
    # Relatório final
    print(f'\n{"="*55}')
    print('RELATÓRIO FINAL')
    print(f'{"="*55}')
    for num, ok in resultados.items():
        status = '✅ OK' if ok else '❌ FALHOU'
        print(f'  Script {num} — {SCRIPTS[num][1]}: {status}')
    
    # Verifica estado final do banco
    print()
    verificar_banco()

if __name__ == '__main__':
    main()
