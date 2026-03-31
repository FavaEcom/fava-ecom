"""
FAVA ECOM — Comparar OP Toolsworld com Histórico de Compras
============================================================
Compara os preços da OP-019610 (31/03/2026) com compras anteriores
e identifica aumentos e reduções de preço.
"""

import requests
import json

RAILWAY_URL = 'https://web-production-5aa0f.up.railway.app'

# Produtos da OP-019610 Toolsworld — código fornecedor → (nome, valor_venda_unitario)
OP_PRODUTOS = {
    '409019':   ('GRAMPEADOR MANUAL TIPO 15/53/300/500 6-14MM MTX',      73.36),
    '982089':   ('CARRINHO CARGA 485X350MM 150KG DOBRAVEL MTX',         269.79),
    '413109':   ('GRAMPOS TIPO 140 TEMPERADO 10MM 1000PCS MTX',           2.90),
    '1833055':  ('ALICATE ANEIS EXTERNOS BICO RETO 6POL MTX',            10.44),
    '1832555':  ('ALICATE ANEIS INTERNOS BICO CURVO 6POL MTX',           10.11),
    '1833555':  ('ALICATE ANEIS INTERNOS BICO RETO 6POL MTX',            10.44),
    '1832055':  ('ALICATE ANEIS EXTERNOS BICO CURVO 6POL MTX',           10.11),
    '204049':   ('GRAMPO SARGENTO 250X50X310MM MTX',                     14.60),
    '154139':   ('JOGO CHAVE COMBINADA 6-32MM 25PCS MTX',               257.65),
    '70504655': ('BROCA CONCRETO 18X600MM SDS GROSS',                    23.72),
    '704739':   ('JOGO SERRAS COPO 19-64MM 11PCS MTX',                  17.07),
    '6146255':  ('PAZINHA BICO DOBRAVEL SERRILHADO 253X590MM PALISAD',   39.08),
    '5732255':  ('CALIBRADOR PNEUS ROSCA 1/4 NPT STELS',                 35.64),
    '204109':   ('GRAMPO SARGENTO 1000X120X1100MM MTX',                  76.04),
    '7091255':  ('BROCA CONCRETO 18X300MM SDS MTX',                      13.45),
    '1772255':  ('ALICATE DESENCAPADOR FIOS PISTOLA 0.2-6MM GROSS',      66.68),
    '185089':   ('MINI TORNO BANCADA MULTIFUNCIONAL MTX',                66.22),
    '614288':   ('PAZINHA BICO DOBRAVEL 150X205/600MM PALISAD',          39.78),
    '728209':   ('BROCA CERAMICA VIDRO 10MM MTX',                         4.93),
    '71007855': ('BROCA CONCRETO 28X460MM SDS MTX',                      45.60),
    '7232155':  ('JOGO BROCAS ACO RAPIDO 1-10MM 19PCS GROSS',           113.06),
    '7101655':  ('BROCA CONCRETO 6X160MM SDS MTX',                        3.30),
    '775939':   ('ARCO DE SERRA BIMETAL 300MM MTX',                      30.51),
    '1044355':  ('MINI MARTELO UNHA FIBRA VIDRO 25MM MTX',               16.53),
    '2107855':  ('PLAINA METAL N3 52X230MM SPARTA',                      45.22),
    '848749':   ('MISTURADOR TINTA 85X8X400MM MTX',                       7.26),
    '70502255': ('BROCA CONCRETO 10X600MM SDS GROSS',                    12.68),
    '164119':   ('JOGO CHAVE ALLEN EXTRA LONGA 2-12MM 9PCS MTX',         21.35),
    '2078255':  ('GRAMPO MULTIUSO MARCENEIRO 3POL MTX',                   2.14),
    '174229':   ('ALICATE BICO MEIA CANA CURVO 7POL MTX',                12.24),
    '982079':   ('CARRINHO CARGA 388X270MM 80KG DOBRAVEL MTX',          156.51),
    '982109':   ('CARRINHO UTILITARIO TELESCOPICO 150KG MTX',            355.41),
    '112269':   ('JOGO CHAVE ALLEN CURTA 1.5-10MM 9PCS MTX',              8.60),
    '8752559':  ('VENTOSA TRIPLA ALUMINIO MTX',                           65.66),
    '157089':   ('ALICATE BOMBA DPOL AGUA 10POL MTX',                    21.33),
    '323719':   ('ESQUADRO ACO CARBONO 24POL 600MM MTX',                 14.35),
    '115719':   ('SOQUETE MAGNETICO 10MM L45MM 2PC MTX',                  3.41),
    '7063055':  ('BROCA CONCRETO 10X110MM SDS MTX',                       5.36),
    '5731855':  ('PISTOLA PINTURA TANQUE ALTO 0.5MM STELS',              48.59),
    '7049155':  ('KIT INSTALACAO FECHADURA 22X48MM 3PCS MTX',            21.52),
    '1876055':  ('TALHADEIRA SEXTAVADA 25X300MM SPARTA',                 11.26),
    '7282155':  ('BROCA FURO CIRCULAR CERAMICA 30-130MM MTX',            25.00),
    '5731555':  ('PISTOLA PINTURA TANQUE ALTO 1.2-1.8MM 1000ML STELS',  67.84),
    '115689':   ('SOQUETE MAGNETICO 7MM L45MM 2PC MTX',                   3.41),
    '115699':   ('SOQUETE MAGNETICO 8MM L45MM 2PC MTX',                   3.41),
    '2078655':  ('GRAMPO MULTIUSO MARCENEIRO 6POL MTX',                   6.24),
    '7021255':  ('JOGO BROCAS 3 PONTAS MADEIRA 3-10MM 8PCS SPARTA',       5.93),
    '115669':   ('SOQUETE MAGNETICO 6MM L45MM 2PC MTX',                   3.23),
    '1585455':  ('JOGO LIMAS MURCA 8POL 5PCS MTX',                       45.09),
    '7106155':  ('BROCA CONCRETO 8X600MM SDS MTX',                       11.46),
    '3122655':  ('TRENA LONGA 50M 12.5MM MTX',                           69.08),
    '576549':   ('GRAMPOS PNEUMATICOS 8MM 5000PCS MTX',                  11.00),
    '576569':   ('GRAMPOS PNEUMATICOS 10MM 5000PCS MTX',                 11.51),
    '7097755':  ('BROCA CONCRETO 14X1000MM SDS MTX',                     23.45),
    '7097555':  ('BROCA CONCRETO 12X1000MM SDS MTX',                     21.89),
    '6474055':  ('KIT REPARO PULVERIZADOR 5/7L 5PCS PALISAD',            12.06),
}

def buscar_historico():
    """Busca histórico de compras do banco Railway."""
    try:
        r = requests.get(f'{RAILWAY_URL}/api/db/historico', timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f'[ERRO] Não foi possível buscar histórico: {e}')
        return []

def main():
    print('='*70)
    print('  FAVA ECOM — Comparativo OP-019610 x Histórico de Compras')
    print('  Fornecedor: TOOLSWORLD - ITAJAI | Data: 31/03/2026')
    print('='*70)

    print('\nBuscando histórico do banco Railway...')
    historico = buscar_historico()
    print(f'  {len(historico)} itens no histórico\n')

    # Monta mapa: cprod → lista de compras anteriores
    hist_map = {}
    for h in historico:
        cod = str(h.get('sku', '') or '').strip()
        if not cod:
            continue
        if cod not in hist_map:
            hist_map[cod] = []
        hist_map[cod].append({
            'data':   h.get('data_emissao', ''),
            'nf':     h.get('nf', ''),
            'forn':   h.get('fornecedor', ''),
            'vunit':  float(h.get('vunit', 0) or 0),
            'nome':   h.get('nome', ''),
        })

    # Compara
    aumentos   = []
    reducoes   = []
    sem_hist   = []
    sem_alter  = []

    for cod, (nome, preco_op) in OP_PRODUTOS.items():
        compras_ant = hist_map.get(cod, [])

        if not compras_ant:
            sem_hist.append((cod, nome, preco_op))
            continue

        # Pega compra mais recente (excluindo esta OP)
        compras_ord = sorted(compras_ant, key=lambda x: x['data'], reverse=True)
        ultima = compras_ord[0]
        preco_ant = ultima['vunit']

        if preco_ant <= 0:
            sem_hist.append((cod, nome, preco_op))
            continue

        variacao = ((preco_op - preco_ant) / preco_ant) * 100

        if variacao > 0.5:
            aumentos.append({
                'cod': cod, 'nome': nome,
                'preco_ant': preco_ant, 'preco_op': preco_op,
                'var': variacao,
                'data_ant': ultima['data'][:10] if ultima['data'] else '—',
                'nf_ant': ultima['nf'],
            })
        elif variacao < -0.5:
            reducoes.append({
                'cod': cod, 'nome': nome,
                'preco_ant': preco_ant, 'preco_op': preco_op,
                'var': variacao,
                'data_ant': ultima['data'][:10] if ultima['data'] else '—',
                'nf_ant': ultima['nf'],
            })
        else:
            sem_alter.append((cod, nome, preco_op, preco_ant))

    # Exibe resultados
    print(f'\n{"="*70}')
    print(f'  🔴 AUMENTOS DE PREÇO ({len(aumentos)} produtos)')
    print(f'{"="*70}')
    if aumentos:
        aumentos.sort(key=lambda x: x['var'], reverse=True)
        print(f'  {"CÓDIGO":<12} {"PRODUTO":<40} {"ANT":>8} {"ATUAL":>8} {"VAR":>7}  {"DATA ANT":<12}')
        print(f'  {"-"*88}')
        for a in aumentos:
            nome_curto = a['nome'][:38]
            print(f'  {a["cod"]:<12} {nome_curto:<40} R${a["preco_ant"]:>6.2f} R${a["preco_op"]:>6.2f} {a["var"]:>+6.1f}%  {a["data_ant"]}')
    else:
        print('  Nenhum aumento identificado')

    print(f'\n{"="*70}')
    print(f'  🟢 REDUÇÕES DE PREÇO ({len(reducoes)} produtos)')
    print(f'{"="*70}')
    if reducoes:
        reducoes.sort(key=lambda x: x['var'])
        print(f'  {"CÓDIGO":<12} {"PRODUTO":<40} {"ANT":>8} {"ATUAL":>8} {"VAR":>7}  {"DATA ANT":<12}')
        print(f'  {"-"*88}')
        for r in reducoes:
            nome_curto = r['nome'][:38]
            print(f'  {r["cod"]:<12} {nome_curto:<40} R${r["preco_ant"]:>6.2f} R${r["preco_op"]:>6.2f} {r["var"]:>+6.1f}%  {r["data_ant"]}')
    else:
        print('  Nenhuma redução identificada')

    print(f'\n{"="*70}')
    print(f'  ⚪ SEM HISTÓRICO ANTERIOR ({len(sem_hist)} produtos — primeira compra ou código novo)')
    print(f'{"="*70}')
    for cod, nome, preco in sem_hist:
        print(f'  {cod:<12} {nome[:48]:<48} R${preco:>7.2f}')

    print(f'\n{"="*70}')
    print(f'  ✅ SEM ALTERAÇÃO ({len(sem_alter)} produtos — variação < 0.5%)')
    print(f'{"="*70}')
    for cod, nome, preco_op, preco_ant in sem_alter:
        print(f'  {cod:<12} {nome[:48]:<48} R${preco_op:>7.2f}')

    print(f'\n{"="*70}')
    print(f'  RESUMO')
    print(f'  Aumentos:        {len(aumentos)} produtos')
    print(f'  Reduções:        {len(reducoes)} produtos')
    print(f'  Sem alteração:   {len(sem_alter)} produtos')
    print(f'  Sem histórico:   {len(sem_hist)} produtos')
    print(f'  Total OP:        {len(OP_PRODUTOS)} produtos')
    print(f'{"="*70}')

    input('\nEnter para fechar...')

if __name__ == '__main__':
    main()
