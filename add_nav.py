"""
add_nav.py - Adiciona nav bar em todos os HTMLs do painel
Uso: cd C:\FAVAECOM\scripts && python add_nav.py
"""
import os

SRV = 'https://web-production-5aa0f.up.railway.app'

PAGES = [
    ('balcao_fava.html',     'Balcao'),
    ('produto_novo.html',    'Produto'),
    ('gestor_anuncios.html', 'Gestor'),
    ('calculadora_fava.html','Calculadora'),
    ('painel_fava.html',     'Painel'),
]

def build_nav(active):
    links = ''
    for fname, label in PAGES:
        is_active = label == active
        color  = '#93c5fd' if is_active else '#9ca3af'
        border = '#60a5fa' if is_active else 'transparent'
        links += (
            f'<a href="{SRV}/{fname}" '
            f'style="padding:8px 14px;color:{color};font-size:12px;font-weight:700;'
            f'text-decoration:none;border-bottom:2px solid {border};margin-bottom:-2px">'
            f'{label}</a>'
        )
    return (
        f'\n<nav style="background:#162040;padding:0 18px;'
        f'display:flex;gap:2px;border-bottom:2px solid #0f1f3a;flex-wrap:wrap">'
        f'{links}</nav>'
    )

def fix_encoding(raw):
    # Corrigir double-encoding PT-BR
    try:
        text = raw.decode('utf-8')
        FIX = [
            ('Ã­','í'),('Ã§','ç'),('Ã£','ã'),('Ã¡','á'),('Ã©','é'),
            ('Ã³','ó'),('Ãº','ú'),('Ã¢','â'),('Ãª','ê'),('Ã´','ô'),
            ('Ã‡','Ç'),('Ã‰','É'),('Ã"','Ó'),('Ãš','Ú'),('Ã ','à'),
        ]
        for bad, good in FIX:
            text = text.replace(bad, good)
        return text
    except:
        return raw.decode('latin-1', errors='replace')

modified = 0
for fname, label in PAGES:
    if not os.path.exists(fname):
        print(f'  SKIP {fname} (nao encontrado)')
        continue

    with open(fname, 'rb') as f:
        raw = f.read()

    html = fix_encoding(raw)

    # Remover nav antiga se existir
    if 'nav-fava' in html or ('nav style="background:#162040' in html):
        # Remover nav antiga
        import re
        html = re.sub(r'\n?<nav[^>]*background:#162040[^>]*>.*?</nav>', '', html, flags=re.DOTALL)

    # Adicionar nav nova
    nav = build_nav(label)
    if '</header>' in html:
        html = html.replace('</header>', '</header>' + nav, 1)
    elif '<body>' in html:
        html = html.replace('<body>', '<body>' + nav, 1)
    else:
        html = nav + html

    with open(fname, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f'  OK {fname} ({label} ativo)')
    modified += 1

print(f'\nConcluido: {modified} arquivos atualizados.')
print('Suba todos no GitHub em: github.com/FavaEcom/fava-ecom/upload/main')
