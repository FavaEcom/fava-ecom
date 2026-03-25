# Fava Ecom — Painel de Gestão

## Como fazer o deploy no Railway

### 1. Estrutura de arquivos necessária
```
fava-ecom/
├── server.py
├── requirements.txt
├── Procfile
├── painel_fava.html
└── calculadora_fava.html
```

### 2. Subir no GitHub
1. Acesse github.com e crie uma conta
2. Clique em "New repository"
3. Nome: `fava-ecom`
4. Clique em "Create repository"
5. Arraste todos os arquivos para a página

### 3. Deploy no Railway
1. Acesse railway.app
2. Login with GitHub
3. "New Project" → "Deploy from GitHub repo"
4. Selecione o repositório `fava-ecom`
5. Railway detecta automaticamente e faz o deploy
6. Em 2 minutos gera um link tipo: `https://fava-ecom.up.railway.app`

### 4. Acessar
- Painel: `https://seu-link.up.railway.app/painel_fava.html`
- Calculadora: `https://seu-link.up.railway.app/calculadora_fava.html`

### Tokens
Os tokens ficam salvos no navegador (localStorage).
O token ML expira em 6h — rode o `autorizar_ml.py` no PC para renovar.
