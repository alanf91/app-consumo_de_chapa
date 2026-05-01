VERSÃO POR CÓDIGO DE PEÇA

Esta versão altera a lógica do sistema:
- O usuário informa o código da peça e a quantidade a cortar.
- A base é importada da planilha base_pecas.xlsx.
- A função Produção DOBUÊ foi removida.
- O sistema informa os códigos que não existem na base.
- Mantém cálculo de chapas por material, plano de corte por encaixe livre e guilhotina por faixas.
- Mantém histórico e exportação Excel.

ARQUIVOS NECESSÁRIOS NO GITHUB/RENDER:
- app.py
- base_pecas.xlsx
- requirements.txt
- render.yaml
- static/logos/dobue.png
- static/logos/grauna.png
- static/logos/simoni_valerio.png

VARIÁVEIS RECOMENDADAS NO RENDER:
AUTH_ENABLED=1
APP_USER=admin
APP_PASSWORD=sua_senha_forte
DATA_DIR=/var/data   (somente se usar Persistent Disk)
KERF_MM=4
META_APROVEITAMENTO=0.95
PERMITE_GIRAR_90=1
