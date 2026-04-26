import base64
import json
import math
import os
import re
import shutil
import sqlite3
import time
import unicodedata
import zipfile
from datetime import datetime
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get('DATA_DIR', BASE_DIR))
DB_PATH = Path(os.environ.get('DB_PATH', str(DATA_DIR / 'consumo_chapas.db')))
SEED_DB_PATH = BASE_DIR / 'consumo_chapas.db'
EXCEL_PATH = BASE_DIR / 'Consumo de chapa por lote.xlsx'
RESULTADOS_XLSX_PATH = Path(os.environ.get('RESULTADOS_XLSX_PATH', str(DATA_DIR / 'historico_calculos_chapas.xlsx')))
PORTA_PADRAO = 8000
APP_USER = os.environ.get('APP_USER', 'admin')
APP_PASSWORD = os.environ.get('APP_PASSWORD', 'troque-esta-senha')
AUTH_ENABLED = os.environ.get('AUTH_ENABLED', '1') != '0'


def normalizar(texto):
    texto = '' if texto is None else str(texto)
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'\s+', ' ', texto.upper()).strip()


def numero(valor, padrao=0.0):
    if valor is None:
        return padrao
    if isinstance(valor, (int, float)):
        return float(valor)
    texto = str(valor).strip()
    if not texto:
        return padrao
    if ',' in texto:
        texto = texto.replace('.', '').replace(',', '.')
    try:
        return float(texto)
    except ValueError:
        return padrao


def fmt_num(valor, casas=2):
    try:
        v = float(valor)
    except (TypeError, ValueError):
        return '0'
    if abs(v - round(v)) < 0.0000001:
        return f'{int(round(v)):,}'.replace(',', '.')
    return f'{v:,.{casas}f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def fmt_m2(valor):
    return fmt_num(valor, 3)


def eh_tipo_calculavel(tipo):
    if not tipo:
        return False
    return normalizar(tipo) not in {'SEM MEDIDA', 'NAO INFORMADO', '0'}


def garantir_tabelas_historico():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS historico_calculos (
            id TEXT PRIMARY KEY,
            data_hora TEXT NOT NULL,
            produtos_totais REAL NOT NULL DEFAULT 0,
            produtos_diferentes INTEGER NOT NULL DEFAULT 0,
            linhas_produto_lote INTEGER NOT NULL DEFAULT 0,
            pecas_totais REAL NOT NULL DEFAULT 0,
            m2_total REAL NOT NULL DEFAULT 0,
            chapas_total INTEGER NOT NULL DEFAULT 0,
            itens_sem_medida INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS historico_produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calculo_id TEXT NOT NULL,
            data_hora TEXT NOT NULL,
            lote TEXT,
            produto TEXT,
            quantidade REAL,
            pecas_unitarias REAL,
            m2_unitario REAL,
            m2_lote REAL,
            tipos TEXT
        );

        CREATE TABLE IF NOT EXISTS historico_chapas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calculo_id TEXT NOT NULL,
            data_hora TEXT NOT NULL,
            tipo_chapa TEXT,
            medida_chapa TEXT,
            qtde_pecas_lote REAL,
            m2_cortado REAL,
            aproveitamento REAL,
            m2_util_chapa REAL,
            chapas_usadas INTEGER
        );

        CREATE TABLE IF NOT EXISTS historico_detalhes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calculo_id TEXT NOT NULL,
            data_hora TEXT NOT NULL,
            lote TEXT,
            produto TEXT,
            codigo TEXT,
            peca TEXT,
            tipo_chapa TEXT,
            comprimento REAL,
            largura REAL,
            espessura REAL,
            qtde_peca_produto REAL,
            qtde_produto REAL,
            qtde_pecas_lote REAL,
            m2_lote REAL
        );

        CREATE INDEX IF NOT EXISTS idx_hist_produtos_calc ON historico_produtos(calculo_id);
        CREATE INDEX IF NOT EXISTS idx_hist_chapas_calc ON historico_chapas(calculo_id);
        CREATE INDEX IF NOT EXISTS idx_hist_detalhes_calc ON historico_detalhes(calculo_id);
        """)
        con.commit()
    finally:
        con.close()


def garantir_banco():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not DB_PATH.exists():
        if SEED_DB_PATH.exists() and SEED_DB_PATH.resolve() != DB_PATH.resolve():
            shutil.copy2(SEED_DB_PATH, DB_PATH)
        elif EXCEL_PATH.exists():
            from importar_excel import importar
            importar(EXCEL_PATH, DB_PATH)
        else:
            raise FileNotFoundError('Não encontrei consumo_chapas.db nem a planilha original para importar.')
    garantir_tabelas_historico()


def conectar():
    garantir_banco()
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def buscar_produtos():
    with conectar() as con:
        return [r['nome'] for r in con.execute('SELECT nome FROM produtos ORDER BY nome')]


def obter_produto(con, nome):
    return con.execute('SELECT * FROM produtos WHERE nome_norm = ?', (normalizar(nome),)).fetchone()


def obter_pecas(con, nome):
    return con.execute('SELECT * FROM pecas WHERE produto_norm = ? ORDER BY id', (normalizar(nome),)).fetchall()


def calcular_lote(itens):
    entradas = {}
    avisos = []
    for item in itens:
        produto = (item.get('produto') or '').strip()
        lote = (item.get('lote') or '').strip()
        qtd = numero(item.get('quantidade'))
        if not produto and qtd == 0 and not lote:
            continue
        if not produto:
            avisos.append('Existe uma linha com lote ou quantidade, mas sem produto informado.')
            continue
        if not lote:
            lote = 'SEM LOTE'
            avisos.append(f'Produto {produto}: número do lote não informado. O sistema salvou como SEM LOTE.')
        if qtd <= 0:
            avisos.append(f'Produto {produto} / lote {lote}: a quantidade precisa ser maior que zero.')
            continue
        chave_produto = normalizar(produto)
        chave_lote = normalizar(lote)
        chave = f'{chave_produto}||{chave_lote}'
        if chave not in entradas:
            entradas[chave] = {'produto_digitado': produto, 'produto_norm': chave_produto, 'lote': lote, 'quantidade': 0.0}
        entradas[chave]['quantidade'] += qtd

    produtos_resumo = []
    detalhes = []
    resumo_tipos = {}
    sem_medida = []

    with conectar() as con:
        for entrada in entradas.values():
            produto_db = con.execute('SELECT * FROM produtos WHERE nome_norm = ?', (entrada['produto_norm'],)).fetchone()
            if not produto_db:
                avisos.append(f'Produto não encontrado no banco: {entrada["produto_digitado"]} / lote {entrada["lote"]}')
                continue
            pecas = con.execute('SELECT * FROM pecas WHERE produto_norm = ? ORDER BY id', (entrada['produto_norm'],)).fetchall()
            if not pecas:
                avisos.append(f'Produto sem peças cadastradas: {produto_db["nome"]} / lote {entrada["lote"]}')
                continue

            qtd_produto = entrada['quantidade']
            lote = entrada['lote']
            pecas_unitarias = sum(float(p['qtde_peca_produto'] or 0) for p in pecas)
            m2_unitario = sum(float(p['qtde_peca_produto'] or 0) * float(p['m2_peca_unit'] or 0) for p in pecas)
            if produto_db['pecas_unitarias']:
                pecas_unitarias = float(produto_db['pecas_unitarias'])
            if produto_db['m2_unitario']:
                m2_unitario = float(produto_db['m2_unitario'])

            produtos_resumo.append({
                'lote': lote,
                'produto': produto_db['nome'],
                'quantidade': qtd_produto,
                'pecas_unitarias': pecas_unitarias,
                'm2_unitario': m2_unitario,
                'm2_lote': qtd_produto * m2_unitario,
                'tipos': produto_db['tipos_chapa_usados'] or ''
            })

            for p in pecas:
                qtde_pecas_lote = qtd_produto * float(p['qtde_peca_produto'] or 0)
                m2_lote = qtde_pecas_lote * float(p['m2_peca_unit'] or 0)
                linha = {
                    'lote': lote,
                    'produto': produto_db['nome'],
                    'codigo': p['codigo'] or '',
                    'peca': p['peca'] or '',
                    'produto_original': p['produto_original'] or '',
                    'comprimento': float(p['comprimento'] or 0),
                    'largura': float(p['largura'] or 0),
                    'espessura': float(p['espessura'] or 0),
                    'material': p['material'] or '',
                    'tipo_chapa': p['tipo_chapa'] or '',
                    'qtde_peca_produto': float(p['qtde_peca_produto'] or 0),
                    'qtde_produto': qtd_produto,
                    'qtde_pecas_lote': qtde_pecas_lote,
                    'm2_lote': m2_lote
                }
                detalhes.append(linha)

                tipo = linha['tipo_chapa']
                if not eh_tipo_calculavel(tipo):
                    if qtde_pecas_lote > 0:
                        sem_medida.append(linha)
                    continue
                if tipo not in resumo_tipos:
                    cfg = con.execute('SELECT * FROM chapas_config WHERE tipo_chapa = ?', (tipo,)).fetchone()
                    resumo_tipos[tipo] = {
                        'tipo_chapa': tipo,
                        'qtde_pecas_lote': 0.0,
                        'm2_cortado': 0.0,
                        'comprimento_chapa': float(cfg['comprimento']) if cfg else 2.75,
                        'largura_chapa': float(cfg['largura']) if cfg else 1.85,
                        'aproveitamento': float(cfg['aproveitamento']) if cfg else 0.95
                    }
                resumo_tipos[tipo]['qtde_pecas_lote'] += qtde_pecas_lote
                resumo_tipos[tipo]['m2_cortado'] += m2_lote

    resumo = []
    for tipo, dados in resumo_tipos.items():
        m2_util = dados['comprimento_chapa'] * dados['largura_chapa'] * dados['aproveitamento']
        chapas = math.ceil(dados['m2_cortado'] / m2_util) if dados['m2_cortado'] > 0 and m2_util > 0 else 0
        dados['m2_util_chapa'] = m2_util
        dados['chapas_usadas'] = chapas
        dados['medida_chapa'] = f'{dados["comprimento_chapa"]:.2f} x {dados["largura_chapa"]:.2f} m'.replace('.', ',')
        resumo.append(dados)

    resumo.sort(key=lambda x: (-x['chapas_usadas'], x['tipo_chapa']))
    produtos_resumo.sort(key=lambda x: (x.get('lote', ''), x['produto']))
    detalhes.sort(key=lambda x: (x.get('lote', ''), x['produto'], x['tipo_chapa'], x['peca']))

    totais = {
        'produtos_totais': sum(p['quantidade'] for p in produtos_resumo),
        'produtos_diferentes': len(set(normalizar(p['produto']) for p in produtos_resumo)),
        'linhas_produto_lote': len(produtos_resumo),
        'pecas_totais': sum(d['qtde_pecas_lote'] for d in detalhes),
        'm2_total': sum(r['m2_cortado'] for r in resumo),
        'chapas_total': sum(r['chapas_usadas'] for r in resumo),
        'itens_sem_medida': len(sem_medida)
    }

    return {
        'entradas': list(entradas.values()),
        'produtos': produtos_resumo,
        'resumo': resumo,
        'detalhes': detalhes,
        'sem_medida': sem_medida,
        'totais': totais,
        'avisos': avisos,
        'excel_salvo': False
    }


def col_excel(numero_coluna):
    letras = ''
    while numero_coluna:
        numero_coluna, resto = divmod(numero_coluna - 1, 26)
        letras = chr(65 + resto) + letras
    return letras


def xml_texto(valor):
    texto = '' if valor is None else str(valor)
    return escape(texto, quote=False).replace('\n', '&#10;')


def xlsx_celula(linha, coluna, valor, estilo=0):
    ref = f'{col_excel(coluna)}{linha}'
    style = f' s="{estilo}"' if estilo else ''
    if isinstance(valor, bool):
        return f'<c r="{ref}" t="b"{style}><v>{1 if valor else 0}</v></c>'
    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        return f'<c r="{ref}"{style}><v>{valor}</v></c>'
    return f'<c r="{ref}" t="inlineStr"{style}><is><t>{xml_texto(valor)}</t></is></c>'


def xlsx_sheet_xml(linhas):
    linhas_xml = []
    for i, linha in enumerate(linhas, start=1):
        estilo = 1 if i == 1 else 0
        celulas = ''.join(xlsx_celula(i, j, valor, estilo) for j, valor in enumerate(linha, start=1))
        linhas_xml.append(f'<row r="{i}">{celulas}</row>')
    max_col = max((len(l) for l in linhas), default=1)
    dim = f'A1:{col_excel(max_col)}{max(len(linhas), 1)}'
    colunas = ''.join(f'<col min="{i}" max="{i}" width="{22 if i <= 4 else 16}" customWidth="1"/>' for i in range(1, max_col + 1))
    filtros = f'<autoFilter ref="A1:{col_excel(max_col)}1"/>' if linhas else ''
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><dimension ref="{dim}"/><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><cols>{colunas}</cols><sheetData>{''.join(linhas_xml)}</sheetData>{filtros}</worksheet>'''


def criar_xlsx_simples(caminho, abas):
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(caminho, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/><Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>''' + ''.join(f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' for i in range(1, len(abas) + 1)) + '</Types>')
        z.writestr('_rels/.rels', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/></Relationships>''')
        sheets_xml = ''.join(f'<sheet name="{xml_texto(nome[:31])}" sheetId="{i}" r:id="rId{i}"/>' for i, (nome, _) in enumerate(abas, start=1))
        z.writestr('xl/workbook.xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>{sheets_xml}</sheets></workbook>''')
        rels = ''.join(f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>' for i in range(1, len(abas) + 1))
        rels += f'<Relationship Id="rId{len(abas) + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        z.writestr('xl/_rels/workbook.xml.rels', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{rels}</Relationships>''')
        z.writestr('xl/styles.xml', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="2"><font><sz val="10"/><name val="Arial"/></font><font><b/><sz val="10"/><name val="Arial"/><color rgb="FFFFFFFF"/></font></fonts><fills count="3"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF2563EB"/><bgColor indexed="64"/></patternFill></fill></fills><borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>''')
        agora = datetime.now().isoformat(timespec='seconds')
        z.writestr('docProps/core.xml', f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:title>Histórico de cálculos de chapas</dc:title><dc:creator>App Consumo de Chapas</dc:creator><cp:lastModifiedBy>App Consumo de Chapas</cp:lastModifiedBy><dcterms:created xsi:type="dcterms:W3CDTF">{agora}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{agora}</dcterms:modified></cp:coreProperties>''')
        z.writestr('docProps/app.xml', '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>Python</Application></Properties>''')
        for i, (_, linhas) in enumerate(abas, start=1):
            z.writestr(f'xl/worksheets/sheet{i}.xml', xlsx_sheet_xml(linhas))


def gerar_excel_historico():
    garantir_tabelas_historico()
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        lotes = [['ID cálculo', 'Data/hora', 'Produtos totais', 'Produtos diferentes', 'Itens produto/lote', 'Peças totais', 'm² total', 'Chapas total', 'Itens sem medida']]
        for r in con.execute('SELECT * FROM historico_calculos ORDER BY data_hora, id'):
            lotes.append([r['id'], r['data_hora'], r['produtos_totais'], r['produtos_diferentes'], r['linhas_produto_lote'], r['pecas_totais'], r['m2_total'], r['chapas_total'], r['itens_sem_medida']])
        produtos = [['ID cálculo', 'Data/hora', 'Lote', 'Produto', 'Quantidade', 'Peças unit.', 'm² unit.', 'm² lote', 'Tipos usados']]
        for r in con.execute('SELECT * FROM historico_produtos ORDER BY data_hora, calculo_id, lote, produto'):
            produtos.append([r['calculo_id'], r['data_hora'], r['lote'], r['produto'], r['quantidade'], r['pecas_unitarias'], r['m2_unitario'], r['m2_lote'], r['tipos']])
        chapas = [['ID cálculo', 'Data/hora', 'Tipo chapa', 'Medida chapa', 'Qtde peças lote', 'm² cortado', 'Aproveitamento', 'm² útil/chapa', 'Chapas usadas']]
        for r in con.execute('SELECT * FROM historico_chapas ORDER BY data_hora, calculo_id, tipo_chapa'):
            chapas.append([r['calculo_id'], r['data_hora'], r['tipo_chapa'], r['medida_chapa'], r['qtde_pecas_lote'], r['m2_cortado'], r['aproveitamento'], r['m2_util_chapa'], r['chapas_usadas']])
        detalhes = [['ID cálculo', 'Data/hora', 'Lote', 'Produto', 'Código', 'Peça', 'Tipo chapa', 'Comprimento', 'Largura', 'Espessura', 'Qtde peça/prod.', 'Qtde produto', 'Qtde peças lote', 'm² lote']]
        for r in con.execute('SELECT * FROM historico_detalhes ORDER BY data_hora, calculo_id, lote, produto, tipo_chapa, peca'):
            detalhes.append([r['calculo_id'], r['data_hora'], r['lote'], r['produto'], r['codigo'], r['peca'], r['tipo_chapa'], r['comprimento'], r['largura'], r['espessura'], r['qtde_peca_produto'], r['qtde_produto'], r['qtde_pecas_lote'], r['m2_lote']])
    criar_xlsx_simples(RESULTADOS_XLSX_PATH, [('LOTES_CALCULADOS', lotes), ('PRODUTOS', produtos), ('CHAPAS', chapas), ('DETALHES_PECAS', detalhes)])
    return RESULTADOS_XLSX_PATH


def salvar_calculo_excel(resultado):
    if not resultado.get('produtos'):
        return None
    garantir_tabelas_historico()
    data_hora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    calculo_id = datetime.now().strftime('%Y%m%d%H%M%S%f')
    t = resultado['totais']
    with sqlite3.connect(DB_PATH) as con:
        con.execute('''INSERT INTO historico_calculos (id, data_hora, produtos_totais, produtos_diferentes, linhas_produto_lote, pecas_totais, m2_total, chapas_total, itens_sem_medida) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (calculo_id, data_hora, t['produtos_totais'], t['produtos_diferentes'], t['linhas_produto_lote'], t['pecas_totais'], t['m2_total'], t['chapas_total'], t['itens_sem_medida']))
        for p in resultado['produtos']:
            con.execute('''INSERT INTO historico_produtos (calculo_id, data_hora, lote, produto, quantidade, pecas_unitarias, m2_unitario, m2_lote, tipos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (calculo_id, data_hora, p.get('lote', ''), p['produto'], p['quantidade'], p['pecas_unitarias'], p['m2_unitario'], p['m2_lote'], p['tipos']))
        for r in resultado['resumo']:
            con.execute('''INSERT INTO historico_chapas (calculo_id, data_hora, tipo_chapa, medida_chapa, qtde_pecas_lote, m2_cortado, aproveitamento, m2_util_chapa, chapas_usadas) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (calculo_id, data_hora, r['tipo_chapa'], r['medida_chapa'], r['qtde_pecas_lote'], r['m2_cortado'], r['aproveitamento'], r['m2_util_chapa'], r['chapas_usadas']))
        for d in resultado['detalhes']:
            con.execute('''INSERT INTO historico_detalhes (calculo_id, data_hora, lote, produto, codigo, peca, tipo_chapa, comprimento, largura, espessura, qtde_peca_produto, qtde_produto, qtde_pecas_lote, m2_lote) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (calculo_id, data_hora, d.get('lote', ''), d['produto'], d['codigo'], d['peca'], d['tipo_chapa'], d['comprimento'], d['largura'], d['espessura'], d['qtde_peca_produto'], d['qtde_produto'], d['qtde_pecas_lote'], d['m2_lote']))
        con.commit()
    return gerar_excel_historico()


STYLE = r'''
:root{--bg:#f5f7fb;--card:#fff;--text:#1f2937;--muted:#64748b;--line:#e5e7eb;--primary:#2563eb;--primary-dark:#1d4ed8;--soft:#eff6ff;--warn:#fff7ed;--warn-text:#9a3412;--ok:#ecfdf5;--ok-text:#047857}*{box-sizing:border-box}body{margin:0;font-family:Arial,Helvetica,sans-serif;background:var(--bg);color:var(--text)}a{color:var(--primary);text-decoration:none}.wrap{max-width:1200px;margin:0 auto;padding:24px}.top{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:20px}.brand h1{margin:0;font-size:26px}.brand p{margin:6px 0 0;color:var(--muted)}.nav{display:flex;gap:8px;flex-wrap:wrap}.nav a,.btn{display:inline-block;border:0;border-radius:10px;padding:10px 14px;background:var(--primary);color:#fff;font-weight:700;cursor:pointer}.nav a{background:#fff;color:var(--primary);border:1px solid var(--line)}.btn:hover{background:var(--primary-dark)}.btn.secondary{background:#fff;color:var(--primary);border:1px solid var(--line)}.btn.danger{background:#dc2626}.card{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:18px;box-shadow:0 8px 24px rgba(15,23,42,.05);margin-bottom:18px}.grid{display:grid;grid-template-columns:repeat(5,minmax(150px,1fr));gap:14px}.kpi{background:var(--soft);border-radius:16px;padding:14px}.kpi span{display:block;color:var(--muted);font-size:13px}.kpi strong{display:block;font-size:24px;margin-top:4px}.table-wrap{overflow:auto;border-radius:14px;border:1px solid var(--line)}table{width:100%;border-collapse:collapse;background:#fff}th,td{padding:10px 12px;border-bottom:1px solid var(--line);text-align:left;font-size:14px;vertical-align:top}th{background:#f8fafc;color:#334155;position:sticky;top:0;z-index:1}tr:last-child td{border-bottom:0}.num{text-align:right;white-space:nowrap}.muted{color:var(--muted)}.notice{background:var(--warn);color:var(--warn-text);border:1px solid #fed7aa;border-radius:14px;padding:12px;margin-bottom:14px}.ok{background:var(--ok);color:var(--ok-text);border:1px solid #bbf7d0;border-radius:14px;padding:12px;margin-bottom:14px}.form-row{display:grid;grid-template-columns:160px 1fr 160px 44px;gap:10px;margin-bottom:10px}.form-row input,.form-row select,.field{width:100%;border:1px solid var(--line);border-radius:10px;padding:10px;font-size:15px;background:#fff}.mini{font-size:12px;color:var(--muted)}.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}.pill{display:inline-block;background:#eef2ff;color:#3730a3;border-radius:999px;padding:4px 9px;font-size:12px;margin:2px}.search{display:grid;grid-template-columns:1fr auto;gap:10px;margin-bottom:14px}.footer{color:var(--muted);font-size:12px;text-align:center;margin-top:24px}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.form-row{grid-template-columns:1fr}.top{display:block}.nav{margin-top:14px}}@media print{.nav,.actions,.remove,.search{display:none}.wrap{max-width:none}.card{box-shadow:none}}
'''


def layout(titulo, corpo):
    return f'''<!doctype html><html lang="pt-br"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(titulo)}</title><link rel="stylesheet" href="/static/style.css"></head><body><div class="wrap"><div class="top"><div class="brand"><h1>{escape(titulo)}</h1><p>Consumo de chapas por lote com histórico automático em Excel.</p></div><div class="nav"><a href="/">Calcular lote</a><a href="/banco">Banco de dados</a><a href="/chapas">Configurar chapas</a><a href="/baixar_excel">Baixar histórico Excel</a></div></div>{corpo}<div class="footer">Software em Python. Cálculo: quantidade do produto × ficha técnica da peça × m² útil da chapa.</div></div></body></html>'''


def tabela_resumo_chapas(resumo):
    if not resumo:
        return '<p class="muted">Nenhum tipo de chapa calculável encontrado para este lote.</p>'
    linhas = []
    for r in resumo:
        linhas.append(f'''<tr><td>{escape(r['tipo_chapa'])}</td><td>{escape(r['medida_chapa'])}</td><td class="num">{fmt_num(r['qtde_pecas_lote'])}</td><td class="num">{fmt_m2(r['m2_cortado'])}</td><td class="num">{fmt_num(r['aproveitamento'] * 100, 1)}%</td><td class="num">{fmt_m2(r['m2_util_chapa'])}</td><td class="num"><strong>{fmt_num(r['chapas_usadas'])}</strong></td></tr>''')
    return '<div class="table-wrap"><table><thead><tr><th>Tipo chapa</th><th>Medida chapa</th><th class="num">Qtde peças lote</th><th class="num">m² cortado</th><th class="num">Aproveit.</th><th class="num">m² útil/chapa</th><th class="num">Chapas usadas</th></tr></thead><tbody>' + ''.join(linhas) + '</tbody></table></div>'


def tabela_produtos(produtos):
    if not produtos:
        return '<p class="muted">Nenhum produto calculado.</p>'
    linhas = []
    for p in produtos:
        tipos = ''.join(f'<span class="pill">{escape(t.strip())}</span>' for t in (p['tipos'] or '').split(',') if t.strip())
        linhas.append(f'''<tr><td>{escape(p.get('lote', ''))}</td><td>{escape(p['produto'])}</td><td class="num">{fmt_num(p['quantidade'])}</td><td class="num">{fmt_num(p['pecas_unitarias'])}</td><td class="num">{fmt_m2(p['m2_unitario'])}</td><td class="num">{fmt_m2(p['m2_lote'])}</td><td>{tipos}</td></tr>''')
    return '<div class="table-wrap"><table><thead><tr><th>Lote</th><th>Produto</th><th class="num">Qtde produtos</th><th class="num">Peças unit.</th><th class="num">m² unit.</th><th class="num">m² lote</th><th>Tipos usados</th></tr></thead><tbody>' + ''.join(linhas) + '</tbody></table></div>'


def tabela_detalhes(detalhes):
    if not detalhes:
        return '<p class="muted">Nenhuma peça calculada.</p>'
    linhas = []
    for d in detalhes:
        linhas.append(f'''<tr><td>{escape(d.get('lote', ''))}</td><td>{escape(d['produto'])}</td><td>{escape(d['codigo'])}</td><td>{escape(d['peca'])}</td><td>{escape(d['tipo_chapa'])}</td><td class="num">{fmt_num(d['comprimento'], 3)}</td><td class="num">{fmt_num(d['largura'], 3)}</td><td class="num">{fmt_num(d['espessura'], 1)}</td><td class="num">{fmt_num(d['qtde_peca_produto'])}</td><td class="num">{fmt_num(d['qtde_pecas_lote'])}</td><td class="num">{fmt_m2(d['m2_lote'])}</td></tr>''')
    return '<div class="table-wrap"><table><thead><tr><th>Lote</th><th>Produto</th><th>Código</th><th>Peça</th><th>Tipo chapa</th><th class="num">Comp.</th><th class="num">Larg.</th><th class="num">Esp.</th><th class="num">Qtde peça/prod.</th><th class="num">Qtde peças lote</th><th class="num">m² lote</th></tr></thead><tbody>' + ''.join(linhas) + '</tbody></table></div>'


def render_inicio(resultado=None, entradas=None):
    produtos = buscar_produtos()
    datalist = ''.join(f'<option value="{escape(p)}"></option>' for p in produtos)
    if entradas is None:
        entradas = [{'lote': '', 'produto_digitado': '', 'quantidade': ''} for _ in range(4)]
    rows = []
    for e in entradas:
        rows.append(f'''<div class="form-row"><input name="lote" placeholder="Nº do lote" value="{escape(str(e.get('lote', '')))}"><input name="produto" list="produtos-list" placeholder="Digite ou selecione o produto" value="{escape(str(e.get('produto_digitado', '')))}"><input name="quantidade" type="text" inputmode="decimal" placeholder="Quantidade" value="{escape(str(e.get('quantidade', '')))}"><button type="button" class="btn secondary remove" onclick="removerLinha(this)">×</button></div>''')
    resultado_html = ''
    if resultado:
        avisos = ''.join(f'<div class="notice">{escape(a)}</div>' for a in resultado['avisos'])
        if resultado['produtos']:
            t = resultado['totais']
            excel = '<div class="ok">Cálculo salvo automaticamente no Excel de histórico. <a href="/baixar_excel">Clique aqui para baixar o arquivo Excel atualizado.</a></div>' if resultado.get('excel_salvo') else ''
            sem = f'<div class="notice">{t["itens_sem_medida"]} item(ns) têm tipo sem medida ou não calculável. Eles aparecem no detalhamento, mas não geram chapas.</div>' if t['itens_sem_medida'] else ''
            resultado_html = f'''{avisos}{excel}{sem}<div class="card"><div class="grid"><div class="kpi"><span>Produtos totais</span><strong>{fmt_num(t['produtos_totais'])}</strong></div><div class="kpi"><span>Produtos diferentes</span><strong>{fmt_num(t['produtos_diferentes'])}</strong></div><div class="kpi"><span>Itens produto/lote</span><strong>{fmt_num(t['linhas_produto_lote'])}</strong></div><div class="kpi"><span>m² total cortado</span><strong>{fmt_m2(t['m2_total'])}</strong></div><div class="kpi"><span>Chapas totais</span><strong>{fmt_num(t['chapas_total'])}</strong></div></div></div><div class="card"><h2>Chapas utilizadas por tipo</h2>{tabela_resumo_chapas(resultado['resumo'])}</div><div class="card"><h2>Resumo por produto e lote</h2>{tabela_produtos(resultado['produtos'])}</div><div class="card"><h2>Detalhamento das peças</h2>{tabela_detalhes(resultado['detalhes'])}</div>'''
        else:
            resultado_html = avisos or '<div class="notice">Informe pelo menos um produto válido para calcular.</div>'
    corpo = f'''<div class="card"><h2>Entrada do lote</h2><p class="muted">Informe o número do lote, o produto agregado/completo e a quantidade que será produzida. Ao calcular, o sistema salva automaticamente o resultado no Excel de histórico.</p><form method="post" action="/calcular" id="form-lote"><datalist id="produtos-list">{datalist}</datalist><div id="linhas">{''.join(rows)}</div><div class="actions"><button type="button" class="btn secondary" onclick="adicionarLinha()">+ Adicionar produto</button><button type="submit" class="btn">Calcular e salvar no Excel</button><a class="btn secondary" href="/baixar_excel">Baixar histórico Excel</a><button type="button" class="btn secondary" onclick="window.print()">Imprimir / salvar PDF</button></div></form></div>{resultado_html}<script>function adicionarLinha(){{const div=document.createElement('div');div.className='form-row';div.innerHTML='<input name="lote" placeholder="Nº do lote"><input name="produto" list="produtos-list" placeholder="Digite ou selecione o produto"><input name="quantidade" type="text" inputmode="decimal" placeholder="Quantidade"><button type="button" class="btn secondary remove" onclick="removerLinha(this)">×</button>';document.getElementById('linhas').appendChild(div);}}function removerLinha(btn){{const linhas=document.querySelectorAll('.form-row');if(linhas.length>1)btn.parentElement.remove();}}</script>'''
    return layout('Consumo de chapas por lote', corpo)


def render_banco(query=''):
    qnorm = f'%{normalizar(query)}%'
    with conectar() as con:
        stats = {'produtos': con.execute('SELECT COUNT(*) FROM produtos').fetchone()[0], 'pecas': con.execute('SELECT COUNT(*) FROM pecas').fetchone()[0], 'chapas': con.execute('SELECT COUNT(*) FROM chapas_config').fetchone()[0]}
        produtos = con.execute('SELECT * FROM produtos WHERE nome_norm LIKE ? ORDER BY nome LIMIT 200', (qnorm,)).fetchall() if query else con.execute('SELECT * FROM produtos ORDER BY nome LIMIT 200').fetchall()
    linhas = []
    for p in produtos:
        linhas.append(f'''<tr><td><a href="/pecas?produto={escape(p['nome'])}">{escape(p['nome'])}</a></td><td class="num">{fmt_num(p['pecas_unitarias'])}</td><td class="num">{fmt_m2(p['m2_unitario'])}</td><td>{escape(p['tipos_chapa_usados'] or '')}</td></tr>''')
    corpo = f'''<div class="card"><div class="grid"><div class="kpi"><span>Produtos cadastrados</span><strong>{stats['produtos']}</strong></div><div class="kpi"><span>Peças cadastradas</span><strong>{stats['pecas']}</strong></div><div class="kpi"><span>Tipos de chapa</span><strong>{stats['chapas']}</strong></div><div class="kpi"><span>Banco</span><strong>SQLite</strong></div><div class="kpi"><span>Histórico</span><strong>Excel</strong></div></div></div><div class="card"><h2>Produtos do banco</h2><form class="search" method="get" action="/banco"><input class="field" name="q" placeholder="Buscar produto" value="{escape(query)}"><button class="btn" type="submit">Buscar</button></form><div class="table-wrap"><table><thead><tr><th>Produto</th><th class="num">Peças unit.</th><th class="num">m² unit.</th><th>Tipos de chapa usados</th></tr></thead><tbody>{''.join(linhas)}</tbody></table></div><p class="mini">Mostrando até 200 registros. Clique no nome do produto para ver a ficha de peças.</p></div>'''
    return layout('Banco de dados', corpo)


def render_pecas(produto):
    with conectar() as con:
        prod = obter_produto(con, produto)
        pecas = obter_pecas(con, produto)
    if not prod:
        return layout('Peças do produto', f'<div class="notice">Produto não encontrado: {escape(produto)}</div>')
    linhas = []
    for p in pecas:
        linhas.append(f'''<tr><td>{escape(p['codigo'] or '')}</td><td>{escape(p['peca'] or '')}</td><td>{escape(p['tipo_chapa'] or '')}</td><td class="num">{fmt_num(p['comprimento'], 3)}</td><td class="num">{fmt_num(p['largura'], 3)}</td><td class="num">{fmt_num(p['espessura'], 1)}</td><td class="num">{fmt_num(p['qtde_peca_produto'])}</td><td class="num">{fmt_m2(p['m2_peca_unit'])}</td></tr>''')
    corpo = f'''<div class="card"><h2>{escape(prod['nome'])}</h2><p class="muted">Peças unitárias: <strong>{fmt_num(prod['pecas_unitarias'])}</strong> · m² unitário: <strong>{fmt_m2(prod['m2_unitario'])}</strong></p><div class="table-wrap"><table><thead><tr><th>Código</th><th>Peça</th><th>Tipo chapa</th><th class="num">Comp.</th><th class="num">Larg.</th><th class="num">Esp.</th><th class="num">Qtde/prod.</th><th class="num">m² unit.</th></tr></thead><tbody>{''.join(linhas)}</tbody></table></div></div>'''
    return layout('Peças do produto', corpo)


def render_chapas(mensagem=''):
    with conectar() as con:
        chapas = con.execute('SELECT * FROM chapas_config ORDER BY tipo_chapa').fetchall()
    linhas = []
    for c in chapas:
        tipo_id = escape(c['tipo_chapa'])
        linhas.append(f'''<tr><td>{tipo_id}<input type="hidden" name="tipo" value="{tipo_id}"></td><td><input class="field" name="comprimento" value="{fmt_num(c['comprimento'], 3)}"></td><td><input class="field" name="largura" value="{fmt_num(c['largura'], 3)}"></td><td><input class="field" name="aproveitamento" value="{fmt_num(c['aproveitamento'], 3)}"></td><td class="num">{fmt_m2(float(c['comprimento']) * float(c['largura']) * float(c['aproveitamento']))}</td></tr>''')
    ok = f'<div class="ok">{escape(mensagem)}</div>' if mensagem else ''
    corpo = f'''{ok}<div class="card"><h2>Configuração das chapas</h2><p class="muted">Ajuste comprimento, largura e aproveitamento por tipo de chapa.</p><form method="post" action="/chapas"><div class="table-wrap"><table><thead><tr><th>Tipo chapa</th><th>Comp. chapa (m)</th><th>Larg. chapa (m)</th><th>Aproveitamento</th><th class="num">m² útil/chapa</th></tr></thead><tbody>{''.join(linhas)}</tbody></table></div><div class="actions"><button class="btn" type="submit">Salvar configurações</button></div></form></div>'''
    return layout('Configurar chapas', corpo)


def atualizar_chapas(campos):
    tipos = campos.get('tipo', [])
    comps = campos.get('comprimento', [])
    largs = campos.get('largura', [])
    aprovs = campos.get('aproveitamento', [])
    with conectar() as con:
        for i, tipo in enumerate(tipos):
            comp = numero(comps[i] if i < len(comps) else 2.75, 2.75)
            larg = numero(largs[i] if i < len(largs) else 1.85, 1.85)
            aprov = numero(aprovs[i] if i < len(aprovs) else 0.95, 0.95)
            if comp <= 0 or larg <= 0 or aprov <= 0:
                continue
            con.execute('UPDATE chapas_config SET comprimento=?, largura=?, aproveitamento=? WHERE tipo_chapa=?', (comp, larg, aprov, tipo))
        con.commit()


class App(BaseHTTPRequestHandler):
    def autorizado(self):
        if not AUTH_ENABLED:
            return True
        cabecalho = self.headers.get('Authorization', '')
        if not cabecalho.startswith('Basic '):
            return False
        try:
            usuario_senha = base64.b64decode(cabecalho.split(' ', 1)[1]).decode('utf-8')
            usuario, senha = usuario_senha.split(':', 1)
            return usuario == APP_USER and senha == APP_PASSWORD
        except Exception:
            return False

    def pedir_login(self):
        conteudo = layout('Login necessário', '<div class="notice">Acesso restrito. Informe usuário e senha.</div>').encode('utf-8')
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="Consumo de Chapas"')
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(conteudo)))
        self.end_headers()
        self.wfile.write(conteudo)

    def enviar(self, conteudo, status=200, content_type='text/html; charset=utf-8'):
        if isinstance(conteudo, str):
            conteudo = conteudo.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(conteudo)))
        self.end_headers()
        self.wfile.write(conteudo)

    def enviar_arquivo(self, caminho, nome_download):
        dados = Path(caminho).read_bytes()
        self.send_response(200)
        self.send_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        self.send_header('Content-Disposition', f'attachment; filename="{nome_download}"')
        self.send_header('Content-Length', str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def do_GET(self):
        rota = urlparse(self.path)
        if rota.path == '/healthz':
            self.enviar('ok', content_type='text/plain; charset=utf-8')
            return
        if not self.autorizado():
            self.pedir_login()
            return
        params = parse_qs(rota.query, keep_blank_values=True)
        try:
            if rota.path == '/static/style.css':
                self.enviar(STYLE, content_type='text/css; charset=utf-8')
            elif rota.path == '/':
                self.enviar(render_inicio())
            elif rota.path == '/banco':
                self.enviar(render_banco(params.get('q', [''])[0]))
            elif rota.path == '/pecas':
                self.enviar(render_pecas(params.get('produto', [''])[0]))
            elif rota.path == '/chapas':
                self.enviar(render_chapas())
            elif rota.path == '/baixar_excel':
                if not RESULTADOS_XLSX_PATH.exists():
                    gerar_excel_historico()
                self.enviar_arquivo(RESULTADOS_XLSX_PATH, 'historico_calculos_chapas.xlsx')
            elif rota.path == '/api/produtos':
                self.enviar(json.dumps(buscar_produtos(), ensure_ascii=False), content_type='application/json; charset=utf-8')
            else:
                self.enviar(layout('Página não encontrada', '<div class="notice">Rota não encontrada.</div>'), status=404)
        except Exception as e:
            self.enviar(layout('Erro', f'<div class="notice">{escape(str(e))}</div>'), status=500)

    def do_POST(self):
        if not self.autorizado():
            self.pedir_login()
            return
        tamanho = int(self.headers.get('Content-Length', 0))
        corpo = self.rfile.read(tamanho).decode('utf-8')
        campos = parse_qs(corpo, keep_blank_values=True)
        try:
            if self.path == '/calcular':
                lotes = campos.get('lote', [])
                produtos = campos.get('produto', [])
                quantidades = campos.get('quantidade', [])
                total_linhas = max(len(lotes), len(produtos), len(quantidades))
                itens = []
                for i in range(total_linhas):
                    itens.append({'lote': lotes[i] if i < len(lotes) else '', 'produto': produtos[i] if i < len(produtos) else '', 'quantidade': quantidades[i] if i < len(quantidades) else ''})
                resultado = calcular_lote(itens)
                if resultado.get('produtos'):
                    salvar_calculo_excel(resultado)
                    resultado['excel_salvo'] = True
                entradas = [{'lote': i.get('lote', ''), 'produto_digitado': i.get('produto', ''), 'quantidade': i.get('quantidade', '')} for i in itens]
                self.enviar(render_inicio(resultado, entradas))
            elif self.path == '/chapas':
                atualizar_chapas(campos)
                self.enviar(render_chapas('Configurações salvas com sucesso.'))
            else:
                self.enviar(layout('Página não encontrada', '<div class="notice">Rota não encontrada.</div>'), status=404)
        except Exception as e:
            self.enviar(layout('Erro', f'<div class="notice">{escape(str(e))}</div>'), status=500)

    def log_message(self, formato, *args):
        return


def main():
    garantir_banco()
    porta = int(os.environ.get('PORT', os.environ.get('PORTA', PORTA_PADRAO)))
    host = os.environ.get('HOST', '0.0.0.0')
    servidor = ThreadingHTTPServer((host, porta), App)
    print(f'Servidor iniciado em http://{host}:{porta}')
    print(f'Banco de dados em: {DB_PATH}')
    print(f'Histórico Excel em: {RESULTADOS_XLSX_PATH}')
    print('Login ativo. Configure APP_USER e APP_PASSWORD no servidor.')
    print('Pressione CTRL+C para encerrar.')
    servidor.serve_forever()


if __name__ == '__main__':
    main()
