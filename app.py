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

KERF_MM_PADRAO = float(os.environ.get('KERF_MM', '4'))
META_APROVEITAMENTO_PADRAO = float(os.environ.get('META_APROVEITAMENTO', '0.95'))
PERMITE_GIRAR_90_PADRAO = os.environ.get('PERMITE_GIRAR_90', '1') != '0'
MAX_PECAS_PLANO = int(os.environ.get('MAX_PECAS_PLANO', '12000'))


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




# ---------------------------------------------------------------------------
# MÓDULO DE PLANO DE CORTE REAL
# ---------------------------------------------------------------------------

CORES_PLANO = [
    '#dbeafe', '#dcfce7', '#fef3c7', '#fae8ff', '#fee2e2', '#e0f2fe',
    '#ede9fe', '#ccfbf1', '#ffedd5', '#fce7f3', '#ecfccb', '#e5e7eb'
]


def mm(valor_metros):
    return int(round(float(valor_metros or 0) * 1000))


def m2_de_mm2(valor_mm2):
    return float(valor_mm2 or 0) / 1000000.0


def fmt_mm(valor):
    try:
        return f'{int(round(float(valor)))}'
    except Exception:
        return '0'


def texto_curto(texto, limite=34):
    texto = '' if texto is None else str(texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto if len(texto) <= limite else texto[:limite - 1] + '…'


def cor_plano(indice):
    return CORES_PLANO[indice % len(CORES_PLANO)]


def explodir_pecas_para_plano(resultado):
    pecas_por_tipo = {}
    avisos = []
    total_expandido = 0

    for d in resultado.get('detalhes', []):
        tipo = d.get('tipo_chapa') or ''
        if not eh_tipo_calculavel(tipo):
            continue

        largura_mm = mm(d.get('largura'))
        comprimento_mm = mm(d.get('comprimento'))
        if largura_mm <= 0 or comprimento_mm <= 0:
            avisos.append(f'Peça ignorada por medida inválida: {d.get("produto", "")} - {d.get("peca", "")}')
            continue

        qtd_float = float(d.get('qtde_pecas_lote') or 0)
        qtd = int(round(qtd_float))
        if abs(qtd_float - qtd) > 0.001:
            qtd = int(math.ceil(qtd_float))
            avisos.append(
                f'Quantidade fracionada arredondada para cima no plano de corte: '
                f'{d.get("produto", "")} - {d.get("peca", "")}: {fmt_num(qtd_float)} -> {qtd}'
            )
        if qtd <= 0:
            continue

        if total_expandido + qtd > MAX_PECAS_PLANO:
            restante = MAX_PECAS_PLANO - total_expandido
            if restante <= 0:
                avisos.append(
                    f'O plano atingiu o limite de {MAX_PECAS_PLANO} peças expandidas. '
                    'Aumente MAX_PECAS_PLANO no servidor para lotes maiores.'
                )
                break
            avisos.append(
                f'Limite de peças atingido. A peça {d.get("peca", "")} foi limitada para {restante} unidade(s) no plano.'
            )
            qtd = restante

        for _ in range(qtd):
            total_expandido += 1
            peca = {
                'id': f'P{total_expandido:05d}',
                'lote': d.get('lote', ''),
                'produto': d.get('produto', ''),
                'codigo': d.get('codigo', ''),
                'peca': d.get('peca', ''),
                'tipo_chapa': tipo,
                'w': comprimento_mm,
                'h': largura_mm,
                'comp_original': comprimento_mm,
                'larg_original': largura_mm,
                'espessura': d.get('espessura', ''),
                'area': comprimento_mm * largura_mm,
            }
            pecas_por_tipo.setdefault(tipo, []).append(peca)

    return pecas_por_tipo, avisos


def nova_chapa(tipo, largura, altura, numero):
    return {
        'tipo_chapa': tipo,
        'numero': numero,
        'w': int(largura),
        'h': int(altura),
        'free': [{'x': 0, 'y': 0, 'w': int(largura), 'h': int(altura)}],
        'pecas': []
    }


def limpar_espacos_livres(chapa):
    livres = []
    for r in chapa['free']:
        if r['w'] > 0 and r['h'] > 0:
            livres.append(r)

    filtrados = []
    for i, a in enumerate(livres):
        contido = False
        for j, b in enumerate(livres):
            if i == j:
                continue
            if (
                a['x'] >= b['x'] and a['y'] >= b['y']
                and a['x'] + a['w'] <= b['x'] + b['w']
                and a['y'] + a['h'] <= b['y'] + b['h']
            ):
                contido = True
                break
        if not contido:
            filtrados.append(a)

    filtrados.sort(key=lambda r: (r['y'], r['x'], r['h'] * r['w']))
    chapa['free'] = filtrados[:250]


def buscar_melhor_posicao(chapa, peca, kerf_mm, permite_girar):
    candidatos = []
    orientacoes = [(peca['w'], peca['h'], False)]
    if permite_girar and peca['w'] != peca['h']:
        orientacoes.append((peca['h'], peca['w'], True))

    for idx, livre in enumerate(chapa['free']):
        for pw, ph, girada in orientacoes:
            if pw <= livre['w'] and ph <= livre['h']:
                sobra_area = livre['w'] * livre['h'] - pw * ph
                sobra_menor_lado = min(livre['w'] - pw, livre['h'] - ph)
                candidatos.append((sobra_area, sobra_menor_lado, idx, livre, pw, ph, girada))

    if not candidatos:
        return None

    candidatos.sort(key=lambda x: (x[0], x[1]))
    return candidatos[0]


def colocar_peca_na_chapa(chapa, peca, kerf_mm=4, permite_girar=True):
    escolha = buscar_melhor_posicao(chapa, peca, kerf_mm, permite_girar)
    if not escolha:
        return False

    _, _, idx_livre, livre, pw, ph, girada = escolha
    x = livre['x']
    y = livre['y']

    colocada = dict(peca)
    colocada.update({'x': x, 'y': y, 'w': pw, 'h': ph, 'girada': girada})
    chapa['pecas'].append(colocada)

    reserva_w = min(livre['w'], pw + int(round(kerf_mm)))
    reserva_h = min(livre['h'], ph + int(round(kerf_mm)))

    novos_livres = chapa['free'][:idx_livre] + chapa['free'][idx_livre + 1:]

    direita = {
        'x': x + reserva_w,
        'y': y,
        'w': livre['w'] - reserva_w,
        'h': ph
    }
    abaixo = {
        'x': x,
        'y': y + reserva_h,
        'w': livre['w'],
        'h': livre['h'] - reserva_h
    }

    if direita['w'] > 0 and direita['h'] > 0:
        novos_livres.append(direita)
    if abaixo['w'] > 0 and abaixo['h'] > 0:
        novos_livres.append(abaixo)

    chapa['free'] = novos_livres
    limpar_espacos_livres(chapa)
    return True


def empacotar_pecas(tipo, pecas, largura_chapa_mm, altura_chapa_mm, kerf_mm=4, permite_girar=True):
    avisos = []
    chapas = []

    pecas_ordenadas = sorted(
        pecas,
        key=lambda p: (max(p['w'], p['h']), p['w'] * p['h'], min(p['w'], p['h'])),
        reverse=True
    )

    for peca in pecas_ordenadas:
        cabe_normal = peca['w'] <= largura_chapa_mm and peca['h'] <= altura_chapa_mm
        cabe_girada = permite_girar and peca['h'] <= largura_chapa_mm and peca['w'] <= altura_chapa_mm
        if not cabe_normal and not cabe_girada:
            avisos.append(
                f'Peça maior que a chapa e não foi encaixada: {peca["produto"]} - {peca["peca"]} '
                f'({peca["w"]}x{peca["h"]} mm) em {tipo} ({largura_chapa_mm}x{altura_chapa_mm} mm).'
            )
            continue

        inserida = False
        for chapa in sorted(chapas, key=lambda c: -sum(x['area'] for x in c['pecas'])):
            if colocar_peca_na_chapa(chapa, peca, kerf_mm, permite_girar):
                inserida = True
                break

        if not inserida:
            chapa = nova_chapa(tipo, largura_chapa_mm, altura_chapa_mm, len(chapas) + 1)
            if colocar_peca_na_chapa(chapa, peca, kerf_mm, permite_girar):
                chapas.append(chapa)
            else:
                avisos.append(
                    f'Não foi possível encaixar a peça: {peca["produto"]} - {peca["peca"]} '
                    f'({peca["w"]}x{peca["h"]} mm).'
                )

    for chapa in chapas:
        area_usada = sum(p['area'] for p in chapa['pecas'])
        area_chapa = chapa['w'] * chapa['h']
        chapa['area_usada_mm2'] = area_usada
        chapa['area_sobra_mm2'] = max(area_chapa - area_usada, 0)
        chapa['aproveitamento'] = area_usada / area_chapa if area_chapa else 0
        chapa['pecas'].sort(key=lambda p: (p['y'], p['x'], p['peca']))
        chapa['modelo_corte'] = 'encaixe'
        chapa['sequencia_corte'] = sequencia_encaixe(chapa)

    return chapas, avisos



def orientacoes_para_plano(peca, permite_girar=True):
    orientacoes = [(peca['w'], peca['h'], False)]
    if permite_girar and peca['w'] != peca['h']:
        orientacoes.append((peca['h'], peca['w'], True))
    return orientacoes


def sequencia_encaixe(chapa):
    seq = ['Modo ENCAIXE LIVRE: seguir o desenho e identificar as peças por posição X/Y.']
    pecas = sorted(chapa.get('pecas', []), key=lambda p: (p.get('y', 0), p.get('x', 0), p.get('peca', '')))
    for i, p in enumerate(pecas[:120], start=1):
        seq.append(
            f'{i}. {p["id"]} - {texto_curto(p.get("peca", ""), 34)} - '
            f'{fmt_mm(p.get("comp_original", p.get("w", 0)))}x{fmt_mm(p.get("larg_original", p.get("h", 0)))} mm - '
            f'posição X {fmt_mm(p.get("x", 0))} / Y {fmt_mm(p.get("y", 0))}.'
        )
    if len(pecas) > 120:
        seq.append(f'... mais {len(pecas) - 120} peça(s) nesta chapa.')
    return seq


def sequencia_guilhotina(chapa):
    seq = ['Modo GUILHOTINA: primeiro corte as faixas; depois corte as peças dentro de cada faixa.']
    passo = 1
    for faixa in sorted(chapa.get('faixas', []), key=lambda f: f['y']):
        seq.append(
            f'{passo}. Cortar FAIXA {faixa["indice"]} com altura útil '
            f'{fmt_mm(faixa["h_util"])} mm na posição Y {fmt_mm(faixa["y"])} mm.'
        )
        passo += 1
        for p in sorted(faixa.get('pecas', []), key=lambda item: item['x']):
            rot = ' girada 90°' if p.get('girada') else ''
            seq.append(
                f'{passo}. Na FAIXA {faixa["indice"]}, cortar {p["id"]} - '
                f'{texto_curto(p.get("peca", ""), 32)} - '
                f'{fmt_mm(p.get("comp_original", p.get("w", 0)))}x{fmt_mm(p.get("larg_original", p.get("h", 0)))} mm{rot} - '
                f'posição X {fmt_mm(p.get("x", 0))}.'
            )
            passo += 1
    return seq


def nova_chapa_guilhotina(tipo, largura, altura, numero):
    return {
        'tipo_chapa': tipo,
        'numero': numero,
        'w': int(largura),
        'h': int(altura),
        'pecas': [],
        'modelo_corte': 'guilhotina',
        'faixas': [],
        'ocupado_y': 0
    }


def escolher_orientacao_faixa(peca, faixa, largura_chapa, kerf, permite_girar=True):
    candidatos = []
    for pw, ph, girada in orientacoes_para_plano(peca, permite_girar):
        adicional = (kerf if faixa['pecas'] else 0) + pw
        if ph <= faixa['h_util'] and faixa['x_ocupado'] + adicional <= largura_chapa:
            sobra_altura = faixa['h_util'] - ph
            sobra_largura = largura_chapa - (faixa['x_ocupado'] + adicional)
            candidatos.append((sobra_altura, sobra_largura, pw, ph, girada))
    if not candidatos:
        return None
    candidatos.sort(key=lambda x: (x[0], x[1]))
    return candidatos[0]


def colocar_peca_guilhotina(chapa, peca, kerf_mm=4, permite_girar=True):
    kerf = int(round(kerf_mm))
    melhor = None

    for faixa in chapa['faixas']:
        escolha = escolher_orientacao_faixa(peca, faixa, chapa['w'], kerf, permite_girar)
        if escolha:
            sobra_altura, sobra_largura, pw, ph, girada = escolha
            chave = (sobra_altura, sobra_largura, len(faixa['pecas']))
            if melhor is None or chave < melhor[0]:
                melhor = (chave, faixa, pw, ph, girada)

    if melhor:
        _, faixa, pw, ph, girada = melhor
        x = faixa['x_ocupado'] + (kerf if faixa['pecas'] else 0)
        y = faixa['y']
        colocada = dict(peca)
        colocada.update({'x': x, 'y': y, 'w': pw, 'h': ph, 'girada': girada, 'faixa': faixa['indice']})
        faixa['pecas'].append(colocada)
        faixa['x_ocupado'] = x + pw
        chapa['pecas'].append(colocada)
        return True

    y_base = 0 if not chapa['faixas'] else chapa['ocupado_y'] + kerf
    candidatos = []
    for pw, ph, girada in orientacoes_para_plano(peca, permite_girar):
        if pw <= chapa['w'] and y_base + ph <= chapa['h']:
            sobra_altura_chapa = chapa['h'] - (y_base + ph)
            sobra_largura = chapa['w'] - pw
            candidatos.append((sobra_altura_chapa, sobra_largura, pw, ph, girada))

    if not candidatos:
        return False

    candidatos.sort(key=lambda x: (x[0], x[1]))
    _, _, pw, ph, girada = candidatos[0]
    faixa = {'indice': len(chapa['faixas']) + 1, 'y': y_base, 'h_util': ph, 'pecas': [], 'x_ocupado': 0}
    chapa['faixas'].append(faixa)
    chapa['ocupado_y'] = y_base + ph

    colocada = dict(peca)
    colocada.update({'x': 0, 'y': y_base, 'w': pw, 'h': ph, 'girada': girada, 'faixa': faixa['indice']})
    faixa['pecas'].append(colocada)
    faixa['x_ocupado'] = pw
    chapa['pecas'].append(colocada)
    return True


def empacotar_pecas_guilhotina(tipo, pecas, largura_chapa_mm, altura_chapa_mm, kerf_mm=4, permite_girar=True):
    avisos = []
    chapas = []

    pecas_ordenadas = sorted(
        pecas,
        key=lambda p: (max(p['w'], p['h']), min(p['w'], p['h']), p['w'] * p['h']),
        reverse=True
    )

    for peca in pecas_ordenadas:
        cabe_normal = peca['w'] <= largura_chapa_mm and peca['h'] <= altura_chapa_mm
        cabe_girada = permite_girar and peca['h'] <= largura_chapa_mm and peca['w'] <= altura_chapa_mm
        if not cabe_normal and not cabe_girada:
            avisos.append(
                f'Peça maior que a chapa e não foi encaixada em guilhotina: {peca["produto"]} - {peca["peca"]} '
                f'({peca["w"]}x{peca["h"]} mm) em {tipo} ({largura_chapa_mm}x{altura_chapa_mm} mm).'
            )
            continue

        inserida = False
        for chapa in sorted(chapas, key=lambda c: (len(c.get('faixas', [])), -sum(x['area'] for x in c['pecas']))):
            if colocar_peca_guilhotina(chapa, peca, kerf_mm, permite_girar):
                inserida = True
                break

        if not inserida:
            chapa = nova_chapa_guilhotina(tipo, largura_chapa_mm, altura_chapa_mm, len(chapas) + 1)
            if colocar_peca_guilhotina(chapa, peca, kerf_mm, permite_girar):
                chapas.append(chapa)
            else:
                avisos.append(
                    f'Não foi possível encaixar em guilhotina: {peca["produto"]} - {peca["peca"]} '
                    f'({peca["w"]}x{peca["h"]} mm).'
                )

    for chapa in chapas:
        area_usada = sum(p['area'] for p in chapa['pecas'])
        area_chapa = chapa['w'] * chapa['h']
        chapa['area_usada_mm2'] = area_usada
        chapa['area_sobra_mm2'] = max(area_chapa - area_usada, 0)
        chapa['aproveitamento'] = area_usada / area_chapa if area_chapa else 0
        chapa['pecas'].sort(key=lambda p: (p['y'], p['x'], p['peca']))
        chapa['sequencia_corte'] = sequencia_guilhotina(chapa)

    return chapas, avisos


def gerar_plano_corte(itens, modo_corte='encaixe', kerf_mm=KERF_MM_PADRAO, meta_aproveitamento=META_APROVEITAMENTO_PADRAO, permite_girar=PERMITE_GIRAR_90_PADRAO):
    modo_corte = 'guilhotina' if str(modo_corte).lower().startswith('g') else 'encaixe'
    resultado = calcular_lote(itens)
    pecas_por_tipo, avisos_expansao = explodir_pecas_para_plano(resultado)

    cfg_por_tipo = {}
    for r in resultado.get('resumo', []):
        cfg_por_tipo[r['tipo_chapa']] = {
            'comprimento': r['comprimento_chapa'],
            'largura': r['largura_chapa'],
            'aproveitamento_planilha': r['aproveitamento']
        }

    grupos = []
    avisos = list(resultado.get('avisos', [])) + avisos_expansao

    for tipo, pecas in sorted(pecas_por_tipo.items()):
        cfg = cfg_por_tipo.get(tipo, {'comprimento': 2.75, 'largura': 1.85, 'aproveitamento_planilha': 0.95})
        largura_chapa_mm = mm(cfg['comprimento'])
        altura_chapa_mm = mm(cfg['largura'])

        if modo_corte == 'guilhotina':
            chapas, avisos_pack = empacotar_pecas_guilhotina(
                tipo,
                pecas,
                largura_chapa_mm,
                altura_chapa_mm,
                kerf_mm=kerf_mm,
                permite_girar=permite_girar
            )
        else:
            chapas, avisos_pack = empacotar_pecas(
                tipo,
                pecas,
                largura_chapa_mm,
                altura_chapa_mm,
                kerf_mm=kerf_mm,
                permite_girar=permite_girar
            )

        area_pecas = sum(c['area_usada_mm2'] for c in chapas)
        area_chapas = len(chapas) * largura_chapa_mm * altura_chapa_mm
        aproveitamento_real = area_pecas / area_chapas if area_chapas else 0

        grupos.append({
            'tipo_chapa': tipo,
            'largura_chapa_mm': largura_chapa_mm,
            'altura_chapa_mm': altura_chapa_mm,
            'qtde_pecas': len(pecas),
            'chapas': chapas,
            'chapas_usadas': len(chapas),
            'area_pecas_mm2': area_pecas,
            'area_chapas_mm2': area_chapas,
            'sobra_mm2': max(area_chapas - area_pecas, 0),
            'aproveitamento_real': aproveitamento_real,
            'meta_aproveitamento': meta_aproveitamento,
            'status_meta': 'Meta atingida' if aproveitamento_real >= meta_aproveitamento else 'Abaixo da meta'
        })
        avisos.extend(avisos_pack)

    totais = {
        'tipos_chapa': len(grupos),
        'chapas_total': sum(g['chapas_usadas'] for g in grupos),
        'pecas_total': sum(g['qtde_pecas'] for g in grupos),
        'area_pecas_mm2': sum(g['area_pecas_mm2'] for g in grupos),
        'area_chapas_mm2': sum(g['area_chapas_mm2'] for g in grupos),
        'sobra_mm2': sum(g['sobra_mm2'] for g in grupos),
        'aproveitamento_real': 0
    }
    totais['aproveitamento_real'] = (
        totais['area_pecas_mm2'] / totais['area_chapas_mm2'] if totais['area_chapas_mm2'] else 0
    )

    return {
        'resultado_area': resultado,
        'grupos': grupos,
        'totais': totais,
        'avisos': avisos,
        'kerf_mm': kerf_mm,
        'permite_girar': permite_girar,
        'meta_aproveitamento': meta_aproveitamento,
        'modo_corte': modo_corte,
        'modo_corte_nome': 'Guilhotina por faixas' if modo_corte == 'guilhotina' else 'Encaixe livre',
    }


def svg_chapa(chapa, indice_global=0):
    W = max(int(chapa['w']), 1)
    H = max(int(chapa['h']), 1)
    rects = [
        f'<svg class="sheet-svg" viewBox="0 0 {W} {H}" role="img" aria-label="Plano da chapa {chapa["numero"]}">',
        f'<rect x="0" y="0" width="{W}" height="{H}" fill="#fff" stroke="#0f172a" stroke-width="10"/>'
    ]

    if chapa.get('modelo_corte') == 'guilhotina':
        for faixa in chapa.get('faixas', []):
            y_faixa = faixa.get('y', 0)
            h_faixa = faixa.get('h_util', 0)
            rects.append(
                f'<line x1="0" y1="{y_faixa}" x2="{W}" y2="{y_faixa}" '
                f'stroke="#ef4444" stroke-width="6" stroke-dasharray="20 12"/>'
            )
            rects.append(
                f'<text x="12" y="{max(y_faixa + 38, 42)}" font-size="30" '
                f'fill="#991b1b" font-family="Arial">Faixa {faixa.get("indice", "")} - {fmt_mm(h_faixa)} mm</text>'
            )

    for i, p in enumerate(chapa['pecas']):
        fill = cor_plano(i + indice_global)
        x, y, w, h = p['x'], p['y'], p['w'], p['h']
        titulo = (
            f'{p["id"]} - {p["peca"]} - {p["produto"]} - Lote {p.get("lote", "")} - '
            f'{fmt_mm(p["comp_original"])} x {fmt_mm(p["larg_original"])} mm'
        )
        rects.append(f'<g><title>{escape(titulo)}</title>')
        rects.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" '
            f'stroke="#334155" stroke-width="4"/>'
        )

        menor = min(w, h)
        font = 34 if menor >= 240 else 26 if menor >= 160 else 20
        if w >= 160 and h >= 90:
            linhas = [
                texto_curto(p['peca'], 30),
                f'{fmt_mm(p["comp_original"])}x{fmt_mm(p["larg_original"])} mm',
                texto_curto(p['produto'], 26)
            ]
            if h < 180:
                linhas = linhas[:2]
            yy = y + font + 8
            rects.append(f'<text x="{x + 10}" y="{yy}" font-size="{font}" fill="#0f172a" font-family="Arial">')
            for n, linha in enumerate(linhas):
                dy = 0 if n == 0 else font + 4
                rects.append(f'<tspan x="{x + 10}" dy="{dy}">{escape(linha)}</tspan>')
            rects.append('</text>')
        else:
            rects.append(
                f'<text x="{x + 6}" y="{y + max(18, font)}" font-size="{font}" fill="#0f172a" '
                f'font-family="Arial">{escape(p["id"])}</text>'
            )
        rects.append('</g>')

    rects.append('</svg>')
    return ''.join(rects)


def tabela_pecas_chapa(chapa):
    linhas = []
    for idx, p in enumerate(chapa['pecas'], start=1):
        linhas.append(
            f'<tr><td>{idx}</td><td>{escape(p["id"])}</td><td>{escape(str(p.get("faixa", "-")))}</td><td>{escape(p.get("lote", ""))}</td>'
            f'<td>{escape(p["produto"])}</td><td>{escape(p["peca"])}</td>'
            f'<td class="num">{fmt_mm(p["comp_original"])} x {fmt_mm(p["larg_original"])}</td>'
            f'<td>{ "Sim" if p.get("girada") else "Não" }</td>'
            f'<td class="num">X {fmt_mm(p["x"])} / Y {fmt_mm(p["y"])}</td></tr>'
        )
    return (
        '<div class="table-wrap"><table><thead><tr><th>#</th><th>ID</th><th>Faixa</th><th>Lote</th><th>Produto</th>'
        '<th>Peça</th><th class="num">Medida original mm</th><th>Girada 90°</th><th class="num">Posição</th>'
        '</tr></thead><tbody>' + ''.join(linhas) + '</tbody></table></div>'
    )


def lista_sequencia_chapa(chapa):
    linhas = []
    for etapa in chapa.get('sequencia_corte', [])[:140]:
        linhas.append(f'<li>{escape(etapa)}</li>')
    if len(chapa.get('sequencia_corte', [])) > 140:
        linhas.append(f'<li>Mais {len(chapa.get("sequencia_corte", [])) - 140} etapa(s) ocultas.</li>')
    return '<ol class="seq">' + ''.join(linhas) + '</ol>'


def campos_hidden_lote(entradas, modo_corte='encaixe'):
    html = [f'<input type="hidden" name="modo_corte" value="{escape(str(modo_corte))}">']
    for item in entradas:
        html.append(f'<input type="hidden" name="lote" value="{escape(str(item.get("lote", "")))}">')
        html.append(f'<input type="hidden" name="produto" value="{escape(str(item.get("produto", item.get("produto_digitado", ""))))}">')
        html.append(f'<input type="hidden" name="quantidade" value="{escape(str(item.get("quantidade", "")))}">')
    return ''.join(html)


def render_planos_corte(plano=None, entradas=None, modo_corte='encaixe'):
    produtos = buscar_produtos()
    datalist = ''.join(f'<option value="{escape(p)}"></option>' for p in produtos)
    if entradas is None:
        entradas = [{'lote': '', 'produto': '', 'quantidade': ''} for _ in range(4)]

    modo_corte = 'guilhotina' if str(modo_corte).lower().startswith('g') else 'encaixe'
    checked_encaixe = 'checked' if modo_corte == 'encaixe' else ''
    checked_guilhotina = 'checked' if modo_corte == 'guilhotina' else ''

    rows = []
    for e in entradas:
        produto_valor = e.get('produto', e.get('produto_digitado', ''))
        rows.append(
            f'''<div class="form-row"><input name="lote" placeholder="Nº do lote" value="{escape(str(e.get('lote', '')))}">'''
            f'''<input name="produto" list="produtos-list" placeholder="Digite ou selecione o produto" value="{escape(str(produto_valor))}">'''
            f'''<input name="quantidade" type="text" inputmode="decimal" placeholder="Quantidade" value="{escape(str(e.get('quantidade', '')))}">'''
            f'''<button type="button" class="btn secondary remove" onclick="removerLinha(this)">×</button></div>'''
        )

    resultado_html = ''
    if plano:
        avisos = ''.join(f'<div class="notice">{escape(a)}</div>' for a in plano.get('avisos', [])[:30])
        if len(plano.get('avisos', [])) > 30:
            avisos += f'<div class="notice">Existem mais {len(plano["avisos"]) - 30} aviso(s) oculto(s).</div>'

        t = plano['totais']
        resultado_html += f'''
        {avisos}
        <div class="card">
            <h2>Resumo do plano de corte</h2>
            <div class="badge-mode">{escape(plano.get('modo_corte_nome', 'Encaixe livre'))}</div>
            <div class="grid">
                <div class="kpi"><span>Tipos de chapa</span><strong>{fmt_num(t['tipos_chapa'])}</strong></div>
                <div class="kpi"><span>Total de chapas</span><strong>{fmt_num(t['chapas_total'])}</strong></div>
                <div class="kpi"><span>Total de peças</span><strong>{fmt_num(t['pecas_total'])}</strong></div>
                <div class="kpi"><span>Aproveitamento real</span><strong>{fmt_num(t['aproveitamento_real'] * 100, 1)}%</strong></div>
                <div class="kpi"><span>Sobra total</span><strong>{fmt_m2(m2_de_mm2(t['sobra_mm2']))} m²</strong></div>
            </div>
            <div class="actions">
                <form method="post" action="/baixar_plano_pdf">
                    {campos_hidden_lote(entradas, plano.get('modo_corte', 'encaixe'))}
                    <button class="btn" type="submit">Baixar PDF do plano de corte</button>
                </form>
                <button type="button" class="btn secondary" onclick="window.print()">Imprimir / salvar PDF pelo navegador</button>
            </div>
            <p class="mini">Regra usada: {escape(plano.get('modo_corte_nome', 'Encaixe livre'))}, rotação 90° permitida, sem veio, serra/kerf de {fmt_num(plano['kerf_mm'], 1)} mm. A meta de 95% é objetivo, não garantia.</p>
        </div>
        '''

        if not plano['grupos']:
            resultado_html += '<div class="notice">Nenhuma peça calculável encontrada para gerar plano de corte.</div>'

        idx_global = 0
        for grupo in plano['grupos']:
            status_cls = 'ok' if grupo['aproveitamento_real'] >= grupo['meta_aproveitamento'] else 'notice'
            resultado_html += f'''
            <div class="card">
                <h2>{escape(grupo['tipo_chapa'])}</h2>
                <div class="grid">
                    <div class="kpi"><span>Medida da chapa</span><strong>{grupo['largura_chapa_mm']} x {grupo['altura_chapa_mm']} mm</strong></div>
                    <div class="kpi"><span>Chapas</span><strong>{fmt_num(grupo['chapas_usadas'])}</strong></div>
                    <div class="kpi"><span>Peças</span><strong>{fmt_num(grupo['qtde_pecas'])}</strong></div>
                    <div class="kpi"><span>Aproveitamento</span><strong>{fmt_num(grupo['aproveitamento_real'] * 100, 1)}%</strong></div>
                    <div class="kpi"><span>Sobra</span><strong>{fmt_m2(m2_de_mm2(grupo['sobra_mm2']))} m²</strong></div>
                </div>
                <div class="{status_cls}">Meta de {fmt_num(grupo['meta_aproveitamento'] * 100, 1)}%: {escape(grupo['status_meta'])}</div>
            </div>
            '''
            for chapa in grupo['chapas']:
                resultado_html += f'''
                <div class="card sheet-card">
                    <h3>Chapa {chapa['numero']} - {escape(grupo['tipo_chapa'])}</h3>
                    <p class="muted">Aproveitamento: <strong>{fmt_num(chapa['aproveitamento'] * 100, 1)}%</strong> · Sobra: <strong>{fmt_m2(m2_de_mm2(chapa['area_sobra_mm2']))} m²</strong> · Peças: <strong>{len(chapa['pecas'])}</strong></p>
                    <div class="sheet-box">{svg_chapa(chapa, idx_global)}</div>
                    <details>
                        <summary>Ver lista e sequência sugerida das peças desta chapa</summary>
                        <p class="mini">Sequência operacional gerada conforme o modo escolhido.</p>
                        {lista_sequencia_chapa(chapa)}
                        {tabela_pecas_chapa(chapa)}
                    </details>
                </div>
                '''
                idx_global += len(chapa['pecas'])

    corpo = f'''
    <div class="card">
        <h2>Gerar plano de corte</h2>
        <p class="muted">Informe os produtos do lote, escolha o modo de corte e gere o desenho chapa por chapa.</p>
        <form method="post" action="/planos_corte" id="form-lote">
            <datalist id="produtos-list">{datalist}</datalist>
            <div class="cut-options">
                <label><input type="radio" name="modo_corte" value="encaixe" {checked_encaixe}> <strong>Encaixe livre</strong><span>Busca ocupar espaços livres da chapa. Pode aproveitar melhor, mas não é sequência pura de seccionadora.</span></label>
                <label><input type="radio" name="modo_corte" value="guilhotina" {checked_guilhotina}> <strong>Guilhotina por faixas</strong><span>Organiza em faixas para operação de seccionadora, com sequência mais clara.</span></label>
            </div>
            <div id="linhas">{''.join(rows)}</div>
            <div class="actions">
                <button type="button" class="btn secondary" onclick="adicionarLinha()">+ Adicionar produto</button>
                <button type="submit" class="btn">Gerar plano de corte</button>
            </div>
        </form>
    </div>
    {resultado_html}
    <script>
    function adicionarLinha(){{
        const div=document.createElement('div');
        div.className='form-row';
        div.innerHTML='<input name="lote" placeholder="Nº do lote"><input name="produto" list="produtos-list" placeholder="Digite ou selecione o produto"><input name="quantidade" type="text" inputmode="decimal" placeholder="Quantidade"><button type="button" class="btn secondary remove" onclick="removerLinha(this)">×</button>';
        document.getElementById('linhas').appendChild(div);
    }}
    function removerLinha(btn){{
        const linhas=document.querySelectorAll('.form-row');
        if(linhas.length>1)btn.parentElement.remove();
    }}
    </script>
    '''
    return layout('Planos de corte', corpo)


def montar_itens_de_campos(campos):
    lotes = campos.get('lote', [])
    produtos = campos.get('produto', [])
    quantidades = campos.get('quantidade', [])
    total_linhas = max(len(lotes), len(produtos), len(quantidades))
    itens = []
    for i in range(total_linhas):
        itens.append({
            'lote': lotes[i] if i < len(lotes) else '',
            'produto': produtos[i] if i < len(produtos) else '',
            'quantidade': quantidades[i] if i < len(quantidades) else ''
        })
    return itens


def pdf_limpar_texto(texto):
    texto = '' if texto is None else str(texto)
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
    texto = texto.replace('\\', '/').replace('(', '[').replace(')', ']')
    return texto[:160]


def pdf_escape(texto):
    return pdf_limpar_texto(texto).replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')


def pdf_texto(cmds, x, y, texto, tamanho=10, negrito=False):
    fonte = '/F2' if negrito else '/F1'
    cmds.append('0 0 0 rg')
    cmds.append(f'BT {fonte} {tamanho} Tf {x:.2f} {y:.2f} Td ({pdf_escape(texto)}) Tj ET')


def pdf_retangulo(cmds, x, y, w, h, fill=None, stroke=(0.15, 0.2, 0.3), lw=0.5):
    if fill:
        cmds.append(f'{fill[0]:.3f} {fill[1]:.3f} {fill[2]:.3f} rg')
    if stroke:
        cmds.append(f'{stroke[0]:.3f} {stroke[1]:.3f} {stroke[2]:.3f} RG')
    cmds.append(f'{lw:.2f} w {x:.2f} {y:.2f} {w:.2f} {h:.2f} re {"B" if fill and stroke else "f" if fill else "S"}')


def hex_para_rgb(cor_hex):
    cor_hex = cor_hex.lstrip('#')
    return tuple(int(cor_hex[i:i+2], 16) / 255.0 for i in (0, 2, 4))


def criar_pdf_simples(paginas_cmds):
    objetos = []

    def add_obj(conteudo):
        objetos.append(conteudo if isinstance(conteudo, bytes) else conteudo.encode('latin-1', 'replace'))
        return len(objetos)

    add_obj(b'<< /Type /Catalog /Pages 2 0 R >>')
    add_obj(b'')
    add_obj(b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>')
    add_obj(b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>')

    page_ids = []
    for cmds in paginas_cmds:
        stream = ('\n'.join(cmds)).encode('latin-1', 'replace')
        content_id = add_obj(b'<< /Length ' + str(len(stream)).encode() + b' >>\nstream\n' + stream + b'\nendstream')
        page_id = add_obj(
            f'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 842 595] '
            f'/Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents {content_id} 0 R >>'
        )
        page_ids.append(page_id)

    kids = ' '.join(f'{pid} 0 R' for pid in page_ids)
    objetos[1] = f'<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>'.encode('latin-1')

    saida = bytearray()
    saida.extend(b'%PDF-1.4\n%\xe2\xe3\xcf\xd3\n')
    offsets = [0]
    for i, obj in enumerate(objetos, start=1):
        offsets.append(len(saida))
        saida.extend(f'{i} 0 obj\n'.encode('ascii'))
        saida.extend(obj)
        saida.extend(b'\nendobj\n')
    xref_pos = len(saida)
    saida.extend(f'xref\n0 {len(objetos)+1}\n'.encode('ascii'))
    saida.extend(b'0000000000 65535 f \n')
    for off in offsets[1:]:
        saida.extend(f'{off:010d} 00000 n \n'.encode('ascii'))
    saida.extend(
        f'trailer\n<< /Size {len(objetos)+1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF'.encode('ascii')
    )
    return bytes(saida)


def gerar_pdf_plano_corte(plano):
    paginas = []
    agora = datetime.now().strftime('%d/%m/%Y %H:%M')

    cmds = []
    pdf_texto(cmds, 30, 555, 'PLANO DE CORTE - RESUMO DO LOTE', 18, True)
    pdf_texto(cmds, 30, 532, f'Gerado em: {agora}', 10)
    pdf_texto(cmds, 30, 512, f'Modo: {plano.get("modo_corte_nome", "Encaixe livre")} | Serra/Kerf: {fmt_num(plano["kerf_mm"], 1)} mm | Rotacao 90 graus: {"Sim" if plano["permite_girar"] else "Nao"} | Meta: {fmt_num(plano["meta_aproveitamento"]*100, 1)}%', 10)

    t = plano['totais']
    resumo_linhas = [
        f'Tipos de chapa: {fmt_num(t["tipos_chapa"])}',
        f'Total de chapas: {fmt_num(t["chapas_total"])}',
        f'Total de pecas: {fmt_num(t["pecas_total"])}',
        f'Area de pecas: {fmt_m2(m2_de_mm2(t["area_pecas_mm2"]))} m2',
        f'Area total de chapas: {fmt_m2(m2_de_mm2(t["area_chapas_mm2"]))} m2',
        f'Sobra total: {fmt_m2(m2_de_mm2(t["sobra_mm2"]))} m2',
        f'Aproveitamento real medio: {fmt_num(t["aproveitamento_real"] * 100, 1)}%',
    ]
    y = 480
    for linha in resumo_linhas:
        pdf_texto(cmds, 50, y, linha, 12)
        y -= 20

    y -= 10
    pdf_texto(cmds, 30, y, 'Resumo por tipo de chapa', 13, True)
    y -= 22
    for g in plano['grupos']:
        linha = (
            f'{g["tipo_chapa"]} | chapa {g["largura_chapa_mm"]}x{g["altura_chapa_mm"]} mm | '
            f'{g["chapas_usadas"]} chapa[s] | {g["qtde_pecas"]} peca[s] | '
            f'aproveit. {fmt_num(g["aproveitamento_real"]*100, 1)}% | sobra {fmt_m2(m2_de_mm2(g["sobra_mm2"]))} m2'
        )
        pdf_texto(cmds, 40, y, linha, 9)
        y -= 15
        if y < 40:
            paginas.append(cmds)
            cmds = []
            y = 555
    paginas.append(cmds)

    indice_global = 0
    for g in plano['grupos']:
        for chapa in g['chapas']:
            cmds = []
            titulo = f'Chapa {chapa["numero"]} - {g["tipo_chapa"]} - {chapa["w"]}x{chapa["h"]} mm'
            pdf_texto(cmds, 30, 560, titulo, 15, True)
            pdf_texto(
                cmds, 30, 540,
                f'Modo: {plano.get("modo_corte_nome", "Encaixe livre")} | Aproveitamento: {fmt_num(chapa["aproveitamento"]*100, 1)}% | Sobra: {fmt_m2(m2_de_mm2(chapa["area_sobra_mm2"]))} m2 | Pecas: {len(chapa["pecas"])}',
                10
            )

            max_w, max_h = 760, 410
            escala = min(max_w / chapa['w'], max_h / chapa['h'])
            sx = 40
            sy = 100
            dw = chapa['w'] * escala
            dh = chapa['h'] * escala
            pdf_retangulo(cmds, sx, sy, dw, dh, fill=(1, 1, 1), stroke=(0.05, 0.09, 0.16), lw=1.2)

            if chapa.get('modelo_corte') == 'guilhotina':
                for faixa in chapa.get('faixas', []):
                    yline = sy + (chapa['h'] - faixa.get('y', 0)) * escala
                    cmds.append('0.9 0.1 0.1 RG')
                    cmds.append(f'0.75 w {sx:.2f} {yline:.2f} m {sx + dw:.2f} {yline:.2f} l S')

            for i, p in enumerate(chapa['pecas']):
                px = sx + p['x'] * escala
                py = sy + (chapa['h'] - p['y'] - p['h']) * escala
                pw = p['w'] * escala
                ph = p['h'] * escala
                fill = hex_para_rgb(cor_plano(indice_global + i))
                pdf_retangulo(cmds, px, py, pw, ph, fill=fill, stroke=(0.2, 0.25, 0.35), lw=0.35)
                if pw > 45 and ph > 20:
                    texto1 = f'{p["id"]} {texto_curto(p["peca"], 22)}'
                    texto2 = f'{fmt_mm(p["comp_original"])}x{fmt_mm(p["larg_original"])} mm'
                    pdf_texto(cmds, px + 3, py + ph - 10, texto1, 5.5)
                    if ph > 32:
                        pdf_texto(cmds, px + 3, py + ph - 20, texto2, 5.5)

            y = 80
            pdf_texto(cmds, 40, y, 'Sequencia operacional sugerida:', 8, True)
            y -= 12
            sequencia = chapa.get('sequencia_corte', [])
            for linha in sequencia[:18]:
                pdf_texto(cmds, 40, y, linha, 6.2)
                y -= 8.5
                if y < 18:
                    break
            if len(sequencia) > 18:
                pdf_texto(cmds, 40, 18, f'Mais {len(sequencia) - 18} etapa[s] constam na visualizacao web.', 6.2)

            paginas.append(cmds)
            indice_global += len(chapa['pecas'])

    return criar_pdf_simples(paginas)


STYLE = r'''
:root{--bg:#f5f7fb;--card:#fff;--text:#1f2937;--muted:#64748b;--line:#e5e7eb;--primary:#2563eb;--primary-dark:#1d4ed8;--soft:#eff6ff;--warn:#fff7ed;--warn-text:#9a3412;--ok:#ecfdf5;--ok-text:#047857}*{box-sizing:border-box}body{margin:0;font-family:Arial,Helvetica,sans-serif;background:var(--bg);color:var(--text)}a{color:var(--primary);text-decoration:none}.wrap{max-width:1200px;margin:0 auto;padding:24px}.top{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:20px}.brand h1{margin:0;font-size:26px}.brand p{margin:6px 0 0;color:var(--muted)}.nav{display:flex;gap:8px;flex-wrap:wrap}.nav a,.btn{display:inline-block;border:0;border-radius:10px;padding:10px 14px;background:var(--primary);color:#fff;font-weight:700;cursor:pointer}.nav a{background:#fff;color:var(--primary);border:1px solid var(--line)}.btn:hover{background:var(--primary-dark)}.btn.secondary{background:#fff;color:var(--primary);border:1px solid var(--line)}.btn.danger{background:#dc2626}.card{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:18px;box-shadow:0 8px 24px rgba(15,23,42,.05);margin-bottom:18px}.grid{display:grid;grid-template-columns:repeat(5,minmax(150px,1fr));gap:14px}.kpi{background:var(--soft);border-radius:16px;padding:14px}.kpi span{display:block;color:var(--muted);font-size:13px}.kpi strong{display:block;font-size:24px;margin-top:4px}.table-wrap{overflow:auto;border-radius:14px;border:1px solid var(--line)}table{width:100%;border-collapse:collapse;background:#fff}th,td{padding:10px 12px;border-bottom:1px solid var(--line);text-align:left;font-size:14px;vertical-align:top}th{background:#f8fafc;color:#334155;position:sticky;top:0;z-index:1}tr:last-child td{border-bottom:0}.num{text-align:right;white-space:nowrap}.muted{color:var(--muted)}.notice{background:var(--warn);color:var(--warn-text);border:1px solid #fed7aa;border-radius:14px;padding:12px;margin-bottom:14px}.ok{background:var(--ok);color:var(--ok-text);border:1px solid #bbf7d0;border-radius:14px;padding:12px;margin-bottom:14px}.form-row{display:grid;grid-template-columns:160px 1fr 160px 44px;gap:10px;margin-bottom:10px}.form-row input,.form-row select,.field{width:100%;border:1px solid var(--line);border-radius:10px;padding:10px;font-size:15px;background:#fff}.mini{font-size:12px;color:var(--muted)}.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}.pill{display:inline-block;background:#eef2ff;color:#3730a3;border-radius:999px;padding:4px 9px;font-size:12px;margin:2px}.search{display:grid;grid-template-columns:1fr auto;gap:10px;margin-bottom:14px}.footer{color:var(--muted);font-size:12px;text-align:center;margin-top:24px}.sheet-card h3{margin-top:0}.sheet-box{width:100%;overflow:auto;border:1px solid var(--line);border-radius:14px;background:#f8fafc;padding:12px;margin:12px 0}.sheet-svg{width:100%;height:auto;max-height:720px;background:white;border-radius:10px}.sheet-svg text{paint-order:stroke;stroke:#fff;stroke-width:3px;stroke-linejoin:round}details{margin-top:12px}summary{cursor:pointer;font-weight:700;color:var(--primary)}.cut-options{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:14px 0}.cut-options label{display:block;border:1px solid var(--line);border-radius:16px;padding:14px;background:#fff;cursor:pointer}.cut-options label span{display:block;color:var(--muted);font-size:13px;margin-top:4px}.cut-options input{margin-right:8px}.badge-mode{display:inline-block;background:#0f172a;color:#fff;border-radius:999px;padding:6px 12px;font-weight:700;margin-bottom:14px}.seq{background:#f8fafc;border:1px solid var(--line);border-radius:14px;padding:14px 14px 14px 32px;max-height:360px;overflow:auto}.seq li{margin-bottom:6px}@media(max-width:900px){.grid,.cut-options{grid-template-columns:1fr}.form-row{grid-template-columns:1fr}.top{display:block}.nav{margin-top:14px}}@media print{.nav,.actions,.remove,.search{display:none}.wrap{max-width:none}.card{box-shadow:none}}
'''


def layout(titulo, corpo):
    return f'''<!doctype html><html lang="pt-br"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(titulo)}</title><link rel="stylesheet" href="/static/style.css"></head><body><div class="wrap"><div class="top"><div class="brand"><h1>{escape(titulo)}</h1><p>Consumo de chapas, histórico em Excel e plano de corte visual por chapa.</p></div><div class="nav"><a href="/">Calcular lote</a><a href="/planos_corte">Planos de corte</a><a href="/banco">Banco de dados</a><a href="/chapas">Configurar chapas</a><a href="/baixar_excel">Baixar histórico Excel</a></div></div>{corpo}<div class="footer">Software em Python. Plano de corte: encaixe retangular por tipo de chapa, rotação 90° permitida, kerf padrão de 4 mm.</div></div></body></html>'''


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
    corpo = f'''<div class="card"><h2>Entrada do lote</h2><p class="muted">Informe o número do lote, o produto agregado/completo e a quantidade que será produzida. Ao calcular, o sistema salva automaticamente o resultado no Excel de histórico.</p><form method="post" action="/calcular" id="form-lote"><datalist id="produtos-list">{datalist}</datalist><div id="linhas">{''.join(rows)}</div><div class="actions"><button type="button" class="btn secondary" onclick="adicionarLinha()">+ Adicionar produto</button><button type="submit" class="btn">Calcular e salvar no Excel</button><a class="btn secondary" href="/planos_corte">Gerar plano de corte</a><a class="btn secondary" href="/baixar_excel">Baixar histórico Excel</a><button type="button" class="btn secondary" onclick="window.print()">Imprimir / salvar PDF</button></div></form></div>{resultado_html}<script>function adicionarLinha(){{const div=document.createElement('div');div.className='form-row';div.innerHTML='<input name="lote" placeholder="Nº do lote"><input name="produto" list="produtos-list" placeholder="Digite ou selecione o produto"><input name="quantidade" type="text" inputmode="decimal" placeholder="Quantidade"><button type="button" class="btn secondary remove" onclick="removerLinha(this)">×</button>';document.getElementById('linhas').appendChild(div);}}function removerLinha(btn){{const linhas=document.querySelectorAll('.form-row');if(linhas.length>1)btn.parentElement.remove();}}</script>'''
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

    def enviar_arquivo(self, caminho, nome_download, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):
        dados = Path(caminho).read_bytes()
        self.enviar_download_bytes(dados, nome_download, content_type)

    def enviar_download_bytes(self, dados, nome_download, content_type):
        self.send_response(200)
        self.send_header('Content-Type', content_type)
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
            elif rota.path == '/planos_corte':
                self.enviar(render_planos_corte())
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
                itens = montar_itens_de_campos(campos)
                resultado = calcular_lote(itens)
                if resultado.get('produtos'):
                    salvar_calculo_excel(resultado)
                    resultado['excel_salvo'] = True
                entradas = [{'lote': i.get('lote', ''), 'produto_digitado': i.get('produto', ''), 'quantidade': i.get('quantidade', '')} for i in itens]
                self.enviar(render_inicio(resultado, entradas))
            elif self.path == '/planos_corte':
                itens = montar_itens_de_campos(campos)
                modo_corte = campos.get('modo_corte', ['encaixe'])[0]
                plano = gerar_plano_corte(itens, modo_corte=modo_corte)
                entradas = [{'lote': i.get('lote', ''), 'produto': i.get('produto', ''), 'quantidade': i.get('quantidade', '')} for i in itens]
                self.enviar(render_planos_corte(plano, entradas, plano.get('modo_corte', 'encaixe')))
            elif self.path == '/baixar_plano_pdf':
                itens = montar_itens_de_campos(campos)
                modo_corte = campos.get('modo_corte', ['encaixe'])[0]
                plano = gerar_plano_corte(itens, modo_corte=modo_corte)
                pdf = gerar_pdf_plano_corte(plano)
                nome = 'plano_de_corte_guilhotina.pdf' if plano.get('modo_corte') == 'guilhotina' else 'plano_de_corte_encaixe.pdf'
                self.enviar_download_bytes(pdf, nome, 'application/pdf')
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
