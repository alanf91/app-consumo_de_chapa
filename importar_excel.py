import sqlite3
import zipfile
import xml.etree.ElementTree as ET
import re
import unicodedata
from pathlib import Path

ARQUIVO_EXCEL = Path('Consumo de chapa por lote.xlsx')
BANCO = Path('consumo_chapas.db')
NS = {'a': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main', 'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'}


def normalizar(texto):
    texto = '' if texto is None else str(texto)
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'\s+', ' ', texto.upper()).strip()


def to_float(valor, padrao=0.0):
    if valor is None or valor == '':
        return padrao
    if isinstance(valor, (int, float)):
        return float(valor)
    valor = str(valor).strip().replace('.', '').replace(',', '.') if ',' in str(valor) else str(valor).strip()
    try:
        return float(valor)
    except ValueError:
        return padrao


def coluna_para_numero(coluna):
    numero = 0
    for letra in coluna:
        numero = numero * 26 + ord(letra.upper()) - 64
    return numero


def numero_para_coluna(numero):
    texto = ''
    while numero:
        numero, resto = divmod(numero - 1, 26)
        texto = chr(65 + resto) + texto
    return texto


def ler_shared_strings(z):
    if 'xl/sharedStrings.xml' not in z.namelist():
        return []
    raiz = ET.fromstring(z.read('xl/sharedStrings.xml'))
    textos = []
    for si in raiz.findall('a:si', NS):
        textos.append(''.join(t.text or '' for t in si.findall('.//a:t', NS)))
    return textos


def mapa_abas(z):
    workbook = ET.fromstring(z.read('xl/workbook.xml'))
    rels = ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
    relmap = {rel.attrib['Id']: rel.attrib['Target'] for rel in rels}
    abas = {}
    for aba in workbook.find('a:sheets', NS):
        nome = aba.attrib['name']
        rid = aba.attrib['{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id']
        alvo = relmap[rid]
        if not alvo.startswith('xl/'):
            alvo = 'xl/' + alvo
        abas[nome] = alvo
    return abas


def ler_aba(z, caminho, shared):
    raiz = ET.fromstring(z.read(caminho))
    celulas = {}
    for c in raiz.findall('.//a:c', NS):
        ref = c.attrib.get('r')
        tipo = c.attrib.get('t')
        v = c.find('a:v', NS)
        if not ref or v is None:
            continue
        valor = v.text
        if tipo == 's':
            valor = shared[int(valor)]
        elif tipo == 'b':
            valor = bool(int(valor))
        else:
            try:
                numero = float(valor)
                valor = int(numero) if numero.is_integer() else numero
            except (TypeError, ValueError):
                pass
        celulas[ref] = valor
    return celulas


def valor(celulas, coluna, linha, padrao=None):
    return celulas.get(f'{coluna}{linha}', padrao)


def criar_tabelas(con):
    con.executescript('''
    DROP TABLE IF EXISTS produtos;
    DROP TABLE IF EXISTS pecas;
    DROP TABLE IF EXISTS chapas_config;

    CREATE TABLE produtos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL UNIQUE,
        nome_norm TEXT NOT NULL UNIQUE,
        pecas_unitarias REAL NOT NULL DEFAULT 0,
        m2_unitario REAL NOT NULL DEFAULT 0,
        tipos_chapa_usados TEXT,
        nomes_originais TEXT
    );

    CREATE TABLE pecas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT,
        produto_agrupado TEXT NOT NULL,
        produto_norm TEXT NOT NULL,
        peca TEXT,
        produto_original TEXT,
        comprimento REAL NOT NULL DEFAULT 0,
        largura REAL NOT NULL DEFAULT 0,
        espessura REAL NOT NULL DEFAULT 0,
        material TEXT,
        tipo_chapa TEXT,
        qtde_peca_produto REAL NOT NULL DEFAULT 0,
        m2_peca_unit REAL NOT NULL DEFAULT 0,
        descr_familia TEXT,
        produto_extraido TEXT
    );

    CREATE TABLE chapas_config (
        tipo_chapa TEXT PRIMARY KEY,
        comprimento REAL NOT NULL DEFAULT 2.75,
        largura REAL NOT NULL DEFAULT 1.85,
        aproveitamento REAL NOT NULL DEFAULT 0.95
    );

    CREATE INDEX idx_pecas_produto_norm ON pecas(produto_norm);
    CREATE INDEX idx_pecas_tipo_chapa ON pecas(tipo_chapa);
    ''')


def importar(arquivo_excel=ARQUIVO_EXCEL, banco=BANCO):
    arquivo_excel = Path(arquivo_excel)
    banco = Path(banco)
    if not arquivo_excel.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {arquivo_excel}')

    with zipfile.ZipFile(arquivo_excel) as z:
        shared = ler_shared_strings(z)
        abas = mapa_abas(z)
        base = ler_aba(z, abas['BASE_PRODUTOS'], shared)
        listas = ler_aba(z, abas['LISTAS'], shared)
        calculo = ler_aba(z, abas['CALCULO_LOTE'], shared)

    con = sqlite3.connect(banco)
    try:
        criar_tabelas(con)

        for linha in range(2, 5000):
            nome = valor(listas, 'A', linha)
            if not nome:
                continue
            con.execute('''
                INSERT OR REPLACE INTO produtos
                (nome, nome_norm, pecas_unitarias, m2_unitario, tipos_chapa_usados, nomes_originais)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                str(nome).strip(), normalizar(nome), to_float(valor(listas, 'B', linha)),
                to_float(valor(listas, 'C', linha)), str(valor(listas, 'D', linha, '') or ''),
                str(valor(listas, 'E', linha, '') or '')
            ))

        for linha in range(2, 10000):
            produto = valor(base, 'B', linha)
            if not produto:
                continue
            comp = to_float(valor(base, 'E', linha))
            larg = to_float(valor(base, 'F', linha))
            qtd_peca = to_float(valor(base, 'J', linha), 1.0)
            m2_unit = to_float(valor(base, 'K', linha), comp * larg)
            con.execute('''
                INSERT INTO pecas
                (codigo, produto_agrupado, produto_norm, peca, produto_original, comprimento, largura, espessura,
                 material, tipo_chapa, qtde_peca_produto, m2_peca_unit, descr_familia, produto_extraido)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                str(valor(base, 'A', linha, '') or ''), str(produto).strip(), normalizar(produto),
                str(valor(base, 'C', linha, '') or ''), str(valor(base, 'D', linha, '') or ''),
                comp, larg, to_float(valor(base, 'G', linha)), str(valor(base, 'H', linha, '') or ''),
                str(valor(base, 'I', linha, '') or ''), qtd_peca, m2_unit,
                str(valor(base, 'O', linha, '') or ''), str(valor(base, 'P', linha, '') or '')
            ))

        con.execute('''
            INSERT OR IGNORE INTO produtos (nome, nome_norm, pecas_unitarias, m2_unitario, tipos_chapa_usados, nomes_originais)
            SELECT produto_agrupado, produto_norm,
                   SUM(qtde_peca_produto),
                   SUM(qtde_peca_produto * m2_peca_unit),
                   GROUP_CONCAT(DISTINCT tipo_chapa),
                   GROUP_CONCAT(DISTINCT produto_extraido)
            FROM pecas
            GROUP BY produto_norm
        ''')

        tipos_configurados = set()
        for linha in range(10, 22):
            tipo = valor(calculo, 'V', linha)
            if isinstance(tipo, str) and tipo.strip() and tipo.strip() not in {'0', 'SEM MEDIDA'}:
                tipo = tipo.strip()
                tipos_configurados.add(normalizar(tipo))
                con.execute('''
                    INSERT OR REPLACE INTO chapas_config (tipo_chapa, comprimento, largura, aproveitamento)
                    VALUES (?, ?, ?, ?)
                ''', (tipo, to_float(valor(calculo, 'Z', linha), 2.75), to_float(valor(calculo, 'AA', linha), 1.85), to_float(valor(calculo, 'AB', linha), 0.95)))

        tipos = con.execute('''
            SELECT DISTINCT tipo_chapa FROM pecas
            WHERE tipo_chapa IS NOT NULL AND TRIM(tipo_chapa) <> '' AND tipo_chapa <> 'SEM MEDIDA'
            ORDER BY tipo_chapa
        ''').fetchall()
        for (tipo,) in tipos:
            con.execute('''
                INSERT OR IGNORE INTO chapas_config (tipo_chapa, comprimento, largura, aproveitamento)
                VALUES (?, 2.75, 1.85, 0.95)
            ''', (tipo,))

        con.commit()
        total_produtos = con.execute('SELECT COUNT(*) FROM produtos').fetchone()[0]
        total_pecas = con.execute('SELECT COUNT(*) FROM pecas').fetchone()[0]
        total_chapas = con.execute('SELECT COUNT(*) FROM chapas_config').fetchone()[0]
        return total_produtos, total_pecas, total_chapas
    finally:
        con.close()


if __name__ == '__main__':
    totais = importar()
    print(f'Banco criado: {BANCO}')
    print(f'Produtos: {totais[0]} | Peças: {totais[1]} | Tipos de chapa: {totais[2]}')
