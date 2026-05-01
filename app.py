import base64
import hashlib
import hmac
import json
import math
import mimetypes
import os
import re
import sqlite3
import time
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


def _resolver_data_dir():
    desejado = Path(os.environ.get("DATA_DIR", str(BASE_DIR))).resolve()
    try:
        desejado.mkdir(parents=True, exist_ok=True)
        teste = desejado / ".teste_permissao"
        teste.write_text("ok", encoding="utf-8")
        teste.unlink(missing_ok=True)
        return desejado
    except Exception as exc:
        print(f"[AVISO] Nao foi possivel usar DATA_DIR={desejado}: {exc}")
        print(f"[AVISO] Usando pasta local do projeto: {BASE_DIR}")
        return BASE_DIR


DATA_DIR = _resolver_data_dir()
DB_PATH = Path(os.environ.get("DB_PATH", str(DATA_DIR / "consumo_chapas.db")))
BASE_XLSX_CANDIDATOS = [
    DATA_DIR / "base_pecas.xlsx",
    BASE_DIR / "base_pecas.xlsx",
    DATA_DIR / "corte_separado_com_tipo_material.xlsx",
    BASE_DIR / "corte_separado_com_tipo_material.xlsx",
    DATA_DIR / "corte_separado_com_tipo_material(3).xlsx",
    BASE_DIR / "corte_separado_com_tipo_material(3).xlsx",
]
BASE_XLSX_PATH = next((p for p in BASE_XLSX_CANDIDATOS if p.exists()), BASE_XLSX_CANDIDATOS[0])
ABA_BASE = os.environ.get("ABA_BASE", "Pecas Separadas")
PORTA_PADRAO = int(os.environ.get("PORT", "8000"))
APP_USER = os.environ.get("APP_USER", "admin")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "troque-esta-senha")
AUTH_ENABLED = os.environ.get("AUTH_ENABLED", "1") != "0"
APP_SECRET = os.environ.get("APP_SECRET", "troque-este-segredo")
KERF_MM_PADRAO = float(os.environ.get("KERF_MM", "4"))
META_APROVEITAMENTO_PADRAO = float(os.environ.get("META_APROVEITAMENTO", "0.95"))
PERMITE_GIRAR_90_PADRAO = os.environ.get("PERMITE_GIRAR_90", "1") != "0"
MAX_PECAS_PLANO = int(os.environ.get("MAX_PECAS_PLANO", "7000"))

LOGO_DOBUE_DATA = "/static/logos/dobue.png"
LOGO_GRAUNA_DATA = "/static/logos/grauna.png"
LOGO_SIMONI_DATA = "/static/logos/simoni_valerio.png"


def normalizar(texto):
    texto = "" if texto is None else str(texto)
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto.upper()).strip()


def limpar_codigo(valor):
    if valor is None:
        return ""
    if isinstance(valor, float) and valor.is_integer():
        valor = int(valor)
    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    texto = re.sub(r"[^0-9A-Za-z]", "", texto)
    return texto.upper()


def numero(valor, padrao=0.0):
    if valor is None:
        return padrao
    if isinstance(valor, (int, float)):
        return float(valor)
    txt = str(valor).strip()
    if not txt:
        return padrao
    if "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except Exception:
        return padrao


def fmt_num(valor, casas=3):
    try:
        v = float(valor)
    except Exception:
        return "0"
    s = f"{v:.{casas}f}".rstrip("0").rstrip(".")
    return s.replace(".", ",")


def fmt_m(valor):
    return fmt_num(valor, 3)


def fmt_mm(valor_m):
    try:
        return str(int(round(float(valor_m) * 1000)))
    except Exception:
        return "0"


def espessura_mm(material, espessura_raw):
    mat = "" if material is None else str(material)
    m = re.search(r"(\d+(?:[,.]\d+)?)", mat)
    if m:
        return int(round(numero(m.group(1))))
    e = numero(espessura_raw)
    if e <= 0:
        return 0
    if e < 1:
        return int(round(e * 100))
    if e < 100:
        return int(round(e))
    return int(round(e))


def html_escape(x):
    return escape("" if x is None else str(x), quote=True)




def cor_hash(valor):
    """Gera uma cor pastel estável para cada código/peça no desenho do plano de corte."""
    texto = str(valor or "PECA")
    digest = hashlib.md5(texto.encode("utf-8")).hexdigest()
    r = 180 + (int(digest[0:2], 16) % 55)
    g = 180 + (int(digest[2:4], 16) % 55)
    b = 180 + (int(digest[4:6], 16) % 55)
    return f"#{r:02x}{g:02x}{b:02x}"

def agora_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_cookie(header):
    out = {}
    if not header:
        return out
    for part in header.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def assinar_token(usuario):
    payload = f"{usuario}|{int(time.time())}"
    sig = hmac.new(APP_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{payload}|{sig}".encode()).decode()


def validar_token(token):
    try:
        raw = base64.urlsafe_b64decode(token.encode()).decode()
        usuario, ts, sig = raw.split("|", 2)
        payload = f"{usuario}|{ts}"
        esperado = hmac.new(APP_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, esperado):
            return False
        return time.time() - int(ts) <= 30 * 24 * 3600
    except Exception:
        return False


# ============================================================
# LEITURA DE XLSX SEM DEPENDÊNCIAS EXTERNAS
# ============================================================

def col_to_idx(ref):
    m = re.match(r"([A-Z]+)", ref or "")
    if not m:
        return 0
    idx = 0
    for ch in m.group(1):
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def ler_xlsx_planilha(caminho, nome_aba=None):
    if not caminho.exists():
        raise FileNotFoundError(f"Planilha não encontrada: {caminho}")

    ns = {
        "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
    }

    with zipfile.ZipFile(caminho, "r") as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            root = ET.fromstring(z.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", ns):
                textos = []
                for t in si.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"):
                    textos.append(t.text or "")
                shared.append("".join(textos))

        wb_root = ET.fromstring(z.read("xl/workbook.xml"))
        rel_root = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        rels = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rel_root.findall("rel:Relationship", ns)}

        sheet_target = None
        first_target = None
        for sh in wb_root.findall(".//a:sheet", ns):
            nome = sh.attrib.get("name", "")
            rid = sh.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = rels.get(rid)
            if first_target is None:
                first_target = target
            if nome_aba is None or normalizar(nome) == normalizar(nome_aba):
                sheet_target = target
                break
        if sheet_target is None:
            sheet_target = first_target
        sheet_path = sheet_target if sheet_target.startswith("xl/") else "xl/" + sheet_target.lstrip("/")

        sheet_root = ET.fromstring(z.read(sheet_path))
        linhas = []
        for row in sheet_root.findall(".//a:sheetData/a:row", ns):
            temp = {}
            max_idx = -1
            for c in row.findall("a:c", ns):
                ci = col_to_idx(c.attrib.get("r", ""))
                max_idx = max(max_idx, ci)
                t = c.attrib.get("t")
                v_el = c.find("a:v", ns)
                if t == "s" and v_el is not None:
                    idx = int(v_el.text or 0)
                    valor = shared[idx] if idx < len(shared) else ""
                elif t == "inlineStr":
                    valor = "".join(te.text or "" for te in c.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"))
                elif v_el is not None:
                    raw = v_el.text
                    try:
                        f = float(raw)
                        valor = int(f) if f.is_integer() else f
                    except Exception:
                        valor = raw
                else:
                    valor = ""
                temp[ci] = valor
            if max_idx >= 0:
                linhas.append([temp.get(i, "") for i in range(max_idx + 1)])
        return linhas


# ============================================================
# BANCO DE DADOS
# ============================================================

def conectar():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def garantir_colunas(conn, tabela, colunas_def):
    """Garante colunas em tabelas antigas do banco persistente do Render."""
    existentes = {row[1] for row in conn.execute(f"PRAGMA table_info({tabela})").fetchall()}
    for coluna, definicao in colunas_def.items():
        if coluna not in existentes:
            conn.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {definicao}")


PECAS_COLUNAS_ESPERADAS = {
    "id", "codigo", "codigo_norm", "descricao", "comprimento", "largura",
    "espessura_raw", "espessura_mm", "material", "produto", "tipo_material", "atualizado_em"
}


def criar_tabela_pecas(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS pecas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT,
        codigo_norm TEXT,
        descricao TEXT,
        comprimento REAL,
        largura REAL,
        espessura_raw REAL,
        espessura_mm INTEGER,
        material TEXT,
        produto TEXT,
        tipo_material TEXT,
        atualizado_em TEXT
    )
    """)


def tabela_pecas_incompativel(conn):
    """Detecta bancos antigos do sistema por produto.

    No Render, o SQLite pode ficar salvo no disco persistente com colunas antigas,
    por exemplo produto_agrupado NOT NULL. A nova versão trabalha por código da peça.
    Se a tabela antiga tiver colunas obrigatórias extras, o INSERT da nova base falha.
    Nesse caso, recriamos somente a tabela pecas e reimportamos a planilha base_pecas.xlsx.
    """
    info = conn.execute("PRAGMA table_info(pecas)").fetchall()
    if not info:
        return False
    nomes = {row[1] for row in info}
    if "produto_agrupado" in nomes or "qtde_peca_produto" in nomes or "tipo_chapa" in nomes:
        return True
    for row in info:
        nome = row[1]
        notnull = bool(row[3])
        default = row[4]
        pk = bool(row[5])
        if nome not in PECAS_COLUNAS_ESPERADAS and notnull and default is None and not pk:
            return True
    return False


def resetar_tabela_pecas(conn):
    conn.execute("DROP INDEX IF EXISTS idx_pecas_codigo_norm")
    conn.execute("DROP INDEX IF EXISTS idx_pecas_material")
    conn.execute("DROP TABLE IF EXISTS pecas")
    criar_tabela_pecas(conn)


def garantir_banco():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    precisa_reimportar_base = False

    with conectar() as conn:
        criar_tabela_pecas(conn)

        if tabela_pecas_incompativel(conn):
            print("[MIGRACAO] Banco antigo detectado. Recriando tabela pecas para a nova logica por codigo.")
            resetar_tabela_pecas(conn)
            precisa_reimportar_base = True

        cols_pecas_antes = {row[1] for row in conn.execute("PRAGMA table_info(pecas)").fetchall()}
        if "codigo_norm" not in cols_pecas_antes:
            precisa_reimportar_base = True

        garantir_colunas(conn, "pecas", {
            "codigo": "TEXT",
            "codigo_norm": "TEXT",
            "descricao": "TEXT",
            "comprimento": "REAL",
            "largura": "REAL",
            "espessura_raw": "REAL",
            "espessura_mm": "INTEGER",
            "material": "TEXT",
            "produto": "TEXT",
            "tipo_material": "TEXT",
            "atualizado_em": "TEXT",
        })

        rows_sem_norm = conn.execute("""
            SELECT id, codigo FROM pecas
            WHERE (codigo_norm IS NULL OR TRIM(codigo_norm) = '')
              AND codigo IS NOT NULL AND TRIM(codigo) <> ''
        """).fetchall()
        for row in rows_sem_norm:
            conn.execute("UPDATE pecas SET codigo_norm=? WHERE id=?", (limpar_codigo(row["codigo"]), row["id"]))

        conn.execute("CREATE INDEX IF NOT EXISTS idx_pecas_codigo_norm ON pecas(codigo_norm)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pecas_material ON pecas(material)")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS chapas (
            material TEXT PRIMARY KEY,
            tipo_material TEXT,
            comprimento REAL NOT NULL DEFAULT 2.75,
            largura REAL NOT NULL DEFAULT 1.85,
            aproveitamento REAL NOT NULL DEFAULT 0.95,
            kerf_mm REAL NOT NULL DEFAULT 4,
            permite_girar INTEGER NOT NULL DEFAULT 1
        )
        """)
        garantir_colunas(conn, "chapas", {
            "material": "TEXT",
            "tipo_material": "TEXT",
            "comprimento": "REAL NOT NULL DEFAULT 2.75",
            "largura": "REAL NOT NULL DEFAULT 1.85",
            "aproveitamento": "REAL NOT NULL DEFAULT 0.95",
            "kerf_mm": "REAL NOT NULL DEFAULT 4",
            "permite_girar": "INTEGER NOT NULL DEFAULT 1",
        })

        conn.execute("""
        CREATE TABLE IF NOT EXISTS calculos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            criado_em TEXT,
            modo TEXT,
            entradas_json TEXT,
            resumo_json TEXT,
            desconhecidos_json TEXT
        )
        """)
        garantir_colunas(conn, "calculos", {
            "criado_em": "TEXT",
            "modo": "TEXT",
            "entradas_json": "TEXT",
            "resumo_json": "TEXT",
            "desconhecidos_json": "TEXT",
        })

        conn.execute("""
        CREATE TABLE IF NOT EXISTS calculo_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calculo_id INTEGER,
            codigo TEXT,
            descricao TEXT,
            material TEXT,
            produto TEXT,
            quantidade INTEGER,
            comprimento REAL,
            largura REAL,
            espessura_mm INTEGER,
            m2_total REAL
        )
        """)
        garantir_colunas(conn, "calculo_itens", {
            "calculo_id": "INTEGER",
            "codigo": "TEXT",
            "descricao": "TEXT",
            "material": "TEXT",
            "produto": "TEXT",
            "quantidade": "INTEGER",
            "comprimento": "REAL",
            "largura": "REAL",
            "espessura_mm": "INTEGER",
            "m2_total": "REAL",
        })

        conn.commit()
        qtd_validos = conn.execute("""
            SELECT COUNT(*) AS c FROM pecas
            WHERE codigo_norm IS NOT NULL AND TRIM(codigo_norm) <> ''
        """).fetchone()["c"]

    if (qtd_validos == 0 or precisa_reimportar_base) and BASE_XLSX_PATH.exists():
        importar_base_xlsx(BASE_XLSX_PATH, apagar=True)

def detectar_colunas(headers):
    mapa = {normalizar(h): i for i, h in enumerate(headers)}
    def achar(*nomes):
        for n in nomes:
            nn = normalizar(n)
            if nn in mapa:
                return mapa[nn]
        for n in nomes:
            nn = normalizar(n)
            for k, i in mapa.items():
                if nn in k or k in nn:
                    return i
        return None
    return {
        "codigo": achar("Código", "Codigo"),
        "descricao": achar("Descrição original", "Descricao original", "Descrição", "Descricao"),
        "comprimento": achar("Comprimento", "Comp."),
        "largura": achar("Largura", "Larg."),
        "espessura": achar("Espessura", "Esp."),
        "material": achar("Material"),
        "produto": achar("Produto"),
        "tipo_material": achar("Tipo Material", "Tipo"),
    }


def importar_base_xlsx(caminho=None, apagar=True):
    caminho = Path(caminho or BASE_XLSX_PATH)
    linhas = ler_xlsx_planilha(caminho, ABA_BASE)
    if not linhas:
        raise ValueError("Planilha vazia ou aba não encontrada.")
    headers = linhas[0]
    cols = detectar_colunas(headers)
    obrig = ["codigo", "descricao", "comprimento", "largura", "material"]
    faltando = [c for c in obrig if cols.get(c) is None]
    if faltando:
        raise ValueError(f"Colunas obrigatórias não encontradas: {', '.join(faltando)}")

    registros = []
    materiais = {}
    for row in linhas[1:]:
        def get(col):
            idx = cols.get(col)
            return row[idx] if idx is not None and idx < len(row) else ""
        codigo = limpar_codigo(get("codigo"))
        if not codigo:
            continue
        comp = numero(get("comprimento"))
        larg = numero(get("largura"))
        if comp <= 0 or larg <= 0:
            continue
        material = str(get("material") or "").strip()
        tipo_material = str(get("tipo_material") or "").strip()
        descricao = str(get("descricao") or "").strip()
        produto = str(get("produto") or "").strip()
        esp_raw = numero(get("espessura"))
        esp_mm = espessura_mm(material, esp_raw)
        registros.append((str(codigo), codigo, descricao, comp, larg, esp_raw, esp_mm, material, produto, tipo_material, agora_iso()))
        if material:
            materiais[material] = tipo_material

    with conectar() as conn:
        if apagar:
            conn.execute("DELETE FROM pecas")
        conn.executemany("""
            INSERT INTO pecas (codigo, codigo_norm, descricao, comprimento, largura, espessura_raw, espessura_mm, material, produto, tipo_material, atualizado_em)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, registros)
        for material, tipo_material in materiais.items():
            existe = conn.execute("SELECT material FROM chapas WHERE material=?", (material,)).fetchone()
            if not existe:
                conn.execute("""
                    INSERT INTO chapas (material, tipo_material, comprimento, largura, aproveitamento, kerf_mm, permite_girar)
                    VALUES (?, ?, 2.75, 1.85, ?, ?, ?)
                """, (material, tipo_material, META_APROVEITAMENTO_PADRAO, KERF_MM_PADRAO, 1 if PERMITE_GIRAR_90_PADRAO else 0))
        conn.commit()
    return len(registros), len(materiais)


def obter_chapas_dict():
    with conectar() as conn:
        rows = conn.execute("SELECT * FROM chapas ORDER BY material").fetchall()
    return {r["material"]: dict(r) for r in rows}


def buscar_peca(codigo):
    cn = limpar_codigo(codigo)
    if not cn:
        return None
    with conectar() as conn:
        row = conn.execute("SELECT * FROM pecas WHERE codigo_norm=? ORDER BY id LIMIT 1", (cn,)).fetchone()
    return dict(row) if row else None


def listar_pecas(q="", limite=300):
    termo = normalizar(q)
    with conectar() as conn:
        if termo:
            like = f"%{termo}%"
            rows = conn.execute("""
                SELECT * FROM pecas
                WHERE codigo_norm LIKE ? OR UPPER(descricao) LIKE ? OR UPPER(material) LIKE ? OR UPPER(produto) LIKE ?
                ORDER BY codigo_norm LIMIT ?
            """, (f"%{limpar_codigo(q)}%", like, like, like, limite)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM pecas ORDER BY codigo_norm LIMIT ?", (limite,)).fetchall()
    return [dict(r) for r in rows]


def contagens_base():
    with conectar() as conn:
        pecas = conn.execute("SELECT COUNT(*) AS c FROM pecas").fetchone()["c"]
        mats = conn.execute("SELECT COUNT(DISTINCT material) AS c FROM pecas").fetchone()["c"]
        cods = conn.execute("SELECT COUNT(DISTINCT codigo_norm) AS c FROM pecas").fetchone()["c"]
    return pecas, cods, mats


# ============================================================
# CÁLCULO E PLANOS
# ============================================================

def calcular_por_codigos(entradas):
    chapas = obter_chapas_dict()
    itens, desconhecidos, resumo = [], [], {}
    for ent in entradas:
        codigo = limpar_codigo(ent.get("codigo"))
        qtd = int(numero(ent.get("quantidade"), 0))
        if not codigo and qtd <= 0:
            continue
        if not codigo or qtd <= 0:
            desconhecidos.append({"codigo": codigo or ent.get("codigo", ""), "quantidade": qtd, "motivo": "Código ou quantidade inválida"})
            continue
        peca = buscar_peca(codigo)
        if not peca:
            desconhecidos.append({"codigo": codigo, "quantidade": qtd, "motivo": "Código não encontrado na base de dados"})
            continue
        material = peca["material"] or "SEM MATERIAL"
        ch = chapas.get(material) or {"material": material, "tipo_material": peca.get("tipo_material", ""), "comprimento": 2.75, "largura": 1.85, "aproveitamento": 0.95, "kerf_mm": 4, "permite_girar": 1}
        area_unit = peca["comprimento"] * peca["largura"]
        m2_total = area_unit * qtd
        item = {
            "codigo": peca["codigo"], "descricao": peca["descricao"], "produto": peca["produto"],
            "material": material, "tipo_material": peca["tipo_material"],
            "comprimento": peca["comprimento"], "largura": peca["largura"],
            "espessura_mm": peca["espessura_mm"], "quantidade": qtd,
            "m2_unit": area_unit, "m2_total": m2_total,
        }
        itens.append(item)
        r = resumo.setdefault(material, {"material": material, "tipo_material": peca.get("tipo_material", ""), "qtd_pecas": 0, "m2_total": 0.0, "comprimento_chapa": ch["comprimento"], "largura_chapa": ch["largura"], "aproveitamento": ch["aproveitamento"], "kerf_mm": ch["kerf_mm"], "permite_girar": ch["permite_girar"]})
        r["qtd_pecas"] += qtd
        r["m2_total"] += m2_total
    for r in resumo.values():
        area_chapa = r["comprimento_chapa"] * r["largura_chapa"]
        area_util = area_chapa * r["aproveitamento"]
        r["area_chapa"] = area_chapa
        r["area_util"] = area_util
        r["chapas_area"] = int(math.ceil(r["m2_total"] / area_util)) if area_util > 0 else 0
        r["aproveitamento_estimado"] = (r["m2_total"] / (r["chapas_area"] * area_chapa)) if r["chapas_area"] else 0
    return itens, list(resumo.values()), desconhecidos


def expandir_itens_para_plano(itens, material):
    total = sum(int(i["quantidade"]) for i in itens if i["material"] == material)
    if total > MAX_PECAS_PLANO:
        raise ValueError(f"Quantidade de peças para {material} ({total}) ultrapassa o limite de {MAX_PECAS_PLANO} para desenho do plano.")
    pecas, count = [], 0
    for item in itens:
        if item["material"] != material:
            continue
        for _ in range(int(item["quantidade"])):
            count += 1
            pecas.append({"codigo": item["codigo"], "descricao": item["descricao"], "produto": item["produto"], "material": item["material"], "w": float(item["comprimento"]), "h": float(item["largura"]), "espessura_mm": item["espessura_mm"], "idx": count})
    return pecas


def escolher_orientacao(peca, rect, permite_girar):
    opcoes = [(peca["w"], peca["h"], False)]
    if permite_girar and abs(peca["w"] - peca["h"]) > 1e-9:
        opcoes.append((peca["h"], peca["w"], True))
    ok = [o for o in opcoes if o[0] <= rect["w"] + 1e-9 and o[1] <= rect["h"] + 1e-9]
    if not ok:
        return None
    return min(ok, key=lambda o: (rect["w"] - o[0]) * (rect["h"] - o[1]))


def finalizar_chapas(chapas, chapa_w, chapa_h, modo):
    area = chapa_w * chapa_h
    for i, ch in enumerate(chapas, 1):
        a = sum(p["w_draw"] * p["h_draw"] for p in ch["pecas"])
        ch.update({"numero": i, "area_pecas": a, "aproveitamento": a / area if area else 0, "sobra_m2": max(area - a, 0), "modo": modo, "chapa_w": chapa_w, "chapa_h": chapa_h})
    return chapas


def plano_encaixe_livre(pecas, chapa_w, chapa_h, kerf, permite_girar):
    pecas = sorted(pecas, key=lambda p: p["w"] * p["h"], reverse=True)
    chapas = []
    def nova_chapa():
        return {"pecas": [], "livres": [{"x": 0.0, "y": 0.0, "w": chapa_w, "h": chapa_h}], "sequencia": []}
    for peca in pecas:
        colocado = False
        for ch in chapas:
            melhor = None
            for ri, rect in enumerate(ch["livres"]):
                ori = escolher_orientacao(peca, rect, permite_girar)
                if ori:
                    w, h, girada = ori
                    score = rect["w"] * rect["h"] - w * h
                    if melhor is None or score < melhor[0]:
                        melhor = (score, ri, rect, w, h, girada)
            if melhor:
                _, ri, rect, w, h, girada = melhor
                x, y = rect["x"], rect["y"]
                p = dict(peca); p.update({"x": x, "y": y, "w_draw": w, "h_draw": h, "girada": girada})
                ch["pecas"].append(p)
                ch["sequencia"].append(f"Encaixar código {p['codigo']} em X={int(round(x*1000))} mm / Y={int(round(y*1000))} mm, medida {int(round(w*1000))} x {int(round(h*1000))} mm.")
                ch["livres"].pop(ri)
                rem_right_w = rect["w"] - w - kerf
                rem_bottom_h = rect["h"] - h - kerf
                if rem_right_w > 0.02:
                    ch["livres"].append({"x": x + w + kerf, "y": y, "w": rem_right_w, "h": h})
                if rem_bottom_h > 0.02:
                    ch["livres"].append({"x": x, "y": y + h + kerf, "w": rect["w"], "h": rem_bottom_h})
                ch["livres"] = sorted(ch["livres"], key=lambda r: (r["y"], r["x"], -r["w"] * r["h"]))
                colocado = True
                break
        if not colocado:
            ch = nova_chapa(); rect = ch["livres"][0]
            ori = escolher_orientacao(peca, rect, permite_girar)
            if not ori:
                raise ValueError(f"Peça código {peca['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
            w, h, girada = ori
            p = dict(peca); p.update({"x": 0.0, "y": 0.0, "w_draw": w, "h_draw": h, "girada": girada})
            ch["pecas"].append(p)
            if chapa_w - w - kerf > 0.02:
                ch["livres"].append({"x": w + kerf, "y": 0.0, "w": chapa_w - w - kerf, "h": h})
            if chapa_h - h - kerf > 0.02:
                ch["livres"].append({"x": 0.0, "y": h + kerf, "w": chapa_w, "h": chapa_h - h - kerf})
            ch["sequencia"].append(f"Abrir nova chapa e encaixar código {p['codigo']} na origem, medida {int(round(w*1000))} x {int(round(h*1000))} mm.")
            chapas.append(ch)
    return finalizar_chapas(chapas, chapa_w, chapa_h, "encaixe")


def plano_guilhotina_faixas(pecas, chapa_w, chapa_h, kerf, permite_girar):
    pecas = sorted(pecas, key=lambda p: p["w"] * p["h"], reverse=True)
    chapas, ch = [], {"pecas": [], "faixas": [], "sequencia": []}
    def iniciar_faixa(ch, y, h):
        faixa = {"y": y, "h": h, "x": 0.0, "pecas": []}
        ch["faixas"].append(faixa)
        ch["sequencia"].append(f"Cortar faixa guilhotinada de {int(round(h*1000))} mm a partir de Y={int(round(y*1000))} mm.")
        return faixa
    for peca in pecas:
        colocado = False
        for faixa in ch["faixas"]:
            opcoes = [(peca["w"], peca["h"], False)]
            if permite_girar:
                opcoes.append((peca["h"], peca["w"], True))
            opcoes = [o for o in opcoes if o[1] <= faixa["h"] + 1e-9 and faixa["x"] + o[0] <= chapa_w + 1e-9]
            if opcoes:
                w, h, girada = min(opcoes, key=lambda o: (faixa["h"] - o[1], chapa_w - (faixa["x"] + o[0])))
                x = faixa["x"]
                p = dict(peca); p.update({"x": x, "y": faixa["y"], "w_draw": w, "h_draw": h, "girada": girada})
                ch["pecas"].append(p); faixa["pecas"].append(p); faixa["x"] += w + kerf
                ch["sequencia"].append(f"Na faixa {int(round(faixa['h']*1000))} mm, cortar código {p['codigo']} com {int(round(w*1000))} x {int(round(h*1000))} mm.")
                colocado = True
                break
        if colocado:
            continue
        opcoes_nova = [(peca["w"], peca["h"], False)]
        if permite_girar:
            opcoes_nova.append((peca["h"], peca["w"], True))
        opcoes_nova = [o for o in opcoes_nova if o[0] <= chapa_w + 1e-9 and o[1] <= chapa_h + 1e-9]
        if not opcoes_nova:
            raise ValueError(f"Peça código {peca['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
        w, h, girada = min(opcoes_nova, key=lambda o: o[1])
        y_usado = 0.0
        if ch["faixas"]:
            ult = ch["faixas"][-1]
            y_usado = ult["y"] + ult["h"] + kerf
        if y_usado + h > chapa_h + 1e-9:
            chapas.append(ch); ch = {"pecas": [], "faixas": [], "sequencia": []}; y_usado = 0.0
        faixa = iniciar_faixa(ch, y_usado, h)
        p = dict(peca); p.update({"x": 0.0, "y": y_usado, "w_draw": w, "h_draw": h, "girada": girada})
        ch["pecas"].append(p); faixa["pecas"].append(p); faixa["x"] = w + kerf
        ch["sequencia"].append(f"Na nova faixa, cortar código {p['codigo']} com {int(round(w*1000))} x {int(round(h*1000))} mm.")
    if ch["pecas"]:
        chapas.append(ch)
    return finalizar_chapas(chapas, chapa_w, chapa_h, "guilhotina")


def gerar_planos(itens, resumo, modo):
    planos = []
    for r in resumo:
        material = r["material"]
        pecas = expandir_itens_para_plano(itens, material)
        if not pecas:
            continue
        chapa_w, chapa_h = float(r["comprimento_chapa"]), float(r["largura_chapa"])
        kerf = float(r["kerf_mm"]) / 1000.0
        permite = bool(r["permite_girar"])
        chapas = plano_guilhotina_faixas(pecas, chapa_w, chapa_h, kerf, permite) if modo == "guilhotina" else plano_encaixe_livre(pecas, chapa_w, chapa_h, kerf, permite)
        planos.append({"material": material, "chapas": chapas, "modo": modo})
    return planos


def salvar_historico(entradas, itens, resumo, desconhecidos, modo):
    with conectar() as conn:
        cur = conn.execute("""
            INSERT INTO calculos (criado_em, modo, entradas_json, resumo_json, desconhecidos_json)
            VALUES (?, ?, ?, ?, ?)
        """, (agora_iso(), modo, json.dumps(entradas, ensure_ascii=False), json.dumps(resumo, ensure_ascii=False), json.dumps(desconhecidos, ensure_ascii=False)))
        cid = cur.lastrowid
        for it in itens:
            conn.execute("""
                INSERT INTO calculo_itens (calculo_id, codigo, descricao, material, produto, quantidade, comprimento, largura, espessura_mm, m2_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (cid, it["codigo"], it["descricao"], it["material"], it["produto"], it["quantidade"], it["comprimento"], it["largura"], it["espessura_mm"], it["m2_total"]))
        conn.commit()
    return cid


# ============================================================
# EXPORTAÇÃO XLSX SIMPLES
# ============================================================

def xlsx_col(n):
    s = ""
    while n >= 0:
        s = chr(n % 26 + 65) + s
        n = n // 26 - 1
    return s


def xml_escape(s):
    return escape("" if s is None else str(s), quote=False)


def worksheet_xml(rows):
    xml = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>']
    xml.append('<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>')
    for r_idx, row in enumerate(rows, 1):
        xml.append(f'<row r="{r_idx}">')
        for c_idx, val in enumerate(row):
            ref = f"{xlsx_col(c_idx)}{r_idx}"
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                xml.append(f'<c r="{ref}"><v>{val}</v></c>')
            else:
                xml.append(f'<c r="{ref}" t="inlineStr"><is><t>{xml_escape(val)}</t></is></c>')
        xml.append('</row>')
    xml.append('</sheetData></worksheet>')
    return "".join(xml)


def gerar_xlsx_historico():
    with conectar() as conn:
        calc = conn.execute("SELECT * FROM calculos ORDER BY id DESC").fetchall()
        itens = conn.execute("""
            SELECT ci.*, c.criado_em, c.modo FROM calculo_itens ci JOIN calculos c ON c.id = ci.calculo_id ORDER BY ci.calculo_id DESC, ci.id
        """).fetchall()
    rows_calc = [["ID", "Data/Hora", "Modo", "Entradas", "Resumo", "Códigos não encontrados"]]
    for c in calc:
        rows_calc.append([c["id"], c["criado_em"], c["modo"], c["entradas_json"], c["resumo_json"], c["desconhecidos_json"]])
    rows_itens = [["Cálculo", "Data/Hora", "Modo", "Código", "Descrição", "Material", "Produto", "Qtd", "Comprimento", "Largura", "Espessura mm", "m² total"]]
    for it in itens:
        rows_itens.append([it["calculo_id"], it["criado_em"], it["modo"], it["codigo"], it["descricao"], it["material"], it["produto"], it["quantidade"], it["comprimento"], it["largura"], it["espessura_mm"], it["m2_total"]])
    out = DATA_DIR / "historico_corte_por_codigo.xlsx"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>''')
        z.writestr("_rels/.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>''')
        z.writestr("xl/workbook.xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Historico" sheetId="1" r:id="rId1"/><sheet name="Itens" sheetId="2" r:id="rId2"/></sheets></workbook>''')
        z.writestr("xl/_rels/workbook.xml.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/></Relationships>''')
        z.writestr("xl/worksheets/sheet1.xml", worksheet_xml(rows_calc))
        z.writestr("xl/worksheets/sheet2.xml", worksheet_xml(rows_itens))
    return out


# ============================================================
# HTML
# ============================================================

CSS = """
:root{--bg:#f3f6fb;--card:#fff;--text:#0f1f35;--muted:#53657e;--blue:#1f5eea;--border:#dfe6f0;--danger:#b42318;--ok:#027a48}*{box-sizing:border-box}body{font-family:Arial,Helvetica,sans-serif;margin:0;background:var(--bg);color:var(--text)}.wrap{max-width:1220px;margin:0 auto;padding:20px}.header{display:flex;align-items:center;gap:20px;margin-bottom:18px}.logo-main{height:78px;max-width:260px;object-fit:contain}.header h1{font-size:26px;margin:0 0 4px}.header p{margin:0;color:var(--muted)}.nav{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:18px}.nav a,.btn{display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--border);background:#fff;color:#064ceb;text-decoration:none;border-radius:10px;padding:10px 14px;font-weight:700;cursor:pointer;font-size:14px}.btn.primary{background:var(--blue);color:#fff;border-color:var(--blue)}.card{background:var(--card);border:1px solid var(--border);border-radius:18px;padding:18px;margin:14px 0;box-shadow:0 8px 24px rgba(17,33,61,.06)}.card h2{margin:0 0 16px;font-size:24px}.hint{color:var(--muted);font-size:14px}.row{display:grid;grid-template-columns:180px 1fr 160px 48px;gap:10px;margin:8px 0}.row2{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}input,select,textarea{width:100%;border:1px solid var(--border);border-radius:10px;padding:11px 12px;font-size:14px;background:#fff}textarea{min-height:260px;resize:vertical;font-family:Consolas,Menlo,monospace;line-height:1.45}.paste-grid{display:grid;grid-template-columns:1fr 180px;gap:12px;margin-top:12px}.paste-grid label{display:block;font-weight:800;margin:0 0 7px}.minihelp{background:#f8fafc;border:1px dashed var(--border);border-radius:12px;padding:10px 12px;margin:10px 0;color:#53657e;font-size:13px}.counter{font-size:13px;color:#53657e;margin-top:6px}.toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:10px}table{width:100%;border-collapse:separate;border-spacing:0;border:1px solid var(--border);border-radius:12px;overflow:hidden;background:#fff}th,td{padding:10px 12px;border-bottom:1px solid var(--border);text-align:left;font-size:14px;vertical-align:top}th{background:#f8fafc;font-weight:800}tr:last-child td{border-bottom:none}.num{text-align:right}.badge{display:inline-block;border-radius:999px;background:#eef2ff;color:#123cbd;padding:4px 8px;font-size:12px}.badge.danger{background:#fee4e2;color:#b42318}.badge.ok{background:#d1fadf;color:#027a48}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}.stat{background:#f8fafc;border:1px solid var(--border);border-radius:14px;padding:14px}.stat b{font-size:24px}.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}.footer{display:flex;justify-content:center;align-items:center;gap:26px;margin:26px auto 10px;flex-wrap:wrap}.footer img{height:58px;max-width:230px;object-fit:contain;filter:grayscale(.1);opacity:.88}.sheet-card{page-break-inside:avoid}.svgwrap{overflow:auto;border:1px solid var(--border);border-radius:12px;background:#fff;padding:8px}.cutseq{font-size:13px;color:#334155;columns:2}.login{max-width:430px;margin:80px auto}@media (max-width:760px){.paste-grid{grid-template-columns:1fr}.row2{grid-template-columns:1fr}}@media print{.nav,.actions,.no-print,.btn{display:none!important}.wrap{max-width:100%;padding:0}.card{box-shadow:none;border:1px solid #999;page-break-inside:avoid}body{background:#fff}}
"""


def layout(titulo, corpo, ativo=""):
    nav = '<div class="nav no-print"><a href="/">Corte por código</a><a href="/banco">Banco de peças</a><a href="/configurar_chapas">Configurar chapas</a><a href="/historico">Histórico</a><a href="/baixar_historico_xlsx">Baixar histórico Excel</a><a href="/logout">Sair</a></div>'
    return f'''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{html_escape(titulo)}</title><style>{CSS}</style></head><body><div class="wrap"><div class="header"><img class="logo-main" src="{LOGO_DOBUE_DATA}" alt="Dobuê"><div><h1>Plano de corte por código de peça</h1><p>Sistema web para cálculo de chapas, plano de corte e conferência de códigos não cadastrados.</p></div></div>{nav}{corpo}<div class="footer no-print"><img src="{LOGO_GRAUNA_DATA}" alt="Graúna"><img src="{LOGO_SIMONI_DATA}" alt="Simoni & Valério"></div></div></body></html>'''


def pagina_login(msg=""):
    erro = f'<p class="badge danger">{html_escape(msg)}</p>' if msg else ""
    return f'''<!doctype html><html><head><meta charset="utf-8"><title>Login</title><style>{CSS}</style></head><body><div class="login card"><img src="{LOGO_DOBUE_DATA}" style="width:100%;max-height:140px;object-fit:contain"><h2>Entrar no sistema</h2>{erro}<form method="post" action="/login"><p><input name="usuario" placeholder="Usuário" autofocus></p><p><input name="senha" type="password" placeholder="Senha"></p><button class="btn primary" type="submit">Entrar</button></form><p class="hint">Configure usuário e senha no Render pelas variáveis APP_USER e APP_PASSWORD.</p></div></body></html>'''


def datalist_codigos():
    opts = []
    for p in listar_pecas("", 1200):
        label = f"{p['descricao']} | {p['material']} | {fmt_mm(p['comprimento'])}x{fmt_mm(p['largura'])}"
        opts.append(f'<option value="{html_escape(p["codigo"])}">{html_escape(label)}</option>')
    return '<datalist id="codigos">' + "\n".join(opts) + '</datalist>'


def pagina_corte(result_html=""):
    pecas, cods, mats = contagens_base()
    corpo = f'''
    <div class="grid"><div class="stat"><span class="hint">Peças cadastradas</span><br><b>{pecas}</b></div><div class="stat"><span class="hint">Códigos únicos</span><br><b>{cods}</b></div><div class="stat"><span class="hint">Materiais</span><br><b>{mats}</b></div></div>
    <form class="card" method="post" action="/calcular">
        <h2>Entrada do corte</h2>
        <p class="hint">Informe os <b>códigos das peças</b> e as <b>quantidades que devem ser cortadas</b>. Agora você pode colar várias linhas direto do Excel.</p>
        <div class="minihelp"><b>Como colar:</b> copie duas colunas do Excel, por exemplo <b>Código</b> e <b>Quantidade</b>, e cole no campo <b>Códigos da peça</b>. O sistema separa automaticamente as quantidades. Também pode colar uma coluna de códigos e outra coluna de quantidades separadamente.</div>
        <div class="paste-grid">
            <div><label for="codigos_texto">Código da peça</label><textarea name="codigos_texto" id="codigos_texto" spellcheck="false" placeholder="Cole aqui vários códigos, um por linha.&#10;Exemplo:&#10;3008002&#10;3008003&#10;3008004"></textarea><div id="contador_codigos" class="counter">0 códigos informados</div></div>
            <div><label for="quantidades_texto">Quantidade</label><textarea name="quantidades_texto" id="quantidades_texto" spellcheck="false" placeholder="Cole aqui as quantidades, uma por linha.&#10;Exemplo:&#10;40&#10;12&#10;8"></textarea><div id="contador_qtds" class="counter">0 quantidades informadas</div></div>
        </div>
        <div class="toolbar no-print"><button type="button" class="btn" onclick="limparEntrada()">Limpar entrada</button><button type="button" class="btn" onclick="exemploEntrada()">Preencher exemplo</button></div>
        {datalist_codigos()}
        <div class="row2" style="margin-top:12px"><div><label class="hint">Tipo de plano de corte</label><select name="modo_corte"><option value="encaixe">Encaixe livre</option><option value="guilhotina">Guilhotina por faixas</option></select></div><div><label class="hint">Salvar histórico</label><select name="salvar_historico"><option value="1">Sim</option><option value="0">Não</option></select></div><div><label class="hint">Ação</label><select name="acao"><option value="calcular">Apenas calcular consumo</option><option value="plano">Calcular e gerar plano de corte</option></select></div></div>
        <div class="actions"><button class="btn primary" type="submit">Executar</button><button type="button" class="btn" onclick="window.print()">Imprimir / salvar PDF</button></div>
    </form>{result_html}
    <script>
    const codigosEl = document.getElementById('codigos_texto');
    const qtdsEl = document.getElementById('quantidades_texto');
    function linhasValidas(txt){{return txt.split(/
?
/).map(x=>x.trim()).filter(x=>x.length>0);}}
    function atualizarContadores(){{
        document.getElementById('contador_codigos').textContent = linhasValidas(codigosEl.value).length + ' códigos informados';
        document.getElementById('contador_qtds').textContent = linhasValidas(qtdsEl.value).length + ' quantidades informadas';
    }}
    function limparEntrada(){{codigosEl.value='';qtdsEl.value='';atualizarContadores();codigosEl.focus();}}
    function exemploEntrada(){{codigosEl.value='3008002
3008003
3008004';qtdsEl.value='40
12
8';atualizarContadores();}}
    codigosEl.addEventListener('paste', function(e){{
        const texto = (e.clipboardData || window.clipboardData).getData('text');
        if(texto && (texto.includes('	') || texto.includes(';'))){{
            e.preventDefault();
            const codigos=[]; const qtds=[];
            texto.split(/
?
/).forEach(linha=>{{
                const partes = linha.split(/	|;/).map(p=>p.trim()).filter(Boolean);
                if(partes.length>=2){{
                    codigos.push(partes[0]);
                    qtds.push(partes[partes.length-1]);
                }} else if(partes.length===1) {{ codigos.push(partes[0]); }}
            }});
            codigosEl.value = codigos.join('
');
            if(qtds.length) qtdsEl.value = qtds.join('
');
            atualizarContadores();
        }} else {{ setTimeout(atualizarContadores, 0); }}
    }});
    codigosEl.addEventListener('input', atualizarContadores); qtdsEl.addEventListener('input', atualizarContadores); atualizarContadores();
    </script>
    '''
    return layout("Corte por código", corpo, "corte")


def tabela_resumo(resumo):
    if not resumo:
        return ""
    rows, total_chapas, total_m2 = "", 0, 0
    for r in resumo:
        total_chapas += r["chapas_area"]; total_m2 += r["m2_total"]
        rows += f'<tr><td>{html_escape(r["material"])}</td><td>{fmt_m(r["comprimento_chapa"])} x {fmt_m(r["largura_chapa"])} m</td><td class="num">{r["qtd_pecas"]}</td><td class="num">{fmt_num(r["m2_total"],3)}</td><td class="num">{fmt_num(r["aproveitamento"]*100,1)}%</td><td class="num">{fmt_num(r["area_util"],3)}</td><td class="num"><b>{r["chapas_area"]}</b></td></tr>'
    return f'<div class="card"><h2>Chapas utilizadas por material</h2><div class="grid"><div class="stat"><span class="hint">Total de chapas estimado por área</span><br><b>{total_chapas}</b></div><div class="stat"><span class="hint">m² total cortado</span><br><b>{fmt_num(total_m2,3)}</b></div></div><table><thead><tr><th>Material</th><th>Medida chapa</th><th class="num">Qtde peças</th><th class="num">m² cortado</th><th class="num">Aproveit.</th><th class="num">m² útil/chapa</th><th class="num">Chapas usadas</th></tr></thead><tbody>{rows}</tbody></table></div>'


def tabela_itens(itens):
    if not itens:
        return ""
    rows = ""
    for it in itens:
        rows += f'<tr><td>{html_escape(it["codigo"])}</td><td>{html_escape(it["descricao"])}</td><td>{html_escape(it["material"])}</td><td>{html_escape(it["produto"])}</td><td class="num">{fmt_mm(it["comprimento"])} x {fmt_mm(it["largura"])}</td><td class="num">{it["espessura_mm"]}</td><td class="num">{it["quantidade"]}</td><td class="num">{fmt_num(it["m2_total"],3)}</td></tr>'
    return f'<div class="card"><h2>Detalhamento das peças calculadas</h2><table><thead><tr><th>Código</th><th>Descrição</th><th>Material</th><th>Produto</th><th class="num">Medida mm</th><th class="num">Esp. mm</th><th class="num">Qtde</th><th class="num">m² total</th></tr></thead><tbody>{rows}</tbody></table></div>'


def tabela_desconhecidos(desconhecidos):
    if not desconhecidos:
        return ""
    rows = "".join(f'<tr><td>{html_escape(d["codigo"])}</td><td class="num">{d["quantidade"]}</td><td><span class="badge danger">{html_escape(d["motivo"])}</span></td></tr>' for d in desconhecidos)
    return f'<div class="card"><h2>Códigos não encontrados na base</h2><p class="hint">Estes códigos foram informados pelo usuário, mas não existem na planilha-base importada. Eles não entram no cálculo até serem cadastrados na base.</p><table><thead><tr><th>Código</th><th class="num">Quantidade</th><th>Motivo</th></tr></thead><tbody>{rows}</tbody></table></div>'


def _linhas_coladas(texto):
    return [l.strip() for l in str(texto or "").replace("\r\n", "\n").replace("\r", "\n").split("\n") if l.strip()]


def _parse_linha_codigo_quantidade(linha):
    partes = [p.strip() for p in re.split(r"\t|;", str(linha or "")) if p.strip()]
    if len(partes) < 2:
        return None
    codigo = partes[0]
    qtd = ""
    for ptxt in reversed(partes[1:]):
        if numero(ptxt, -1) >= 0:
            qtd = ptxt
            break
    if not qtd:
        qtd = partes[-1]
    return {"codigo": codigo, "quantidade": qtd}


def entradas_do_formulario(form):
    """Aceita entrada em bloco colada do Excel e mantém compatibilidade com os campos antigos."""
    entradas = []
    codigos_texto = form.get("codigos_texto", [""])[0]
    quantidades_texto = form.get("quantidades_texto", [""])[0]
    linhas_codigos = _linhas_coladas(codigos_texto)
    linhas_qtds = _linhas_coladas(quantidades_texto)

    if linhas_codigos:
        if not linhas_qtds and any(("\t" in l or ";" in l) for l in linhas_codigos):
            for linha in linhas_codigos:
                item = _parse_linha_codigo_quantidade(linha)
                if item:
                    entradas.append(item)
                else:
                    entradas.append({"codigo": linha, "quantidade": ""})
        else:
            maior = max(len(linhas_codigos), len(linhas_qtds))
            for i in range(maior):
                codigo = linhas_codigos[i] if i < len(linhas_codigos) else ""
                qtd = linhas_qtds[i] if i < len(linhas_qtds) else ""
                item_linha = _parse_linha_codigo_quantidade(codigo)
                if item_linha and not str(qtd).strip():
                    codigo, qtd = item_linha["codigo"], item_linha["quantidade"]
                if str(codigo).strip() or str(qtd).strip():
                    entradas.append({"codigo": codigo, "quantidade": qtd})

    codigos = form.get("codigo", [])
    quantidades = form.get("quantidade", [])
    for c, q in zip(codigos, quantidades):
        if str(c).strip() or str(q).strip():
            entradas.append({"codigo": c, "quantidade": q})

    return entradas

def svg_chapa(chapa):
    chapa_w, chapa_h = chapa.get("chapa_w", 2.75), chapa.get("chapa_h", 1.85)
    W, H = 980, max(260, int(980 * chapa_h / chapa_w))
    def sx(x): return x / chapa_w * W
    def sy(y): return y / chapa_h * H
    parts = [f'<svg width="100%" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg"><rect x="0" y="0" width="{W}" height="{H}" fill="#fff" stroke="#111827" stroke-width="2"/>']
    for p in chapa["pecas"]:
        x, y, w, h = sx(p["x"]), sy(p["y"]), sx(p["w_draw"]), sy(p["h_draw"])
        label = f"{p['codigo']} | {int(round(p['w_draw']*1000))}x{int(round(p['h_draw']*1000))}"
        fs = max(8, min(16, int(min(w/8, h/3))))
        parts.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{w:.2f}" height="{h:.2f}" fill="{cor_hash(p["codigo"])}" stroke="#334155" stroke-width="1"/><text x="{x+4:.2f}" y="{y+fs+4:.2f}" font-size="{fs}" font-family="Arial" fill="#111827">{html_escape(label)}</text>')
    parts.append("</svg>")
    return "".join(parts)


def html_planos(planos):
    if not planos:
        return ""
    html = ['<div class="card"><h2>Plano de corte visual</h2><p class="hint">Desenho chapa por chapa. Para salvar em PDF, use o botão Imprimir / salvar PDF.</p></div>']
    for grupo in planos:
        material = grupo["material"]
        modo = "Guilhotina por faixas" if grupo["modo"] == "guilhotina" else "Encaixe livre"
        for ch in grupo["chapas"]:
            html.append(f'<div class="card sheet-card"><h2>{html_escape(material)} — Chapa {ch["numero"]} — {html_escape(modo)}</h2><div class="grid"><div class="stat"><span class="hint">Aproveitamento real desta chapa</span><br><b>{fmt_num(ch["aproveitamento"]*100,1)}%</b></div><div class="stat"><span class="hint">Área das peças</span><br><b>{fmt_num(ch["area_pecas"],3)} m²</b></div><div class="stat"><span class="hint">Sobra estimada</span><br><b>{fmt_num(ch["sobra_m2"],3)} m²</b></div></div><div class="svgwrap">{svg_chapa(ch)}</div><h3>Sequência sugerida</h3><div class="cutseq"><ol>{"".join("<li>"+html_escape(s)+"</li>" for s in ch.get("sequencia", [])[:120])}</ol></div></div>')
    return "\n".join(html)


def montar_resultado(entradas, itens, resumo, desconhecidos, planos=None, calculo_id=None):
    msg = f'<div class="card"><span class="badge ok">Histórico salvo no cálculo #{calculo_id}</span></div>' if calculo_id else ""
    return msg + tabela_desconhecidos(desconhecidos) + tabela_resumo(resumo) + tabela_itens(itens) + (html_planos(planos or []) if planos else "")


def pagina_banco(q="", msg=""):
    pecas = listar_pecas(q, 600)
    rows = ""
    for p in pecas:
        rows += f'<tr><td>{html_escape(p["codigo"])}</td><td>{html_escape(p["descricao"])}</td><td>{html_escape(p["material"])}</td><td>{html_escape(p["produto"])}</td><td class="num">{fmt_mm(p["comprimento"])} x {fmt_mm(p["largura"])}</td><td class="num">{p["espessura_mm"]}</td></tr>'
    alert = f'<p class="badge ok">{html_escape(msg)}</p>' if msg else ""
    pc, cods, mats = contagens_base()
    corpo = f'<div class="card"><h2>Banco de peças</h2>{alert}<p class="hint">Base atual: {html_escape(str(BASE_XLSX_PATH))}. Aba usada: <b>{html_escape(ABA_BASE)}</b>.</p><div class="grid"><div class="stat"><span class="hint">Peças</span><br><b>{pc}</b></div><div class="stat"><span class="hint">Códigos únicos</span><br><b>{cods}</b></div><div class="stat"><span class="hint">Materiais</span><br><b>{mats}</b></div></div><form method="get" action="/banco" class="actions"><input name="q" value="{html_escape(q)}" placeholder="Buscar por código, descrição, produto ou material" style="max-width:460px"><button class="btn" type="submit">Buscar</button></form><form method="post" action="/atualizar_base" class="actions"><button class="btn primary" type="submit">Atualizar banco pela planilha base</button></form></div><div class="card"><h2>Peças cadastradas</h2><table><thead><tr><th>Código</th><th>Descrição</th><th>Material</th><th>Produto</th><th class="num">Medida mm</th><th class="num">Esp. mm</th></tr></thead><tbody>{rows}</tbody></table></div>'
    return layout("Banco de peças", corpo, "banco")


def pagina_config(msg=""):
    rows = ""
    for mat, ch in obter_chapas_dict().items():
        safe = html_escape(mat)
        rows += f'<tr><td><input name="material" value="{safe}" readonly></td><td><input name="comprimento_{safe}" value="{fmt_m(ch["comprimento"])}"></td><td><input name="largura_{safe}" value="{fmt_m(ch["largura"])}"></td><td><input name="aproveitamento_{safe}" value="{fmt_num(ch["aproveitamento"]*100,1)}"></td><td><input name="kerf_{safe}" value="{fmt_num(ch["kerf_mm"],1)}"></td><td><select name="girar_{safe}"><option value="1" {"selected" if ch["permite_girar"] else ""}>Sim</option><option value="0" {"" if ch["permite_girar"] else "selected"}>Não</option></select></td></tr>'
    alert = f'<p class="badge ok">{html_escape(msg)}</p>' if msg else ""
    corpo = f'<form class="card" method="post" action="/salvar_chapas"><h2>Configurar chapas</h2>{alert}<p class="hint">Configure a medida da chapa por material. Medidas em metros. Kerf em milímetros.</p><table><thead><tr><th>Material</th><th>Comprimento m</th><th>Largura m</th><th>Aproveitamento %</th><th>Serra/Kerf mm</th><th>Girar 90°</th></tr></thead><tbody>{rows}</tbody></table><div class="actions"><button class="btn primary" type="submit">Salvar configurações</button></div></form>'
    return layout("Configurar chapas", corpo, "config")


def pagina_historico():
    with conectar() as conn:
        calc = conn.execute("SELECT * FROM calculos ORDER BY id DESC LIMIT 100").fetchall()
    rows = ""
    for c in calc:
        try:
            resumo = json.loads(c["resumo_json"] or "[]")
            chapas = sum(r.get("chapas_area", 0) for r in resumo)
            m2 = sum(r.get("m2_total", 0) for r in resumo)
        except Exception:
            chapas, m2 = 0, 0
        rows += f'<tr><td>{c["id"]}</td><td>{html_escape(c["criado_em"])}</td><td>{html_escape(c["modo"])}</td><td class="num">{chapas}</td><td class="num">{fmt_num(m2,3)}</td></tr>'
    corpo = f'<div class="card"><h2>Histórico</h2><div class="actions"><a class="btn primary" href="/baixar_historico_xlsx">Baixar histórico Excel</a></div><table><thead><tr><th>ID</th><th>Data/Hora</th><th>Modo</th><th class="num">Chapas</th><th class="num">m²</th></tr></thead><tbody>{rows}</tbody></table></div>'
    return layout("Histórico", corpo, "hist")


class App(BaseHTTPRequestHandler):
    server_version = "AppCortePorCodigo/2.0"
    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))
    def enviar(self, html, status=200, content_type="text/html; charset=utf-8"):
        data = html.encode("utf-8")
        self.send_response(status); self.send_header("Content-Type", content_type); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def redirect(self, path):
        self.send_response(303); self.send_header("Location", path); self.end_headers()
    def autenticado(self):
        if not AUTH_ENABLED:
            return True
        token = parse_cookie(self.headers.get("Cookie")).get("auth")
        return validar_token(token) if token else False
    def exigir_auth(self):
        if not self.autenticado():
            self.enviar(pagina_login(), 200); return False
        return True
    def body_form(self):
        length = int(self.headers.get("Content-Length", "0")); raw = self.rfile.read(length).decode("utf-8"); return parse_qs(raw, keep_blank_values=True)

    def servir_arquivo_estatico(self, path):
        rel = path[len("/static/"):].strip("/")
        arquivo = (STATIC_DIR / rel).resolve()
        try:
            arquivo.relative_to(STATIC_DIR.resolve())
        except Exception:
            return self.enviar("Acesso negado", 403, "text/plain; charset=utf-8")
        if not arquivo.exists() or not arquivo.is_file():
            return self.enviar("Arquivo não encontrado", 404, "text/plain; charset=utf-8")
        data = arquivo.read_bytes()
        tipo = mimetypes.guess_type(str(arquivo))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path; qs = parse_qs(urlparse(self.path).query)
        if path.startswith("/static/"):
            return self.servir_arquivo_estatico(path)
        if path == "/login": return self.enviar(pagina_login())
        if path == "/logout":
            self.send_response(303); self.send_header("Set-Cookie", "auth=; Path=/; Max-Age=0"); self.send_header("Location", "/login"); self.end_headers(); return
        if not self.exigir_auth(): return
        if path == "/": return self.enviar(pagina_corte())
        if path == "/banco": return self.enviar(pagina_banco(qs.get("q", [""])[0], qs.get("msg", [""])[0]))
        if path == "/configurar_chapas": return self.enviar(pagina_config(qs.get("msg", [""])[0]))
        if path == "/historico": return self.enviar(pagina_historico())
        if path == "/baixar_historico_xlsx":
            arquivo = gerar_xlsx_historico(); data = arquivo.read_bytes()
            self.send_response(200); self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); self.send_header("Content-Disposition", 'attachment; filename="historico_corte_por_codigo.xlsx"'); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data); return
        self.enviar("Página não encontrada", 404)
    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/login":
            form = self.body_form(); usuario = form.get("usuario", [""])[0]; senha = form.get("senha", [""])[0]
            if usuario == APP_USER and senha == APP_PASSWORD:
                token = assinar_token(usuario); self.send_response(303); self.send_header("Set-Cookie", f"auth={token}; Path=/; HttpOnly; SameSite=Lax"); self.send_header("Location", "/"); self.end_headers()
            else:
                self.enviar(pagina_login("Usuário ou senha inválidos."), 401)
            return
        if not self.exigir_auth(): return
        if path == "/calcular":
            form = self.body_form(); entradas = entradas_do_formulario(form)
            modo = form.get("modo_corte", ["encaixe"])[0]; acao = form.get("acao", ["calcular"])[0]; salvar = form.get("salvar_historico", ["1"])[0] == "1"
            try:
                itens, resumo, desconhecidos = calcular_por_codigos(entradas)
                planos = gerar_planos(itens, resumo, modo) if acao == "plano" and itens else None
                cid = salvar_historico(entradas, itens, resumo, desconhecidos, modo if acao == "plano" else "calculo") if salvar else None
                self.enviar(pagina_corte(montar_resultado(entradas, itens, resumo, desconhecidos, planos, cid)))
            except Exception as exc:
                self.enviar(pagina_corte(f'<div class="card"><h2>Erro no cálculo</h2><p class="badge danger">{html_escape(exc)}</p></div>'), 500)
            return
        if path == "/atualizar_base":
            try:
                qtd, mats = importar_base_xlsx(BASE_XLSX_PATH, apagar=True); self.redirect(f"/banco?msg=Base+atualizada:+{qtd}+pecas+e+{mats}+materiais")
            except Exception as exc:
                self.enviar(pagina_banco(msg=f"Erro ao atualizar: {exc}"), 500)
            return
        if path == "/salvar_chapas":
            form = self.body_form(); materiais = form.get("material", [])
            with conectar() as conn:
                for mat in materiais:
                    comp = numero(form.get(f"comprimento_{mat}", ["2,75"])[0]); larg = numero(form.get(f"largura_{mat}", ["1,85"])[0]); aprov = numero(form.get(f"aproveitamento_{mat}", ["95"])[0]) / 100.0; kerf = numero(form.get(f"kerf_{mat}", ["4"])[0]); girar = int(form.get(f"girar_{mat}", ["1"])[0])
                    conn.execute("UPDATE chapas SET comprimento=?, largura=?, aproveitamento=?, kerf_mm=?, permite_girar=? WHERE material=?", (comp, larg, aprov, kerf, girar, mat))
                conn.commit()
            self.redirect("/configurar_chapas?msg=Configuracoes+salvas"); return
        self.enviar("Rota POST não encontrada", 404)


def main():
    garantir_banco()
    server = ThreadingHTTPServer(("0.0.0.0", PORTA_PADRAO), App)
    print(f"Servidor iniciado em http://0.0.0.0:{PORTA_PADRAO}")
    print(f"Banco de dados: {DB_PATH}")
    print(f"Planilha base: {BASE_XLSX_PATH}")
    server.serve_forever()


if __name__ == "__main__":
    main()
