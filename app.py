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
# Limites de segurança para não travar o Render em lotes grandes.
MAX_SEGUNDOS_OTIMIZACAO = float(os.environ.get("MAX_SEGUNDOS_OTIMIZACAO", "8"))
MAX_LINHAS_SEQUENCIA = int(os.environ.get("MAX_LINHAS_SEQUENCIA", "60"))
MAX_SEGUNDOS_COMPACTACAO = float(os.environ.get("MAX_SEGUNDOS_COMPACTACAO", "5"))

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


def adicionar_sequencia(ch, texto):
    """Guarda uma sequência curta e estável para não travar a página/PDF."""
    seq = ch.setdefault("sequencia", [])
    if len(seq) < MAX_LINHAS_SEQUENCIA:
        seq.append(str(texto))
    elif len(seq) == MAX_LINHAS_SEQUENCIA:
        seq.append("Sequência resumida: demais cortes seguem o mesmo padrão de encaixe/faixa.")


def _tempo_esgotado(inicio):
    return (time.time() - inicio) >= MAX_SEGUNDOS_OTIMIZACAO


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


def _rect_intersect(a, b):
    return not (
        a["x"] + a["w"] <= b["x"] + 1e-9 or
        b["x"] + b["w"] <= a["x"] + 1e-9 or
        a["y"] + a["h"] <= b["y"] + 1e-9 or
        b["y"] + b["h"] <= a["y"] + 1e-9
    )


def _rect_contem(a, b):
    """Retorna True se o retângulo a contém totalmente o retângulo b."""
    return (
        b["x"] >= a["x"] - 1e-9 and
        b["y"] >= a["y"] - 1e-9 and
        b["x"] + b["w"] <= a["x"] + a["w"] + 1e-9 and
        b["y"] + b["h"] <= a["y"] + a["h"] + 1e-9
    )


def _prunar_livres(livres, min_dim=0.01):
    """Remove retângulos livres pequenos, duplicados ou contidos em outros.

    Esta etapa evita áreas livres sobrepostas que causavam aproveitamento acima
    de 100% e peças sobrepostas no desenho.
    """
    filtrados = []
    for r in livres:
        if r["w"] <= min_dim or r["h"] <= min_dim:
            continue
        rr = {"x": round(r["x"], 6), "y": round(r["y"], 6), "w": round(r["w"], 6), "h": round(r["h"], 6)}
        if rr["w"] <= min_dim or rr["h"] <= min_dim:
            continue
        filtrados.append(rr)

    saida = []
    for i, r in enumerate(filtrados):
        contido = False
        for j, o in enumerate(filtrados):
            if i != j and _rect_contem(o, r):
                contido = True
                break
        if not contido and r not in saida:
            saida.append(r)
    return sorted(saida, key=lambda r: (r["y"], r["x"], r["w"] * r["h"]))



def _orientacoes_possiveis(peca, permite_girar):
    opcoes = [(float(peca["w"]), float(peca["h"]), False)]
    if permite_girar and abs(float(peca["w"]) - float(peca["h"])) > 1e-9:
        opcoes.append((float(peca["h"]), float(peca["w"]), True))
    return opcoes


def escolher_orientacao(peca, rect, permite_girar):
    opcoes = _orientacoes_possiveis(peca, permite_girar)
    ok = [o for o in opcoes if o[0] <= rect["w"] + 1e-9 and o[1] <= rect["h"] + 1e-9]
    if not ok:
        return None
    return min(ok, key=lambda o: (min(rect["w"] - o[0], rect["h"] - o[1]), (rect["w"] * rect["h"]) - (o[0] * o[1])))


def _validar_chapa_sem_sobreposicao(chapa, chapa_w, chapa_h):
    """Validação defensiva: nenhuma peça pode sair da chapa ou sobrepor outra."""
    pecas = chapa.get("pecas", [])
    for i, p in enumerate(pecas):
        if p["x"] < -1e-9 or p["y"] < -1e-9 or p["x"] + p["w_draw"] > chapa_w + 1e-9 or p["y"] + p["h_draw"] > chapa_h + 1e-9:
            raise ValueError(f"Erro no plano: peça {p.get('codigo','')} ficou fora da chapa.")
        a = {"x": p["x"], "y": p["y"], "w": p["w_draw"], "h": p["h_draw"]}
        for j in range(i + 1, len(pecas)):
            q = pecas[j]
            b = {"x": q["x"], "y": q["y"], "w": q["w_draw"], "h": q["h_draw"]}
            if _rect_intersect(a, b):
                raise ValueError(f"Erro no plano: peças {p.get('codigo','')} e {q.get('codigo','')} ficaram sobrepostas.")


def finalizar_chapas(chapas, chapa_w, chapa_h, modo, estrategia=""):
    area = chapa_w * chapa_h
    for i, ch in enumerate(chapas, 1):
        _validar_chapa_sem_sobreposicao(ch, chapa_w, chapa_h)
        a = sum(float(p["w_draw"]) * float(p["h_draw"]) for p in ch.get("pecas", []))
        aproveitamento = a / area if area else 0
        if aproveitamento > 1.0001:
            raise ValueError(f"Erro no plano da chapa {i}: aproveitamento calculado acima de 100%.")
        ch.update({
            "numero": i,
            "area_pecas": a,
            "aproveitamento": min(aproveitamento, 1.0),
            "sobra_m2": max(area - a, 0),
            "modo": modo,
            "estrategia": estrategia,
            "chapa_w": chapa_w,
            "chapa_h": chapa_h,
        })
    return chapas


def _contato_score(ch, x, y, w, h, chapa_w, chapa_h):
    score = 0.0
    if abs(x) <= 1e-9:
        score += h
    if abs(y) <= 1e-9:
        score += w
    if abs((x + w) - chapa_w) <= 1e-9:
        score += h
    if abs((y + h) - chapa_h) <= 1e-9:
        score += w
    for q in ch.get("pecas", []):
        qx, qy, qw, qh = q["x"], q["y"], q["w_draw"], q["h_draw"]
        if abs(x - (qx + qw)) <= 1e-9 or abs((x + w) - qx) <= 1e-9:
            score += max(0.0, min(y + h, qy + qh) - max(y, qy))
        if abs(y - (qy + qh)) <= 1e-9 or abs((y + h) - qy) <= 1e-9:
            score += max(0.0, min(x + w, qx + qw) - max(x, qx))
    return score


def _pontuar_posicao(criterio, ch, rect, x, y, w, h, chapa_w, chapa_h):
    sobra_w = rect["w"] - w
    sobra_h = rect["h"] - h
    sobra_area = rect["w"] * rect["h"] - w * h
    short_side = min(sobra_w, sobra_h)
    long_side = max(sobra_w, sobra_h)
    contato = _contato_score(ch, x, y, w, h, chapa_w, chapa_h)
    ocupacao_atual = sum(p["w_draw"] * p["h_draw"] for p in ch.get("pecas", []))
    if criterio == "baf":
        return (sobra_area, short_side, long_side, y, x)
    if criterio == "bssf":
        return (short_side, long_side, sobra_area, y, x)
    if criterio == "blsf":
        return (long_side, short_side, sobra_area, y, x)
    if criterio == "contato":
        return (-contato, sobra_area, short_side, y, x)
    if criterio == "top_left":
        return (y, x, short_side, sobra_area)
    if criterio == "densidade":
        return (-ocupacao_atual, sobra_area, short_side, y, x)
    return (sobra_area, short_side, y, x)


def _candidato_interfere_com_pecas(ch, x, y, w, h, kerf):
    """Verifica colisão respeitando o kerf, sem bloquear peças encostadas no limite correto.

    A versão anterior era rígida demais no arredondamento e podia impedir que uma peça
    fosse posicionada exatamente após a folga da serra, abrindo chapas desnecessárias.
    """
    eps = 1e-6
    for q in ch.get("pecas", []):
        ox = float(q["x"])
        oy = float(q["y"])
        ow = float(q["w_draw"]) + kerf
        oh = float(q["h_draw"]) + kerf
        if (x < ox + ow - eps and x + w > ox + eps and y < oy + oh - eps and y + h > oy + eps):
            return True
    return False


def _melhor_posicao_maxrects(ch, peca, chapa_w, chapa_h, kerf, permite_girar, criterio):
    melhor = None
    for rect in ch.get("livres", []):
        for w, h, girada in _orientacoes_possiveis(peca, permite_girar):
            if w > rect["w"] + 1e-9 or h > rect["h"] + 1e-9:
                continue
            x, y = rect["x"], rect["y"]
            if x + w > chapa_w + 1e-9 or y + h > chapa_h + 1e-9:
                continue
            if _candidato_interfere_com_pecas(ch, x, y, w, h, kerf):
                continue
            score = _pontuar_posicao(criterio, ch, rect, x, y, w, h, chapa_w, chapa_h)
            cand = (score, rect, x, y, w, h, girada)
            if melhor is None or cand[0] < melhor[0]:
                melhor = cand
    return melhor


def _split_free_rectangles(livres, usado, min_dim):
    novos = []
    for fr in livres:
        if not _rect_intersect(fr, usado):
            novos.append(fr)
            continue
        fr_right = fr["x"] + fr["w"]
        fr_bottom = fr["y"] + fr["h"]
        us_right = usado["x"] + usado["w"]
        us_bottom = usado["y"] + usado["h"]
        if usado["x"] > fr["x"] + 1e-9:
            novos.append({"x": fr["x"], "y": fr["y"], "w": usado["x"] - fr["x"], "h": fr["h"]})
        if us_right < fr_right - 1e-9:
            novos.append({"x": us_right, "y": fr["y"], "w": fr_right - us_right, "h": fr["h"]})
        if usado["y"] > fr["y"] + 1e-9:
            novos.append({"x": fr["x"], "y": fr["y"], "w": fr["w"], "h": usado["y"] - fr["y"]})
        if us_bottom < fr_bottom - 1e-9:
            novos.append({"x": fr["x"], "y": us_bottom, "w": fr["w"], "h": fr_bottom - us_bottom})
    return _prunar_livres(novos, min_dim=min_dim)


def _inserir_maxrects(ch, peca, pos, chapa_w, chapa_h, kerf):
    _, rect, x, y, w, h, girada = pos
    p = dict(peca)
    p.update({"x": x, "y": y, "w_draw": w, "h_draw": h, "girada": girada})
    ch["pecas"].append(p)
    adicionar_sequencia(ch,
        f"Encaixar código {p['codigo']} em X={int(round(x*1000))} mm / Y={int(round(y*1000))} mm, "
        f"medida {int(round(w*1000))} x {int(round(h*1000))} mm."
    )
    usado = {"x": x, "y": y, "w": min(w + kerf, chapa_w - x), "h": min(h + kerf, chapa_h - y)}
    ch["livres"] = _split_free_rectangles(ch.get("livres", []), usado, min_dim=max(kerf, 0.003))


def _ordenar_pecas(pecas, ordenacao):
    if ordenacao == "area_desc":
        return sorted(pecas, key=lambda p: (p["w"] * p["h"], max(p["w"], p["h"]), min(p["w"], p["h"])), reverse=True)
    if ordenacao == "lado_maior_desc":
        return sorted(pecas, key=lambda p: (max(p["w"], p["h"]), p["w"] * p["h"]), reverse=True)
    if ordenacao == "largura_desc":
        return sorted(pecas, key=lambda p: (p["w"], p["h"], p["w"] * p["h"]), reverse=True)
    if ordenacao == "altura_desc":
        return sorted(pecas, key=lambda p: (p["h"], p["w"], p["w"] * p["h"]), reverse=True)
    if ordenacao == "perimetro_desc":
        return sorted(pecas, key=lambda p: (p["w"] + p["h"], p["w"] * p["h"]), reverse=True)
    if ordenacao == "quadradas_primeiro":
        return sorted(pecas, key=lambda p: (abs(p["w"] - p["h"]), -(p["w"] * p["h"])))
    if ordenacao == "estreitas_depois":
        return sorted(pecas, key=lambda p: (min(p["w"], p["h"]), p["w"] * p["h"]), reverse=True)
    return list(pecas)


def _chapa_vazia(chapa_w, chapa_h):
    return {"pecas": [], "livres": [{"x": 0.0, "y": 0.0, "w": chapa_w, "h": chapa_h}], "sequencia": []}


# ============================================================
# ALGORITMO OPORTUNIDADES DE SOBRA
# ============================================================

def _assinatura_peca(p):
    return (
        str(p.get("codigo", "")),
        str(p.get("descricao", "")),
        str(p.get("produto", "")),
        str(p.get("material", "")),
        round(float(p.get("w", 0)), 6),
        round(float(p.get("h", 0)), 6),
        int(p.get("espessura_mm", 0) or 0),
    )


def _agrupar_pecas_otimizacao(pecas, ordenacao="area_desc"):
    grupos = {}
    for p in pecas:
        key = _assinatura_peca(p)
        if key not in grupos:
            base = dict(p)
            grupos[key] = {"peca": base, "count": 0, "area": float(p["w"]) * float(p["h"])}
        grupos[key]["count"] += 1
    saida = list(grupos.values())

    def chave(g):
        p = g["peca"]
        area = g["area"]
        w, h = float(p["w"]), float(p["h"])
        if ordenacao == "lado_maior_desc":
            return (max(w, h), area, g["count"])
        if ordenacao == "largura_desc":
            return (w, h, area, g["count"])
        if ordenacao == "altura_desc":
            return (h, w, area, g["count"])
        if ordenacao == "estreitas_depois":
            return (min(w, h), area, g["count"])
        return (area, max(w, h), min(w, h), g["count"])

    saida.sort(key=chave, reverse=True)
    return saida


def _grupos_restantes(grupos):
    return sum(max(0, int(g.get("count", 0))) for g in grupos)


def _area_ocupada_chapa(ch):
    return sum(float(p["w_draw"]) * float(p["h_draw"]) for p in ch.get("pecas", []))


def _melhor_posicao_grupo_oportunidade(ch, peca, chapa_w, chapa_h, kerf, permite_girar):
    melhor = None
    area_chapa = chapa_w * chapa_h
    ocupada = _area_ocupada_chapa(ch)
    for rect in ch.get("livres", []):
        for w, h, girada in _orientacoes_possiveis(peca, permite_girar):
            if w > rect["w"] + 1e-9 or h > rect["h"] + 1e-9:
                continue
            x, y = rect["x"], rect["y"]
            if x + w > chapa_w + 1e-9 or y + h > chapa_h + 1e-9:
                continue
            if _candidato_interfere_com_pecas(ch, x, y, w, h, kerf):
                continue

            area = w * h
            sobra_area_rect = max(rect["w"] * rect["h"] - area, 0.0)
            sobra_w = max(rect["w"] - w, 0.0)
            sobra_h = max(rect["h"] - h, 0.0)
            short_side = min(sobra_w, sobra_h)
            long_side = max(sobra_w, sobra_h)
            contato = _contato_score(ch, x, y, w, h, chapa_w, chapa_h)
            util_depois = (ocupada + area) / area_chapa if area_chapa else 0.0

            # Lógica principal: enxergar oportunidade de sobra.
            # 1) Prefere peça que preenche bem o retângulo livre.
            # 2) Prefere peça que encosta em bordas/outras peças.
            # 3) Prefere ocupar mais área sem gerar retalho ruim.
            preenchimento_rect = area / (rect["w"] * rect["h"]) if rect["w"] * rect["h"] else 0.0
            score = (
                -preenchimento_rect,
                short_side,
                sobra_area_rect,
                -contato,
                -area,
                -util_depois,
                y,
                x,
            )
            cand = (score, rect, x, y, w, h, girada)
            if melhor is None or cand[0] < melhor[0]:
                melhor = cand
    return melhor


def _selecionar_semente_chapa(grupos, chapa_w, chapa_h, permite_girar):
    for gi, g in enumerate(grupos):
        if g["count"] <= 0:
            continue
        p = g["peca"]
        if escolher_orientacao(p, {"x": 0.0, "y": 0.0, "w": chapa_w, "h": chapa_h}, permite_girar):
            return gi
    return None


def _melhor_grupo_para_preencher_sobra(ch, grupos, chapa_w, chapa_h, kerf, permite_girar, inicio):
    melhor = None
    for gi, g in enumerate(grupos):
        if g["count"] <= 0:
            continue
        if _tempo_esgotado(inicio):
            break
        p = g["peca"]
        pos = _melhor_posicao_grupo_oportunidade(ch, p, chapa_w, chapa_h, kerf, permite_girar)
        if pos is None:
            continue
        # Em empate, usa o grupo com mais peças restantes para esvaziar repetição.
        score = (pos[0], -g["count"], gi)
        if melhor is None or score < melhor[0]:
            melhor = (score, gi, pos)
    return melhor


def _plano_oportunidades_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao="area_desc"):
    """Preenche uma chapa procurando oportunidades entre TODAS as peças restantes.

    Diferente do fluxo anterior, ele não segue cegamente a ordem das peças.
    A cada espaço livre, ele pergunta: qual peça restante encaixa melhor aqui?
    Isso é o que evita abrir chapas novas enquanto há sobras úteis nas chapas anteriores.
    """
    inicio = time.time()
    grupos = _agrupar_pecas_otimizacao(pecas, ordenacao)
    chapas = []
    area_chapa = chapa_w * chapa_h

    while _grupos_restantes(grupos) > 0:
        if _tempo_esgotado(inicio) and chapas:
            # Se esgotar o tempo no meio do processo, finaliza o restante com MaxRects rápido
            restantes = []
            for g in grupos:
                for _ in range(max(0, int(g["count"]))):
                    restantes.append(dict(g["peca"]))
                g["count"] = 0
            if restantes:
                try:
                    complemento = _plano_maxrects_estrategia(restantes, chapa_w, chapa_h, kerf, permite_girar, "area_desc", "baf")
                    chapas.extend(complemento)
                except Exception:
                    for p in restantes:
                        ch = _chapa_vazia(chapa_w, chapa_h)
                        pos = _melhor_posicao_maxrects(ch, p, chapa_w, chapa_h, kerf, permite_girar, "baf")
                        if pos is None:
                            raise ValueError(f"Peça código {p['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
                        adicionar_sequencia(ch, "Abrir nova chapa por limite de tempo.")
                        _inserir_maxrects(ch, p, pos, chapa_w, chapa_h, kerf)
                        chapas.append(ch)
            break

        seed_idx = _selecionar_semente_chapa(grupos, chapa_w, chapa_h, permite_girar)
        if seed_idx is None:
            raise ValueError("Existe peça que não cabe na medida de chapa configurada.")

        ch = _chapa_vazia(chapa_w, chapa_h)
        seed = grupos[seed_idx]["peca"]
        pos = _melhor_posicao_maxrects(ch, seed, chapa_w, chapa_h, kerf, permite_girar, "baf")
        if pos is None:
            raise ValueError(f"Peça código {seed['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
        adicionar_sequencia(ch, "Abrir nova chapa e iniciar preenchimento por oportunidades de sobra.")
        _inserir_maxrects(ch, seed, pos, chapa_w, chapa_h, kerf)
        grupos[seed_idx]["count"] -= 1

        sem_melhora = 0
        while _grupos_restantes(grupos) > 0:
            if _tempo_esgotado(inicio):
                break
            antes = _area_ocupada_chapa(ch)
            melhor = _melhor_grupo_para_preencher_sobra(ch, grupos, chapa_w, chapa_h, kerf, permite_girar, inicio)
            if melhor is None:
                break
            _, gi, pos = melhor
            p = grupos[gi]["peca"]
            _inserir_maxrects(ch, p, pos, chapa_w, chapa_h, kerf)
            grupos[gi]["count"] -= 1
            depois = _area_ocupada_chapa(ch)
            if depois <= antes + 1e-12:
                sem_melhora += 1
            else:
                sem_melhora = 0
            if sem_melhora >= 3:
                break
            # Se a chapa já está acima da meta e nenhuma peça restante encaixa melhor, fecha.
            if area_chapa and depois / area_chapa >= META_APROVEITAMENTO_PADRAO:
                teste = _melhor_grupo_para_preencher_sobra(ch, grupos, chapa_w, chapa_h, kerf, permite_girar, inicio)
                if teste is None:
                    break

        chapas.append(ch)

    return _finalizar_com_compactacao(chapas, chapa_w, chapa_h, "oportunidades", f"Oportunidades + compactação de sobras / {ordenacao}", kerf, permite_girar, inicio=inicio)


# ============================================================
# PÓS-OTIMIZAÇÃO: PREENCHER VAZIOS COM PEÇAS DE CHAPAS POSTERIORES
# ============================================================

def _recalcular_livres_chapa(ch, chapa_w, chapa_h, kerf):
    """Reconstrói os espaços livres reais da chapa após todas as peças posicionadas.

    Esta função é essencial para enxergar aquela faixa branca lateral/inferior que
    ainda pode receber peças giradas ou peças menores vindas de chapas posteriores.
    """
    livres = [{"x": 0.0, "y": 0.0, "w": chapa_w, "h": chapa_h}]
    for p in ch.get("pecas", []):
        usado = {
            "x": float(p["x"]),
            "y": float(p["y"]),
            "w": min(float(p["w_draw"]) + kerf, chapa_w - float(p["x"])),
            "h": min(float(p["h_draw"]) + kerf, chapa_h - float(p["y"])),
        }
        livres = _split_free_rectangles(livres, usado, min_dim=max(kerf, 0.003))
    ch["livres"] = livres
    return livres


def _area_retangulo(r):
    return max(0.0, float(r.get("w", 0))) * max(0.0, float(r.get("h", 0)))


def _tempo_compactacao_esgotado(inicio):
    return (time.time() - inicio) >= MAX_SEGUNDOS_COMPACTACAO


def _melhor_posicao_para_mover(ch_alvo, peca, chapa_w, chapa_h, kerf, permite_girar):
    """Procura a melhor posição em uma chapa já existente para uma peça vinda de outra chapa."""
    melhor = None
    livres = sorted(ch_alvo.get("livres", []), key=lambda r: (_area_retangulo(r), r["y"], r["x"]))
    for rect in livres:
        if rect["w"] <= max(kerf, 0.003) or rect["h"] <= max(kerf, 0.003):
            continue
        for w, h, girada in _orientacoes_possiveis(peca, permite_girar):
            if w > rect["w"] + 1e-9 or h > rect["h"] + 1e-9:
                continue
            x, y = rect["x"], rect["y"]
            if x + w > chapa_w + 1e-9 or y + h > chapa_h + 1e-9:
                continue
            if _candidato_interfere_com_pecas(ch_alvo, x, y, w, h, kerf):
                continue

            area_peca = w * h
            area_rect = rect["w"] * rect["h"]
            preenchimento = area_peca / area_rect if area_rect else 0.0
            sobra_w = max(rect["w"] - w, 0.0)
            sobra_h = max(rect["h"] - h, 0.0)
            short_side = min(sobra_w, sobra_h)
            long_side = max(sobra_w, sobra_h)
            contato = _contato_score(ch_alvo, x, y, w, h, chapa_w, chapa_h)

            # Prioridade da pós-otimização:
            # 1) ocupar melhor o vazio existente;
            # 2) preferir orientação girada quando ela é a que cabe/preenche melhor;
            # 3) encostar em bordas/peças para deixar desenho mais limpo;
            # 4) usar maior área possível.
            score = (
                -preenchimento,
                short_side,
                long_side,
                -contato,
                -area_peca,
                0 if girada else 1,
                y,
                x,
            )
            cand = (score, rect, x, y, w, h, girada)
            if melhor is None or cand[0] < melhor[0]:
                melhor = cand
    return melhor


def _mover_peca_para_chapa(ch_origem, idx_peca, ch_alvo, pos, chapa_w, chapa_h, kerf):
    """Remove uma peça de uma chapa posterior e coloca no vazio da chapa alvo."""
    _, rect, x, y, w, h, girada = pos
    p = dict(ch_origem["pecas"].pop(idx_peca))
    p.update({"x": x, "y": y, "w_draw": w, "h_draw": h, "girada": girada})
    ch_alvo.setdefault("pecas", []).append(p)
    adicionar_sequencia(ch_alvo, f"Realocar código {p.get('codigo','')} para sobra útil em X={int(round(x*1000))} / Y={int(round(y*1000))} mm.")

    usado = {"x": x, "y": y, "w": min(w + kerf, chapa_w - x), "h": min(h + kerf, chapa_h - y)}
    ch_alvo["livres"] = _split_free_rectangles(ch_alvo.get("livres", []), usado, min_dim=max(kerf, 0.003))
    _recalcular_livres_chapa(ch_origem, chapa_w, chapa_h, kerf)


def _compactar_chapas_com_sobras(chapas, chapa_w, chapa_h, kerf, permite_girar, inicio=None):
    """Segunda passada: aproveita faixas brancas das chapas anteriores.

    A geração inicial pode deixar uma faixa branca lateral e depois abrir novas chapas.
    Esta rotina olha para as chapas posteriores, pega peças que ainda estão nelas e
    tenta realocar nos vazios das chapas anteriores, testando também rotação de 90°.
    Se uma chapa posterior ficar vazia, ela é removida do plano.
    """
    if not chapas or len(chapas) <= 1:
        return chapas
    if inicio is None:
        inicio = time.time()

    for ch in chapas:
        _recalcular_livres_chapa(ch, chapa_w, chapa_h, kerf)

    total_pecas = sum(len(ch.get("pecas", [])) for ch in chapas)
    limite_movimentos = max(50, min(total_pecas * 2, 3000))
    movimentos = 0

    alvo_idx = 0
    while alvo_idx < len(chapas) - 1 and movimentos < limite_movimentos:
        if _tempo_compactacao_esgotado(inicio):
            break
        ch_alvo = chapas[alvo_idx]
        _recalcular_livres_chapa(ch_alvo, chapa_w, chapa_h, kerf)

        mudou_alvo = True
        while mudou_alvo and movimentos < limite_movimentos:
            if _tempo_compactacao_esgotado(inicio):
                break
            mudou_alvo = False
            melhor = None

            # Procura peças nas ÚLTIMAS chapas primeiro, para tentar eliminar chapas no final.
            for src_idx in range(len(chapas) - 1, alvo_idx, -1):
                ch_src = chapas[src_idx]
                for pi, p in enumerate(ch_src.get("pecas", [])):
                    pos = _melhor_posicao_para_mover(ch_alvo, p, chapa_w, chapa_h, kerf, permite_girar)
                    if pos is None:
                        continue
                    area_peca = pos[4] * pos[5]
                    score = (pos[0], -src_idx, -area_peca, pi)
                    if melhor is None or score < melhor[0]:
                        melhor = (score, src_idx, pi, pos)

            if melhor is None:
                break

            _, src_idx, pi, pos = melhor
            ch_src = chapas[src_idx]
            _mover_peca_para_chapa(ch_src, pi, ch_alvo, pos, chapa_w, chapa_h, kerf)
            movimentos += 1
            mudou_alvo = True

            # Remove chapa que ficou completamente vazia.
            if not ch_src.get("pecas"):
                chapas.pop(src_idx)
                if src_idx < alvo_idx:
                    alvo_idx -= 1
                for ch in chapas:
                    _recalcular_livres_chapa(ch, chapa_w, chapa_h, kerf)

        alvo_idx += 1

    for ch in chapas:
        ch["livres"] = _prunar_livres(ch.get("livres", []), min_dim=max(kerf, 0.003))
    return chapas


def _finalizar_com_compactacao(chapas, chapa_w, chapa_h, modo, estrategia, kerf, permite_girar, inicio=None):
    """Finaliza, compacta sobras e recalcula os indicadores."""
    chapas = finalizar_chapas(chapas, chapa_w, chapa_h, modo, estrategia)
    if len(chapas) > 1:
        chapas = _compactar_chapas_com_sobras(chapas, chapa_w, chapa_h, kerf, permite_girar, inicio=time.time())
        chapas = finalizar_chapas(chapas, chapa_w, chapa_h, modo, estrategia + " + compactação de sobras")
    return chapas


def plano_oportunidades_sobra(pecas, chapa_w, chapa_h, kerf, permite_girar):
    """Roda variações do algoritmo que preenche sobras úteis antes de abrir nova chapa."""
    inicio = time.time()
    ordenacoes, _, _ = _listas_estrategias_por_tamanho(len(pecas))
    # Para a lógica de oportunidades, estas ordenações costumam ser as mais úteis.
    preferidas = []
    for o in ["area_desc", "lado_maior_desc", "altura_desc", "largura_desc", "estreitas_depois"]:
        if o in ordenacoes and o not in preferidas:
            preferidas.append(o)
    if not preferidas:
        preferidas = ["area_desc"]

    candidatos = []
    erros = []
    for ordenacao in preferidas:
        if candidatos and _tempo_esgotado(inicio):
            return _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)
        try:
            plano = _plano_oportunidades_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao)
            candidatos.append(plano)
            meta = _metadados_plano(plano, chapa_w, chapa_h)
            if meta["meta_atingida"] and meta["chapas_total"] <= meta["chapas_min_area"]:
                return plano
        except Exception as exc:
            erros.append(str(exc))
    if not candidatos and erros:
        raise ValueError(erros[-1])
    return _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)


def _plano_maxrects_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao, criterio):
    ordenadas = _ordenar_pecas(pecas, ordenacao)
    chapas = []
    for peca in ordenadas:
        if not escolher_orientacao(peca, {"x": 0.0, "y": 0.0, "w": chapa_w, "h": chapa_h}, permite_girar):
            raise ValueError(f"Peça código {peca['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
        melhor_chapa = None
        melhor_pos = None
        for idx, ch in enumerate(chapas):
            pos = _melhor_posicao_maxrects(ch, peca, chapa_w, chapa_h, kerf, permite_girar, criterio)
            if pos is None:
                continue
            ocupada = sum(pp["w_draw"] * pp["h_draw"] for pp in ch.get("pecas", [])) + pos[4] * pos[5]
            score_global = (pos[0], -ocupada, idx)
            if melhor_pos is None or score_global < melhor_pos[0]:
                melhor_pos = (score_global, pos)
                melhor_chapa = ch
        if melhor_chapa is None:
            ch = _chapa_vazia(chapa_w, chapa_h)
            pos = _melhor_posicao_maxrects(ch, peca, chapa_w, chapa_h, kerf, permite_girar, criterio)
            if pos is None:
                raise ValueError(f"Peça código {peca['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
            adicionar_sequencia(ch, "Abrir nova chapa.")
            _inserir_maxrects(ch, peca, pos, chapa_w, chapa_h, kerf)
            chapas.append(ch)
        else:
            _inserir_maxrects(melhor_chapa, peca, melhor_pos[1], chapa_w, chapa_h, kerf)
    return _finalizar_com_compactacao(chapas, chapa_w, chapa_h, "encaixe", f"MaxRects {criterio} / {ordenacao}", kerf, permite_girar)


def _plano_shelf_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao, orientacao_faixas="horizontal"):
    if orientacao_faixas == "vertical":
        pecas_swap = []
        for p in pecas:
            pp = dict(p)
            pp["w"], pp["h"] = p["h"], p["w"]
            pecas_swap.append(pp)
        chapas_swap = _plano_shelf_estrategia(pecas_swap, chapa_h, chapa_w, kerf, permite_girar, ordenacao, "horizontal")
        chapas = []
        for ch in chapas_swap:
            novo = {"pecas": [], "faixas": ch.get("faixas", []), "sequencia": ["Plano guilhotinado vertical convertido para orientação da chapa."] + ch.get("sequencia", [])}
            for p in ch["pecas"]:
                q = dict(p)
                old_x, old_y, old_w, old_h = p["x"], p["y"], p["w_draw"], p["h_draw"]
                q["x"], q["y"], q["w_draw"], q["h_draw"] = old_y, old_x, old_h, old_w
                novo["pecas"].append(q)
            chapas.append(novo)
        return finalizar_chapas(chapas, chapa_w, chapa_h, "guilhotina", f"Faixas verticais / {ordenacao}")

    ordenadas = _ordenar_pecas(pecas, ordenacao)
    chapas = []
    ch = {"pecas": [], "faixas": [], "sequencia": []}

    def iniciar_faixa(ch, y, h):
        faixa = {"y": y, "h": h, "x": 0.0, "pecas": []}
        ch["faixas"].append(faixa)
        adicionar_sequencia(ch, f"Cortar faixa guilhotinada de {int(round(h*1000))} mm a partir de Y={int(round(y*1000))} mm.")
        return faixa

    for peca in ordenadas:
        melhor = None
        for faixa in ch["faixas"]:
            for w, h, girada in _orientacoes_possiveis(peca, permite_girar):
                if h <= faixa["h"] + 1e-9 and faixa["x"] + w <= chapa_w + 1e-9:
                    score = (faixa["h"] - h, chapa_w - (faixa["x"] + w), faixa["y"])
                    cand = (score, faixa, w, h, girada)
                    if melhor is None or cand[0] < melhor[0]:
                        melhor = cand
        if melhor is not None:
            _, faixa, w, h, girada = melhor
            x = faixa["x"]
            p = dict(peca); p.update({"x": x, "y": faixa["y"], "w_draw": w, "h_draw": h, "girada": girada})
            ch["pecas"].append(p); faixa["pecas"].append(p)
            faixa["x"] = x + w + kerf
            adicionar_sequencia(ch, f"Na faixa {int(round(faixa['h']*1000))} mm, cortar código {p['codigo']} com {int(round(w*1000))} x {int(round(h*1000))} mm.")
            continue

        opcoes_nova = [(w, h, girada) for w, h, girada in _orientacoes_possiveis(peca, permite_girar) if w <= chapa_w + 1e-9 and h <= chapa_h + 1e-9]
        if not opcoes_nova:
            raise ValueError(f"Peça código {peca['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
        w, h, girada = min(opcoes_nova, key=lambda o: (o[1], -o[0]))
        y_usado = 0.0
        if ch["faixas"]:
            ult = ch["faixas"][-1]
            y_usado = ult["y"] + ult["h"] + kerf
        if y_usado + h > chapa_h + 1e-9:
            if ch["pecas"]:
                chapas.append(ch)
            ch = {"pecas": [], "faixas": [], "sequencia": []}
            y_usado = 0.0
        faixa = iniciar_faixa(ch, y_usado, h)
        p = dict(peca); p.update({"x": 0.0, "y": y_usado, "w_draw": w, "h_draw": h, "girada": girada})
        ch["pecas"].append(p); faixa["pecas"].append(p); faixa["x"] = w + kerf
        adicionar_sequencia(ch, f"Na nova faixa, cortar código {p['codigo']} com {int(round(w*1000))} x {int(round(h*1000))} mm.")
    if ch["pecas"]:
        chapas.append(ch)
    return finalizar_chapas(chapas, chapa_w, chapa_h, "guilhotina", f"Faixas horizontais / {ordenacao}")



def _plano_shelf_backfill_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao, orientacao_faixas="horizontal"):
    """Guilhotina por faixas com retorno às chapas anteriores.

    Esta é a lógica que resolve o problema apontado nas imagens: quando uma peça menor
    aparece depois, o algoritmo volta nas faixas já abertas de chapas anteriores e tenta
    ocupar as perdas laterais antes de abrir outra chapa.
    """
    if orientacao_faixas == "vertical":
        pecas_swap = []
        for p in pecas:
            pp = dict(p)
            pp["w"], pp["h"] = p["h"], p["w"]
            pecas_swap.append(pp)
        chapas_swap = _plano_shelf_backfill_estrategia(pecas_swap, chapa_h, chapa_w, kerf, permite_girar, ordenacao, "horizontal")
        chapas = []
        for ch in chapas_swap:
            novo = {"pecas": [], "faixas": [], "sequencia": ["Plano guilhotinado vertical com preenchimento de sobras."] + ch.get("sequencia", [])}
            for f in ch.get("faixas", []):
                novo["faixas"].append({"y": f.get("y", 0), "h": f.get("h", 0), "x": f.get("x", 0), "pecas": []})
            for pp in ch["pecas"]:
                q = dict(pp)
                old_x, old_y, old_w, old_h = pp["x"], pp["y"], pp["w_draw"], pp["h_draw"]
                q["x"], q["y"], q["w_draw"], q["h_draw"] = old_y, old_x, old_h, old_w
                novo["pecas"].append(q)
            chapas.append(novo)
        return _finalizar_com_compactacao(chapas, chapa_w, chapa_h, "guilhotina", f"Faixas verticais com backfill / {ordenacao}", kerf, permite_girar)

    ordenadas = _ordenar_pecas(pecas, ordenacao)
    chapas = []
    area_chapa = chapa_w * chapa_h

    def y_proxima_faixa(ch):
        if not ch.get("faixas"):
            return 0.0
        return max(f["y"] + f["h"] for f in ch["faixas"]) + kerf

    def criar_chapa():
        return {"pecas": [], "faixas": [], "sequencia": []}

    def criar_faixa(ch, y, h):
        faixa = {"y": y, "h": h, "x": 0.0, "pecas": []}
        ch["faixas"].append(faixa)
        adicionar_sequencia(ch, f"Cortar faixa guilhotinada de {int(round(h*1000))} mm a partir de Y={int(round(y*1000))} mm.")
        return faixa

    def colocar(ch, faixa, peca, w, h, girada):
        x = faixa["x"]
        p = dict(peca)
        p.update({"x": x, "y": faixa["y"], "w_draw": w, "h_draw": h, "girada": girada})
        ch["pecas"].append(p)
        faixa["pecas"].append(p)
        faixa["x"] = x + w + kerf
        adicionar_sequencia(ch, f"Na faixa {int(round(faixa['h']*1000))} mm, cortar código {p['codigo']} com {int(round(w*1000))} x {int(round(h*1000))} mm.")

    for peca in ordenadas:
        melhor = None

        # 1) Primeiro tenta preencher perdas laterais em faixas já existentes, inclusive em chapas anteriores.
        for ci, ch in enumerate(chapas):
            area_atual = _area_ocupada_chapa(ch)
            for fi, faixa in enumerate(ch.get("faixas", [])):
                for w, h, girada in _orientacoes_possiveis(peca, permite_girar):
                    if h <= faixa["h"] + 1e-9 and faixa["x"] + w <= chapa_w + 1e-9:
                        rem_w = chapa_w - (faixa["x"] + w)
                        slack_h = faixa["h"] - h
                        util_depois = (area_atual + w*h) / area_chapa if area_chapa else 0.0
                        # score começa com 0: preencher faixa existente é prioridade máxima.
                        score = (0, rem_w, slack_h, -util_depois, ci, fi)
                        cand = (score, "existente", ci, faixa, w, h, girada)
                        if melhor is None or cand[0] < melhor[0]:
                            melhor = cand

        # 2) Se não couber em perda lateral, tenta abrir nova faixa em alguma chapa já aberta.
        for ci, ch in enumerate(chapas):
            y = y_proxima_faixa(ch)
            area_atual = _area_ocupada_chapa(ch)
            for w, h, girada in _orientacoes_possiveis(peca, permite_girar):
                if w <= chapa_w + 1e-9 and y + h <= chapa_h + 1e-9:
                    rem_h = chapa_h - (y + h)
                    util_depois = (area_atual + w*h) / area_chapa if area_chapa else 0.0
                    # Ao abrir nova faixa, prioriza faixa mais baixa. Isso evita linhas altas/tortas
                    # e preserva espaço vertical para novas faixas e peças menores.
                    score = (1, h, rem_h, -util_depois, ci)
                    cand = (score, "nova_faixa", ci, None, w, h, girada)
                    if melhor is None or cand[0] < melhor[0]:
                        melhor = cand

        # 3) Se não houver oportunidade em chapas existentes, abre nova chapa.
        if melhor is None:
            opcoes = [(w, h, girada) for w, h, girada in _orientacoes_possiveis(peca, permite_girar) if w <= chapa_w + 1e-9 and h <= chapa_h + 1e-9]
            if not opcoes:
                raise ValueError(f"Peça código {peca['codigo']} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
            w, h, girada = min(opcoes, key=lambda o: (o[1], -o[0]))
            ch = criar_chapa()
            adicionar_sequencia(ch, "Abrir nova chapa.")
            faixa = criar_faixa(ch, 0.0, h)
            colocar(ch, faixa, peca, w, h, girada)
            chapas.append(ch)
            continue

        _, tipo, ci, faixa, w, h, girada = melhor
        ch = chapas[ci]
        if tipo == "nova_faixa":
            faixa = criar_faixa(ch, y_proxima_faixa(ch), h)
        colocar(ch, faixa, peca, w, h, girada)

    return _finalizar_com_compactacao(chapas, chapa_w, chapa_h, "guilhotina", f"Faixas com backfill de sobras / {ordenacao}", kerf, permite_girar, inicio=inicio)



# ============================================================
# PLANO INDUSTRIAL UNIFORME: PEÇAS IGUAIS EM BLOCOS/FAIXAS
# ============================================================

def _chave_grupo_uniforme(peca):
    return (
        str(peca.get("codigo", "")),
        round(float(peca.get("w", 0)), 6),
        round(float(peca.get("h", 0)), 6),
        str(peca.get("material", "")),
    )


def _grupos_uniformes(pecas, ordenacao="area_desc"):
    grupos_map = {}
    ordem = []
    for p in pecas:
        chave = _chave_grupo_uniforme(p)
        if chave not in grupos_map:
            grupos_map[chave] = {"peca": dict(p), "count": 0, "orientacao": None, "orientacao_secundaria_usada": False, "chave": chave}
            ordem.append(chave)
        grupos_map[chave]["count"] += 1
    grupos = [grupos_map[k] for k in ordem]

    def area(g):
        p = g["peca"]
        return float(p["w"]) * float(p["h"])
    def maior_lado(g):
        p = g["peca"]
        return max(float(p["w"]), float(p["h"]))
    def menor_lado(g):
        p = g["peca"]
        return min(float(p["w"]), float(p["h"]))

    if ordenacao == "count_desc":
        grupos.sort(key=lambda g: (g["count"], area(g), maior_lado(g)), reverse=True)
    elif ordenacao == "lado_maior_desc":
        grupos.sort(key=lambda g: (maior_lado(g), area(g), g["count"]), reverse=True)
    elif ordenacao == "estreitas_depois":
        grupos.sort(key=lambda g: (menor_lado(g), area(g), g["count"]), reverse=True)
    else:
        grupos.sort(key=lambda g: (area(g), maior_lado(g), g["count"]), reverse=True)
    return grupos


def _orientacoes_grupo_uniforme(grupo, permite_girar, permitir_secundaria=False):
    """Retorna orientações permitidas para o grupo.

    Regra industrial:
    - a peça mantém uma orientação principal para formar blocos limpos;
    - a rotação 90° é liberada em faixas laterais/inferiores como bloco
      complementar, repetidas vezes se houver espaço, para evitar perda grande;
    - continua sem espalhar peças iguais de forma aleatória.
    """
    if grupo.get("orientacao") is None:
        return _orientacoes_possiveis(grupo["peca"], permite_girar)

    base = [grupo["orientacao"]]
    if not permitir_secundaria or not permite_girar:
        return base

    w, h, girada = grupo["orientacao"]
    if abs(w - h) < 1e-9:
        return base
    secundaria = (h, w, not girada)
    return base + [secundaria]


def _capacidade_bloco_uniforme(rect, w, h, kerf, restante):
    if restante <= 0 or w <= 0 or h <= 0:
        return None
    cols = int(math.floor((float(rect["w"]) + kerf + 1e-9) / (w + kerf)))
    rows = int(math.floor((float(rect["h"]) + kerf + 1e-9) / (h + kerf)))
    if cols <= 0 or rows <= 0:
        return None
    cap = cols * rows
    qtd = min(int(restante), cap)
    if qtd <= 0:
        return None
    linhas_usadas = int(math.ceil(qtd / cols))
    if linhas_usadas <= 1:
        cols_usadas = qtd
    else:
        cols_usadas = cols
    bloco_w = cols_usadas * w + max(0, cols_usadas - 1) * kerf
    bloco_h = linhas_usadas * h + max(0, linhas_usadas - 1) * kerf
    if bloco_w > rect["w"] + 1e-9 or bloco_h > rect["h"] + 1e-9:
        return None
    densidade = (qtd * w * h) / (bloco_w * bloco_h) if bloco_w * bloco_h else 0.0
    return {
        "qtd": qtd,
        "cols": cols,
        "cols_usadas": cols_usadas,
        "linhas_usadas": linhas_usadas,
        "bloco_w": bloco_w,
        "bloco_h": bloco_h,
        "densidade": densidade,
    }


def _melhor_bloco_uniforme(chapas, grupo, chapa_w, chapa_h, kerf, permite_girar):
    melhor = None
    restante = int(grupo.get("count", 0))
    if restante <= 0:
        return None

    orient_principal = grupo.get("orientacao")

    for ci, ch in enumerate(chapas):
        for ri, rect in enumerate(ch.get("livres", [])):
            # Detecta sobras com cara de faixa lateral ou inferior.
            # A faixa lateral é prioridade porque normalmente é a perda que mais aparece
            # depois de blocos grandes, e pode receber peças estreitas giradas 90°.
            faixa_lateral = rect["x"] > chapa_w * 0.40 and rect["w"] <= chapa_w * 0.42 and rect["h"] >= chapa_h * 0.35
            faixa_inferior = rect["y"] > chapa_h * 0.40 and rect["h"] <= chapa_h * 0.42 and rect["w"] >= chapa_w * 0.35
            faixa_qualquer = faixa_lateral or faixa_inferior or rect["x"] > 0 or rect["y"] > 0

            for w, h, girada in _orientacoes_grupo_uniforme(grupo, permite_girar, permitir_secundaria=faixa_qualquer):
                cap = _capacidade_bloco_uniforme(rect, w, h, kerf, restante)
                if not cap:
                    continue

                secundaria = False
                if orient_principal is not None and (abs(w - orient_principal[0]) > 1e-9 or abs(h - orient_principal[1]) > 1e-9):
                    secundaria = True

                sobra_ret = (rect["w"] * rect["h"]) - (cap["bloco_w"] * cap["bloco_h"])
                fill_rect = (cap["bloco_w"] * cap["bloco_h"]) / (rect["w"] * rect["h"]) if rect["w"] * rect["h"] else 0.0
                fill_pecas = (cap["qtd"] * w * h) / (rect["w"] * rect["h"]) if rect["w"] * rect["h"] else 0.0
                cobre_altura_lateral = cap["bloco_h"] / rect["h"] if rect["h"] else 0.0
                cobre_largura_lateral = cap["bloco_w"] / rect["w"] if rect["w"] else 0.0
                cobre_largura_inferior = cap["bloco_w"] / rect["w"] if rect["w"] else 0.0
                cobre_altura_inferior = cap["bloco_h"] / rect["h"] if rect["h"] else 0.0

                encosta = 0
                if abs(rect["x"]) < 1e-9: encosta += 1
                if abs(rect["y"]) < 1e-9: encosta += 1
                if abs((rect["x"] + rect["w"]) - chapa_w) < 1e-9: encosta += 1
                if abs((rect["y"] + rect["h"]) - chapa_h) < 1e-9: encosta += 1

                residuo_w = max(0.0, rect["w"] - cap["bloco_w"])
                residuo_h = max(0.0, rect["h"] - cap["bloco_h"])
                residuo_fino = min(residuo_w, residuo_h)

                # Classe de prioridade:
                # -4: preencher lateral com bloco organizado, mesmo que caiba menos peças do que no rodapé.
                # -3: preencher inferior com bloco organizado.
                # -2/-1: rotações complementares boas em sobras.
                #  0: bloco normal.
                #  5: rotação sem benefício claro, evita bagunça.
                classe = 0
                bonus_faixa = 0

                if faixa_lateral and fill_rect >= 0.32 and cobre_largura_lateral >= 0.42 and cobre_altura_lateral >= 0.35:
                    classe = -4
                    bonus_faixa = -4
                elif faixa_inferior and fill_rect >= 0.32 and cobre_largura_inferior >= 0.35 and cobre_altura_inferior >= 0.42:
                    classe = -3
                    bonus_faixa = -3

                if secundaria:
                    if faixa_lateral and fill_rect >= 0.25 and cobre_largura_lateral >= 0.35:
                        classe = min(classe, -4)
                        bonus_faixa -= 2
                    elif faixa_inferior and fill_rect >= 0.30 and cobre_altura_inferior >= 0.35:
                        classe = min(classe, -3)
                        bonus_faixa -= 1
                    elif faixa_qualquer and fill_rect >= 0.62:
                        classe = min(classe, -1)
                    else:
                        classe = max(classe, 5)

                # Se o grupo ainda não tem orientação principal, também podemos priorizar
                # uma das duas orientações quando ela encaixa melhor na faixa lateral.
                if orient_principal is None:
                    if faixa_lateral and fill_rect >= 0.32 and cobre_largura_lateral >= 0.42:
                        classe = min(classe, -4)
                    elif faixa_inferior and fill_rect >= 0.32 and cobre_altura_inferior >= 0.42:
                        classe = min(classe, -3)

                # Evita escolher um bloco minúsculo quando existe outro que preenche melhor.
                penal_fill_baixo = 1 if fill_rect < 0.25 else 0

                # Importante: classe vem antes da quantidade. Assim, uma faixa lateral útil
                # é preenchida antes de jogar todas as peças no rodapé, como aconteceu no print.
                score = (
                    classe,
                    bonus_faixa,
                    penal_fill_baixo,
                    -fill_rect,
                    -fill_pecas,
                    -cap["densidade"],
                    -cap["qtd"],
                    sobra_ret,
                    -encosta,
                    residuo_fino,
                    rect["y"],
                    rect["x"],
                    ci,
                )
                cand = (score, ci, ri, rect, w, h, girada, cap, secundaria)
                if melhor is None or cand[0] < melhor[0]:
                    melhor = cand
    return melhor


def _inserir_bloco_uniforme(ch, grupo, rect, w, h, girada, cap, chapa_w, chapa_h, kerf):
    qtd = int(cap["qtd"])
    cols = int(cap["cols"])
    x0, y0 = float(rect["x"]), float(rect["y"])
    orient_principal = grupo.get("orientacao")
    secundaria = False
    if orient_principal is not None and (abs(w - orient_principal[0]) > 1e-9 or abs(h - orient_principal[1]) > 1e-9):
        secundaria = True

    for n in range(qtd):
        lin = n // cols
        col = n % cols
        p = dict(grupo["peca"])
        p.update({
            "x": x0 + col * (w + kerf),
            "y": y0 + lin * (h + kerf),
            "w_draw": w,
            "h_draw": h,
            "girada": girada,
            "bloco_uniforme": True,
            "orientacao_secundaria": secundaria,
        })
        ch.setdefault("pecas", []).append(p)
    grupo["count"] -= qtd
    if grupo.get("orientacao") is None:
        grupo["orientacao"] = (w, h, girada)

    usado = {
        "x": x0,
        "y": y0,
        "w": min(cap["bloco_w"] + kerf, chapa_w - x0),
        "h": min(cap["bloco_h"] + kerf, chapa_h - y0),
    }
    ch["livres"] = _split_free_rectangles(ch.get("livres", []), usado, min_dim=max(kerf, 0.003))
    texto_orient = f"{int(round(w*1000))} x {int(round(h*1000))} mm"
    if secundaria:
        adicionar_sequencia(ch, f"Preencher sobra lateral/inferior: código {grupo['peca'].get('codigo','')} com {qtd} peça(s), girado 90°, bloco {texto_orient}.")
    else:
        adicionar_sequencia(ch, f"Bloco uniforme: código {grupo['peca'].get('codigo','')} com {qtd} peça(s), orientação {texto_orient}.")


def _plano_industrial_uniforme_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao="area_desc"):
    """Plano industrial: mantém peças iguais agrupadas e com orientação uniforme.

    Esta lógica evita a 'bagunça' visual: peças de mesmo código não ficam
    espalhadas/rotacionadas aleatoriamente. O algoritmo monta blocos regulares
    e usa as sobras para inserir outros blocos, não peças soltas.
    """
    inicio = time.time()
    grupos = _grupos_uniformes(pecas, ordenacao)
    chapas = []

    while any(int(g.get("count", 0)) > 0 for g in grupos):
        if _tempo_esgotado(inicio) and chapas:
            # Se o tempo acabar, continua com uma estratégia simples e previsível.
            pass

        progresso = False
        for grupo in grupos:
            while int(grupo.get("count", 0)) > 0:
                melhor = _melhor_bloco_uniforme(chapas, grupo, chapa_w, chapa_h, kerf, permite_girar)
                if melhor is None:
                    # Abre nova chapa e força a melhor orientação de bloco na chapa inteira.
                    ch = _chapa_vazia(chapa_w, chapa_h)
                    adicionar_sequencia(ch, "Abrir nova chapa para bloco uniforme.")
                    chapas.append(ch)
                    melhor = _melhor_bloco_uniforme(chapas[-1:], grupo, chapa_w, chapa_h, kerf, permite_girar)
                    if melhor is None:
                        raise ValueError(f"Peça código {grupo['peca'].get('codigo','')} não cabe na chapa {int(chapa_w*1000)} x {int(chapa_h*1000)} mm.")
                    # ajustar índice local para índice real da chapa recém-criada
                    _, _, ri, rect, w, h, girada, cap, secundaria = melhor
                    _inserir_bloco_uniforme(chapas[-1], grupo, rect, w, h, girada, cap, chapa_w, chapa_h, kerf)
                    progresso = True
                else:
                    _, ci, ri, rect, w, h, girada, cap, secundaria = melhor
                    _inserir_bloco_uniforme(chapas[ci], grupo, rect, w, h, girada, cap, chapa_w, chapa_h, kerf)
                    progresso = True
                # Evita laço pesado demais em Render; o plano continua estável.
                if _tempo_esgotado(inicio) and len(chapas) > 0:
                    break
            if _tempo_esgotado(inicio) and len(chapas) > 0:
                # Continua sem testar variações extras, mas não abandona peças.
                continue
        if not progresso:
            break

    return finalizar_chapas(chapas, chapa_w, chapa_h, "industrial", f"Industrial com lateral reforçada / {ordenacao}")


def plano_industrial_uniforme(pecas, chapa_w, chapa_h, kerf, permite_girar):
    ordenacoes = ["area_desc", "count_desc", "lado_maior_desc"]
    candidatos = []
    erros = []
    inicio = time.time()
    for ordenacao in ordenacoes:
        if candidatos and _tempo_esgotado(inicio):
            break
        try:
            candidatos.append(_plano_industrial_uniforme_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao))
        except Exception as exc:
            erros.append(str(exc))
    if not candidatos and erros:
        raise ValueError(erros[-1])
    return _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)

def _aproveitamento_plano(chapas, chapa_w, chapa_h):
    area_chapa = chapa_w * chapa_h
    qtd = len(chapas)
    area_total = sum(ch.get("area_pecas", 0.0) for ch in chapas)
    return area_total / (qtd * area_chapa) if qtd and area_chapa else 0.0


def _pior_chapa_plano(chapas):
    return min((ch.get("aproveitamento", 0.0) for ch in chapas), default=0.0)


def _agrupar_niveis(valores, tolerancia=0.008):
    valores = sorted(float(v) for v in valores)
    grupos = []
    for v in valores:
        if not grupos or abs(v - grupos[-1][-1]) > tolerancia:
            grupos.append([v])
        else:
            grupos[-1].append(v)
    return [sum(g) / len(g) for g in grupos]


def _complexidade_visual_chapa(ch):
    pecas = ch.get("pecas", [])
    if not pecas:
        return 0.0
    xs = _agrupar_niveis([p["x"] for p in pecas] + [p["x"] + p["w_draw"] for p in pecas])
    ys = _agrupar_niveis([p["y"] for p in pecas] + [p["y"] + p["h_draw"] for p in pecas])
    orientacoes = len({(round(p["w_draw"], 3), round(p["h_draw"], 3)) for p in pecas})
    linhas_topo = len(_agrupar_niveis([p["y"] for p in pecas]))
    colunas_esq = len(_agrupar_niveis([p["x"] for p in pecas]))
    return (len(xs) + len(ys)) + 0.7 * orientacoes + 0.8 * linhas_topo + 0.8 * colunas_esq


def _complexidade_visual_plano(chapas):
    return sum(_complexidade_visual_chapa(ch) for ch in chapas)


def _score_plano(chapas, chapa_w, chapa_h):
    qtd = len(chapas)
    aproveitamento_geral = _aproveitamento_plano(chapas, chapa_w, chapa_h)
    pior_chapa = _pior_chapa_plano(chapas)
    complexidade = _complexidade_visual_plano(chapas)
    return (qtd, -aproveitamento_geral, complexidade, -pior_chapa)


def _selecionar_melhor_plano(candidatos, chapa_w, chapa_h):
    candidatos = [c for c in candidatos if c]
    if not candidatos:
        return []

    min_chapas = min(len(c) for c in candidatos)
    candidatos = [c for c in candidatos if len(c) == min_chapas]
    melhor_aproveitamento = max(_aproveitamento_plano(c, chapa_w, chapa_h) for c in candidatos)
    tolerancia_visual = 0.012
    quase_melhores = [c for c in candidatos if _aproveitamento_plano(c, chapa_w, chapa_h) >= melhor_aproveitamento - tolerancia_visual]

    guilhotina_quase = [c for c in quase_melhores if all(ch.get("modo") == "guilhotina" for ch in c)]
    if guilhotina_quase:
        melhor_guilhotina = max(_aproveitamento_plano(c, chapa_w, chapa_h) for c in guilhotina_quase)
        if melhor_guilhotina >= melhor_aproveitamento - 0.008:
            return min(guilhotina_quase, key=lambda chapas: (_complexidade_visual_plano(chapas), -_aproveitamento_plano(chapas, chapa_w, chapa_h), -_pior_chapa_plano(chapas)))

    return min(quase_melhores, key=lambda chapas: (_complexidade_visual_plano(chapas), -_aproveitamento_plano(chapas, chapa_w, chapa_h), -_pior_chapa_plano(chapas)))


def _metadados_plano(chapas, chapa_w, chapa_h, meta=META_APROVEITAMENTO_PADRAO):
    area_chapa = chapa_w * chapa_h
    area_total = sum(ch.get("area_pecas", 0.0) for ch in chapas)
    qtd = len(chapas)
    aproveitamento = area_total / (qtd * area_chapa) if qtd and area_chapa else 0.0
    chapas_min_area = math.ceil(area_total / area_chapa) if area_chapa and area_total > 0 else 0
    limite_teorico_area = area_total / (chapas_min_area * area_chapa) if chapas_min_area and area_chapa else 0.0
    return {
        "chapas_total": qtd,
        "area_total_pecas": area_total,
        "aproveitamento_geral": aproveitamento,
        "sobra_total_m2": max(qtd * area_chapa - area_total, 0.0),
        "meta_alvo": meta,
        "meta_atingida": aproveitamento >= meta - 1e-9,
        "chapas_min_area": chapas_min_area,
        "limite_teorico_area": limite_teorico_area,
    }


def _listas_estrategias_por_tamanho(qtd_pecas):
    """Define quantas tentativas serão feitas conforme o tamanho do lote."""
    if qtd_pecas >= 1200:
        return ["area_desc", "lado_maior_desc"], ["baf", "bssf"], ["area_desc", "lado_maior_desc"]
    if qtd_pecas >= 500:
        return ["area_desc", "lado_maior_desc", "altura_desc"], ["baf", "bssf", "contato"], ["area_desc", "lado_maior_desc", "altura_desc"]
    if qtd_pecas >= 180:
        return ["area_desc", "lado_maior_desc", "largura_desc", "altura_desc"], ["baf", "bssf", "contato", "top_left"], ["area_desc", "lado_maior_desc", "largura_desc", "altura_desc"]
    return ["area_desc", "lado_maior_desc", "largura_desc", "altura_desc", "perimetro_desc", "estreitas_depois"], ["baf", "bssf", "blsf", "contato", "top_left"], ["area_desc", "lado_maior_desc", "largura_desc", "altura_desc", "perimetro_desc"]


def plano_encaixe_livre(pecas, chapa_w, chapa_h, kerf, permite_girar):
    """Encaixe livre estável e rápido. Mantém as melhores estratégias sem travar o Render."""
    inicio = time.time()
    ordenacoes, criterios, _ = _listas_estrategias_por_tamanho(len(pecas))
    candidatos = []
    erros = []

    for ordenacao in ordenacoes:
        for criterio in criterios:
            if candidatos and _tempo_esgotado(inicio):
                return _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)
            try:
                plano = _plano_maxrects_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao, criterio)
                candidatos.append(plano)
                # Se já atingiu a meta e está no mínimo teórico de chapas, não perde tempo procurando demais.
                meta = _metadados_plano(plano, chapa_w, chapa_h)
                if meta["meta_atingida"] and meta["chapas_total"] <= meta["chapas_min_area"]:
                    return plano
            except Exception as exc:
                erros.append(str(exc))
    if not candidatos and erros:
        raise ValueError(erros[-1])
    return _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)


def plano_guilhotina_faixas(pecas, chapa_w, chapa_h, kerf, permite_girar):
    """Plano guilhotinado por faixas, rápido e com sequência mais limpa."""
    inicio = time.time()
    _, _, ordenacoes = _listas_estrategias_por_tamanho(len(pecas))
    candidatos = []
    erros = []
    for ordenacao in ordenacoes:
        for orientacao in ["horizontal", "vertical"]:
            if candidatos and _tempo_esgotado(inicio):
                return _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)
            try:
                plano = _plano_shelf_backfill_estrategia(pecas, chapa_w, chapa_h, kerf, permite_girar, ordenacao, orientacao)
                candidatos.append(plano)
                meta = _metadados_plano(plano, chapa_w, chapa_h)
                if meta["meta_atingida"] and meta["chapas_total"] <= meta["chapas_min_area"]:
                    return plano
            except Exception as exc:
                erros.append(str(exc))
    if not candidatos and erros:
        raise ValueError(erros[-1])
    return _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)


def plano_otimizado_meta95(pecas, chapa_w, chapa_h, kerf, permite_girar):
    """Compara estratégias, mas privilegia plano industrial uniforme.

    Se um plano bagunçado economizar apenas pouco aproveitamento, o sistema
    escolhe o plano uniforme, porque é mais seguro para operação.
    """
    candidatos = []
    erros = []

    for fn in (plano_industrial_uniforme, plano_guilhotina_faixas, plano_oportunidades_sobra, plano_encaixe_livre):
        try:
            plano = fn(pecas, chapa_w, chapa_h, kerf, permite_girar)
            if plano:
                candidatos.append(plano)
        except Exception as exc:
            erros.append(str(exc))

    if not candidatos:
        if erros:
            raise ValueError(erros[-1])
        return []

    # Se o industrial usa a mesma quantidade de chapas e fica até 3 p.p. do melhor,
    # fica com o industrial para manter as peças iguais organizadas.
    melhor_ap = max(_aproveitamento_plano(c, chapa_w, chapa_h) for c in candidatos)
    min_chapas = min(len(c) for c in candidatos)
    industriais = [c for c in candidatos if len(c) == min_chapas and all(ch.get("modo") == "industrial" for ch in c)]
    if industriais:
        melhor_ind = max(industriais, key=lambda c: _aproveitamento_plano(c, chapa_w, chapa_h))
        if _aproveitamento_plano(melhor_ind, chapa_w, chapa_h) >= melhor_ap - 0.03:
            melhor = melhor_ind
        else:
            melhor = _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)
    else:
        melhor = _selecionar_melhor_plano(candidatos, chapa_w, chapa_h)

    for ch in melhor:
        ch["modo"] = "industrial" if ch.get("modo") == "industrial" else "otimizado"
    return melhor


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

        if modo == "guilhotina":
            chapas = plano_guilhotina_faixas(pecas, chapa_w, chapa_h, kerf, permite)
        elif modo == "encaixe":
            chapas = plano_encaixe_livre(pecas, chapa_w, chapa_h, kerf, permite)
        elif modo == "oportunidades":
            chapas = plano_oportunidades_sobra(pecas, chapa_w, chapa_h, kerf, permite)
        elif modo == "industrial":
            chapas = plano_industrial_uniforme(pecas, chapa_w, chapa_h, kerf, permite)
        else:
            chapas = plano_otimizado_meta95(pecas, chapa_w, chapa_h, kerf, permite)

        # A sequência operacional detalhada foi removida da saída para deixar o sistema mais leve.
        for ch in chapas:
            ch["sequencia"] = []

        meta = _metadados_plano(chapas, chapa_w, chapa_h)
        estrategia = chapas[0].get("estrategia", "") if chapas else ""
        planos.append({
            "material": material,
            "chapas": chapas,
            "modo": modo,
            "estrategia": estrategia,
            **meta,
        })
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
:root{--bg:#f3f6fb;--card:#fff;--text:#0f1f35;--muted:#53657e;--blue:#1f5eea;--border:#dfe6f0;--danger:#b42318;--ok:#027a48}*{box-sizing:border-box}body{font-family:Arial,Helvetica,sans-serif;margin:0;background:var(--bg);color:var(--text)}.wrap{max-width:1220px;margin:0 auto;padding:20px}.header{display:flex;align-items:center;gap:20px;margin-bottom:18px}.logo-main{height:78px;max-width:260px;object-fit:contain}.header h1{font-size:26px;margin:0 0 4px}.header p{margin:0;color:var(--muted)}.nav{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:18px}.nav a,.btn{display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--border);background:#fff;color:#064ceb;text-decoration:none;border-radius:10px;padding:10px 14px;font-weight:700;cursor:pointer;font-size:14px}.btn.primary{background:var(--blue);color:#fff;border-color:var(--blue)}.card{background:var(--card);border:1px solid var(--border);border-radius:18px;padding:18px;margin:14px 0;box-shadow:0 8px 24px rgba(17,33,61,.06)}.card h2{margin:0 0 16px;font-size:24px}.hint{color:var(--muted);font-size:14px}.row{display:grid;grid-template-columns:180px 1fr 160px 48px;gap:10px;margin:8px 0}.row2{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}input,select,textarea{width:100%;border:1px solid var(--border);border-radius:10px;padding:11px 12px;font-size:14px;background:#fff}textarea{min-height:260px;resize:vertical;font-family:Consolas,Menlo,monospace;line-height:1.45}.paste-grid{display:grid;grid-template-columns:1fr 180px;gap:12px;margin-top:12px}.paste-grid label{display:block;font-weight:800;margin:0 0 7px}.minihelp{background:#f8fafc;border:1px dashed var(--border);border-radius:12px;padding:10px 12px;margin:10px 0;color:#53657e;font-size:13px}.counter{font-size:13px;color:#53657e;margin-top:6px}.toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:10px}table{width:100%;border-collapse:separate;border-spacing:0;border:1px solid var(--border);border-radius:12px;overflow:hidden;background:#fff}th,td{padding:10px 12px;border-bottom:1px solid var(--border);text-align:left;font-size:14px;vertical-align:top}th{background:#f8fafc;font-weight:800}tr:last-child td{border-bottom:none}.num{text-align:right}.badge{display:inline-block;border-radius:999px;background:#eef2ff;color:#123cbd;padding:4px 8px;font-size:12px}.badge.danger{background:#fee4e2;color:#b42318}.badge.ok{background:#d1fadf;color:#027a48}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}.stat{background:#f8fafc;border:1px solid var(--border);border-radius:14px;padding:14px}.stat b{font-size:24px}.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}.footer{display:flex;justify-content:center;align-items:center;gap:26px;margin:26px auto 10px;flex-wrap:wrap}.footer img{height:58px;max-width:230px;object-fit:contain;filter:grayscale(.1);opacity:.88}.sheet-card{page-break-inside:avoid}.svgwrap{overflow:auto;border:1px solid var(--border);border-radius:12px;background:#fff;padding:8px}.cut-svg{display:block;width:100%;height:auto;max-height:720px;background:#fff;object-fit:contain}.cutseq{font-size:13px;color:#334155;columns:2}.login{max-width:430px;margin:80px auto}@media (max-width:760px){.paste-grid{grid-template-columns:1fr}.row2{grid-template-columns:1fr}}@media print{.nav,.actions,.no-print,.btn{display:none!important}.wrap{max-width:100%;padding:0}.card{box-shadow:none;border:1px solid #999;page-break-inside:avoid}body{background:#fff}}
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
        <div class="row2" style="margin-top:12px"><div><label class="hint">Tipo de plano de corte</label><select name="modo_corte"><option value="industrial">Industrial com lateral reforçada</option><option value="otimizado">Otimizado comparativo</option><option value="guilhotina">Guilhotina por faixas</option><option value="oportunidades">Oportunidades agressivo</option><option value="encaixe">Encaixe livre tradicional</option></select></div><div><label class="hint">Salvar histórico</label><select name="salvar_historico"><option value="1">Sim</option><option value="0">Não</option></select></div><div><label class="hint">Ação</label><select name="acao"><option value="calcular">Apenas calcular consumo</option><option value="plano">Calcular e gerar plano de corte</option></select></div></div>
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


def _quebrar_linhas_svg(texto, max_chars=28):
    texto = str(texto or "").strip()
    if not texto:
        return []
    palavras = texto.split()
    linhas = []
    atual = ""
    for palavra in palavras:
        tentativa = (atual + " " + palavra).strip()
        if atual and len(tentativa) > max_chars:
            linhas.append(atual)
            atual = palavra
        else:
            atual = tentativa
    if atual:
        linhas.append(atual)
    return linhas[:2]


def svg_chapa(chapa):
    """Desenha a chapa em SVG com escala estável e visual mais reto/limpo."""
    chapa_w, chapa_h = chapa.get("chapa_w", 2.75), chapa.get("chapa_h", 1.85)
    W = int(round(chapa_w * 1000))
    H = int(round(chapa_h * 1000))
    sid = f"s{chapa.get('numero', 0)}_{abs(hash(str(chapa.get('estrategia','')))) % 99999}"
    parts = [
        f'<svg class="cut-svg" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid meet" shape-rendering="crispEdges" style="aspect-ratio:{W}/{H}">',
        f'<rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff" stroke="#0f172a" stroke-width="4" vector-effect="non-scaling-stroke"/>',
    ]
    for i, p in enumerate(chapa.get("pecas", [])):
        x = int(round(float(p["x"]) * 1000))
        y = int(round(float(p["y"]) * 1000))
        w = max(1, int(round(float(p["w_draw"]) * 1000)))
        h = max(1, int(round(float(p["h_draw"]) * 1000)))
        clip_id = f"clip_{sid}_{i}"
        codigo = html_escape(p.get("codigo", ""))
        desc = html_escape(str(p.get("descricao", "")))
        medida = f"{int(round(float(p['w_draw'])*1000))}x{int(round(float(p['h_draw'])*1000))}"
        fill = cor_hash(p.get("codigo", i))

        area_mm2 = w * h
        if area_mm2 < 90000:
            fs1, fs2, fs3 = 0, 0, 0
        elif area_mm2 < 180000:
            fs1, fs2, fs3 = 15, 0, 0
        elif area_mm2 < 350000:
            fs1, fs2, fs3 = 16, 14, 0
        else:
            fs1 = max(17, min(32, int(min(w / 12, h / 5))))
            fs2 = max(15, min(24, int(fs1 * 0.82)))
            fs3 = max(13, min(20, int(fs1 * 0.72)))

        parts.append(f'<clipPath id="{clip_id}"><rect x="{x+4}" y="{y+4}" width="{max(w-8,1)}" height="{max(h-8,1)}"/></clipPath>')
        parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" stroke="#475569" stroke-width="3" vector-effect="non-scaling-stroke"/>')

        if fs1 > 0:
            parts.append(f'<g clip-path="url(#{clip_id})" font-family="Arial, Helvetica, sans-serif" fill="#0f172a">')
            cy = y + fs1 + 10
            parts.append(f'<text x="{x+10}" y="{cy}" font-size="{fs1}" font-weight="700">{codigo}</text>')
            if fs2 > 0:
                cy += fs2 + 6
                parts.append(f'<text x="{x+10}" y="{cy}" font-size="{fs2}" font-weight="600">{medida}</text>')
            if fs3 > 0 and h >= 140 and w >= 180:
                for linha in _quebrar_linhas_svg(desc, max_chars=max(18, min(34, w // 28))):
                    cy += fs3 + 5
                    if cy > y + h - 10:
                        break
                    parts.append(f'<text x="{x+10}" y="{cy}" font-size="{fs3}" fill="#334155">{linha}</text>')
            parts.append('</g>')
    parts.append("</svg>")
    return "".join(parts)


def _nome_modo_plano(modo):
    if modo == "industrial":
        return "Industrial com lateral reforçada"
    if modo == "guilhotina":
        return "Guilhotina por faixas"
    if modo == "oportunidades":
        return "Oportunidades agressivo"
    if modo == "otimizado":
        return "Otimizado comparativo"
    return "Encaixe livre tradicional"


def resumo_planos_por_tipo_html(planos):
    """Resumo real do plano gerado por tipo de chapa/material."""
    if not planos:
        return ""

    rows = []
    total_chapas = 0
    total_area_pecas = 0.0
    total_area_chapas = 0.0
    total_perda = 0.0

    for grupo in planos:
        chapas = grupo.get("chapas", [])
        if not chapas:
            continue

        material = grupo.get("material", "")
        primeira = chapas[0]
        chapa_w = float(primeira.get("chapa_w", 0) or 0)
        chapa_h = float(primeira.get("chapa_h", 0) or 0)
        area_chapa = chapa_w * chapa_h
        qtd_chapas = len(chapas)
        area_pecas = sum(float(ch.get("area_pecas", 0) or 0) for ch in chapas)
        area_chapas = qtd_chapas * area_chapa
        perda = max(area_chapas - area_pecas, 0.0)
        aproveitamento = area_pecas / area_chapas if area_chapas else 0.0
        aproveitamentos = [float(ch.get("aproveitamento", 0) or 0) for ch in chapas]
        melhor = max(aproveitamentos) if aproveitamentos else 0.0
        pior = min(aproveitamentos) if aproveitamentos else 0.0
        qtd_pecas = sum(len(ch.get("pecas", [])) for ch in chapas)
        meta = grupo.get("meta_alvo", META_APROVEITAMENTO_PADRAO)
        status = '<span class="badge ok">Meta atingida</span>' if aproveitamento >= meta - 1e-9 else '<span class="badge danger">Abaixo da meta</span>'

        total_chapas += qtd_chapas
        total_area_pecas += area_pecas
        total_area_chapas += area_chapas
        total_perda += perda

        rows.append(
            f'<tr>'
            f'<td>{html_escape(material)}</td>'
            f'<td>{fmt_m(chapa_w)} x {fmt_m(chapa_h)} m</td>'
            f'<td class="num"><b>{qtd_chapas}</b></td>'
            f'<td class="num">{qtd_pecas}</td>'
            f'<td class="num">{fmt_num(area_pecas,3)} m²</td>'
            f'<td class="num">{fmt_num(area_chapas,3)} m²</td>'
            f'<td class="num">{fmt_num(perda,3)} m²</td>'
            f'<td class="num"><b>{fmt_num(aproveitamento*100,1)}%</b></td>'
            f'<td class="num">{fmt_num(melhor*100,1)}%</td>'
            f'<td class="num">{fmt_num(pior*100,1)}%</td>'
            f'<td>{status}</td>'
            f'</tr>'
        )

    if not rows:
        return ""

    aproveitamento_total = total_area_pecas / total_area_chapas if total_area_chapas else 0.0
    return (
        '<div class="card">'
        '<h2>Resumo real dos planos por tipo de chapa</h2>'
        '<p class="hint">Este resumo considera o plano de corte gerado, não apenas o cálculo por área. Mostra quantas chapas foram usadas, quanto foi aproveitado e quanto ficou de perda em cada tipo de chapa/material.</p>'
        '<div class="grid">'
        f'<div class="stat"><span class="hint">Total de chapas no plano</span><br><b>{total_chapas}</b></div>'
        f'<div class="stat"><span class="hint">Aproveitamento geral do plano</span><br><b>{fmt_num(aproveitamento_total*100,1)}%</b></div>'
        f'<div class="stat"><span class="hint">Área aproveitada total</span><br><b>{fmt_num(total_area_pecas,3)} m²</b></div>'
        f'<div class="stat"><span class="hint">Perda total estimada</span><br><b>{fmt_num(total_perda,3)} m²</b></div>'
        '</div>'
        '<table><thead><tr>'
        '<th>Tipo de chapa / material</th><th>Medida chapa</th><th class="num">Chapas</th><th class="num">Peças</th>'
        '<th class="num">Área peças</th><th class="num">Área chapas</th><th class="num">Perda</th>'
        '<th class="num">Aproveit. médio</th><th class="num">Melhor chapa</th><th class="num">Pior chapa</th><th>Status</th>'
        '</tr></thead><tbody>' + "".join(rows) + '</tbody></table></div>'
    )


def html_planos(planos):
    if not planos:
        return ""
    html = [
        '<div class="card"><h2>Plano de corte visual</h2>'
        '<p class="hint">O modo recomendado agora é Industrial com lateral reforçada: peças iguais ficam agrupadas em blocos/faixas. A lógica prioriza o preenchimento de laterais e rodapés com blocos girados 90° quando couber, sem espalhar peças de forma bagunçada.</p></div>',
        resumo_planos_por_tipo_html(planos),
    ]
    for grupo in planos:
        material = grupo["material"]
        modo = _nome_modo_plano(grupo.get("modo", "encaixe"))
        ap = grupo.get("aproveitamento_geral", 0) * 100
        meta = grupo.get("meta_alvo", META_APROVEITAMENTO_PADRAO) * 100
        status = '<span class="badge ok">Meta atingida</span>' if grupo.get("meta_atingida") else '<span class="badge danger">Abaixo da meta</span>'
        html.append(
            f'<div class="card"><h2>{html_escape(material)} — Resumo do plano</h2>'
            f'<div class="grid"><div class="stat"><span class="hint">Aproveitamento geral</span><br><b>{fmt_num(ap,1)}%</b><br>{status}</div>'
            f'<div class="stat"><span class="hint">Meta</span><br><b>{fmt_num(meta,1)}%</b></div>'
            f'<div class="stat"><span class="hint">Chapas usadas</span><br><b>{grupo.get("chapas_total",0)}</b></div>'
            f'<div class="stat"><span class="hint">Sobra total</span><br><b>{fmt_num(grupo.get("sobra_total_m2",0),3)} m²</b></div></div>'
            f'<p class="hint">Modo escolhido: <b>{html_escape(modo)}</b>. Estratégia: <b>{html_escape(grupo.get("estrategia", ""))}</b>. '
            f'Limite teórico por área, sem considerar geometria: {fmt_num(grupo.get("limite_teorico_area",0)*100,1)}%.</p></div>'
        )
        for ch in grupo["chapas"]:
            html.append(
                f'<div class="card sheet-card"><h2>{html_escape(material)} — Chapa {ch["numero"]} — {html_escape(_nome_modo_plano(ch.get("modo", grupo.get("modo", "encaixe"))))}</h2>'
                f'<div class="grid"><div class="stat"><span class="hint">Aproveitamento real desta chapa</span><br><b>{fmt_num(ch["aproveitamento"]*100,1)}%</b></div>'
                f'<div class="stat"><span class="hint">Área das peças</span><br><b>{fmt_num(ch["area_pecas"],3)} m²</b></div>'
                f'<div class="stat"><span class="hint">Sobra estimada</span><br><b>{fmt_num(ch["sobra_m2"],3)} m²</b></div></div>'
                f'<div class="svgwrap">{svg_chapa(ch)}</div></div>'
            )
    return "\n".join(x for x in html if x)

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
    server_version = "AppCortePorCodigo/2.1"
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
