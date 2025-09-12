# main.py — AD Relatório Financeiro — v8.70

from __future__ import annotations

import os
import hashlib
import unicodedata as ud
from io import BytesIO
from datetime import date
from typing import Optional, List, Tuple
from collections import defaultdict

import locale as _locale
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
from sqlalchemy.ext.declarative import declarative_base

from st_aggrid import AgGrid, GridOptionsBuilder

from reportlab.lib.pagesizes import A4, portrait
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER

# ===================== CONFIG =====================
ADMIN_USERNAME = "admin"  # usuário mestre

st.set_page_config(page_title="AD Relatório Financeiro", page_icon="⛪", layout="wide")

CSS = """
<style>
:root{ --brand:#0f172a; --brand-2:#1d4ed8; --accent:#16a34a; --danger:#dc2626; --muted:#6b7280;
--bg:#f8fafc; --card:#ffffff; --ring:#e5e7eb; }
html, body { background: var(--bg); height:auto!important; overflow-y:auto!important; overscroll-behavior-y:contain; -webkit-overflow-scrolling:touch;}
.stApp{height:auto!important; overflow:visible!important; padding-bottom:max(16px, env(safe-area-inset-bottom))!important;}
section.main>div{overflow:visible!important;}
/* esconder coisas que atrapalham em mobile */
@media (pointer:coarse){
  footer,[data-testid="stToolbar"],[aria-label*="Manage app"],[title*="Manage app"]{display:none!important;}
  div[style*="position: fixed"][style*="bottom"]{pointer-events:none!important;touch-action:pan-y!important;}
}
header[data-testid="stHeader"]{ background: linear-gradient(180deg,#ffffff, #f6f9ff) !important; border-bottom:1px solid var(--ring); }
.block-container{ padding-top: .9rem !important; }
[data-testid="stSidebar"]{ background: linear-gradient(180deg,#ffffff,#fbfdff); border-right:1px solid var(--ring); }
[data-testid="stSidebar"] img{ border-radius: .6rem; border:1px solid var(--ring); box-shadow: 0 4px 16px rgba(0,0,0,.06); }
.page-title{ font-family:'Nunito','Inter',system-ui; font-size:36px; font-weight:900; letter-spacing:.4px; color: var(--brand); margin:.25rem 0 1rem; }
h2, .stMarkdown h2, .st-subheader{ color:#0f172a!important; font-weight:900 !important; }
.st-container-card{ border: 1px solid var(--ring); border-radius: 1rem; padding: 1rem; margin-bottom: 1.15rem; background: var(--card); box-shadow: 0 10px 30px rgb(31 58 138 / .06), 0 2px 8px rgb(0 0 0 / .04); position: relative; }
.st-container-card::before{ content:""; position: absolute; left:0; top:0; bottom:0; width:6px; background: linear-gradient(180deg,var(--brand-2), var(--brand)); border-top-left-radius:1rem; border-bottom-left-radius:1rem; }
div[data-testid="stMetric"]{ padding: .75rem .9rem; border:1px solid var(--ring); border-radius:.9rem; background:var(--card); box-shadow:0 1px 2px rgb(0 0 0 / .06); }
div[data-testid="stMetricLabel"]{ color:#334155; font-weight:800; }
div[data-testid="stMetricValue"]{ font-size:26px!important; line-height:1.15!important; white-space:nowrap!important; font-weight:900!important; }
.stat-card{ border:1px solid var(--ring); border-radius:.9rem; background:#fff; padding:.9rem 1rem; box-shadow:0 1px 2px rgba(0,0,0,.06); height:86px; display:flex; flex-direction:column; justify-content:center; }
.stat-label{ font-size:.92rem; color:#334155; font-weight:800; margin-bottom:.18rem; }
.stat-value{ font-size:1.25rem; font-weight:900; color:#111827; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
/* remover tooltip (causava duplicação visual) */
.stat-card .tooltip{ display:none!important; }
label, .stTextInput label, .stSelectbox label, .stNumberInput label{ color:#0f172a; font-weight:800; }
.stTextInput input, .stNumberInput input, .stDateInput input{ border:1px solid var(--ring)!important; border-radius:.65rem!important; background:#fff!important; }
.stSelectbox [data-baseweb="select"]>div{ border:1px solid var(--ring)!important; border-radius:.65rem!important; background:#fff!important; }
.stTextInput input:focus, .stNumberInput input:focus, .stDateInput input:focus{ outline:none!important; border-color:#93c5fd!important; box-shadow:0 0 0 4px rgba(37,99,235,.18)!important; }
.stButton>button{ border:1px solid #2563eb!important; color:#fff!important; background: linear-gradient(180deg,#3b82f6,#2563eb)!important; font-weight:900!important; border-radius:.7rem!important; padding:.52rem .95rem!important; box-shadow:0 2px 6px rgba(37,99,235,.25)!important; }
.stButton>button:hover{ filter:brightness(1.03); transform:translateY(-1px); }
.btn-green button{ background: linear-gradient(180deg,#22c55e,#16a34a)!important; border-color:#16a34a!important; color:#fff!important; }
.btn-red button{ background: linear-gradient(180deg,#ef4444,#dc2626)!important; border-color:#dc2626!important; color:#fff!important; }
.stDownloadButton>button{ border:1px solid var(--ring)!important; color:#0f172a!important; background:#fff!important; border-radius:.7rem!important; font-weight:900!important; }
[data-testid="stDataFrame"]{ border:1px solid var(--ring); border-radius:.9rem; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,.04); }
[data-testid="stDataFrame"] thead tr th{ position:sticky; top:0; z-index:2; background:#f1f5f9!important; font-weight:900!important; color:#0f172a!important; border-bottom:1px solid var(--ring)!important; }
[data-testid="stDataFrame"] tbody tr:nth-child(even) td{ background:#fcfcfd; }
[data-testid="stDataFrame"] tbody tr:hover td{ background:#f8fbff; }
.st-expander{ border:1px solid var(--ring)!important; border-radius:.9rem!important; background:#fff!important; box-shadow:0 4px 16px rgb(31 58 138 /.06)!important; }
.st-expanderHeader{ font-weight:900!important; color:#0f172a!important; }
.small-note{ color: var(--muted); font-size:.92rem; }
.cong-title{ font-weight:900; font-size:1.05rem; color:#0f172a; margin-bottom:.35rem; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# Passivo (mantém compatibilidade com versões antigas do app)
components.html("<script>/* mobile scroll noop */</script>", height=0)

# ===================== LOCALE =====================
def _set_locale_ptbr():
    for loc in ("pt_BR.utf8","pt_BR.UTF-8","pt_BR","Portuguese_Brazil.1252"):
        try: _locale.setlocale(_locale.LC_TIME, loc); return
        except Exception: pass
_set_locale_ptbr()

# ===================== UTILS =====================
MONTHS = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
MONTHS_SHORT = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

def format_currency(v) -> str:
    try: x = float(v or 0.0)
    except Exception: x = 0.0
    return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def format_date(d: date) -> str: return d.strftime("%d/%m/%Y")

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = ud.normalize("NFD", s)
    return "".join(c for c in s if ud.category(c) != "Mn").replace(" ","")

def month_bounds(ref: date) -> Tuple[date,date]:
    start = ref.replace(day=1)
    end = date(start.year + (start.month==12), (start.month%12)+1, 1)
    return start, end

def get_month_selector(label="Mês de referência") -> date:
    today = date.today()
    colm, coly = st.columns([2,1])
    with colm: m = st.selectbox(f"{label} — Mês", list(range(1,13)), index=today.month-1, format_func=lambda i: MONTHS[i-1])
    with coly: y = st.number_input("Ano", value=today.year, step=1, format="%d")
    return date(int(y), int(m), 1)

def _confirm_ok(txt: str) -> bool: return str(txt or "").strip().upper() == "EXCLUIR"

# ===================== DB =====================
Base = declarative_base()

class User(Base):
    __tablename__="users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)
    congregation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("congregations.id"))
    congregation: Mapped[Optional["Congregation"]] = relationship(back_populates="users")

class Congregation(Base):
    __tablename__="congregations"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    users: Mapped[List["User"]] = relationship(back_populates="congregation")
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="congregation")
    tithes: Mapped[List["Tithe"]] = relationship(back_populates="congregation")

class Category(Base):
    __tablename__="categories"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    type: Mapped[str] = mapped_column(String)  # DOAÇÃO | SAÍDA
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="category")

class Transaction(Base):
    __tablename__="transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    type: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    amount: Mapped[float] = mapped_column(Float)
    description: Mapped[Optional[str]] = mapped_column(String)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)
    category: Mapped["Category"] = relationship(back_populates="transactions", lazy="joined")
    congregation: Mapped["Congregation"] = relationship(back_populates="transactions")

class Tithe(Base):
    __tablename__="tithes"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    tither_name: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)
    congregation: Mapped["Congregation"] = relationship(back_populates="tithes")

TYPE_IN = "DOAÇÃO"
TYPE_OUT = "SAÍDA"
LEGACY_TYPES = {"DOAÇÃO":["RECEITA"], "SAÍDA":["DESPESA"]}

@st.cache_resource
def get_engine():
    db_url = st.secrets.get("DATABASE_URL", os.environ.get("DATABASE_URL"))
    if not db_url: db_url = "sqlite:///database.db"
    return create_engine(db_url, pool_pre_ping=True)

@st.cache_resource
def get_sessionmaker():
    engine = get_engine()
    Base.metadata.create_all(engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

SessionLocal = get_sessionmaker()

# ===================== AUTH =====================
def hash_password(pwd: str) -> str:
    salt = os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt, 100000)
    return salt.hex()+":"+h.hex()

def verify_password(pwd: str, stored: str) -> bool:
    salt_hex, h_hex = stored.split(":")
    salt = bytes.fromhex(salt_hex); h = bytes.fromhex(h_hex)
    nh = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt, 100000)
    return nh == h

APP_SECRET = st.secrets.get("APP_SECRET", os.environ.get("APP_SECRET", "dev-secret-change-me"))

def make_token(user: "User") -> str:
    data = f"{user.id}:{user.password_hash}:{APP_SECRET}"
    return hashlib.sha256(data.encode()).hexdigest()[:32]

def _qp_get_first(key: str) -> Optional[str]:
    try:
        val = st.query_params.get(key)
    except Exception:
        return None
    if val is None: return None
    if isinstance(val, (list,tuple)): return val[0] if val else None
    return str(val)

def _set_auth_query_params(uid: int, token: str):
    try:
        st.query_params["uid"] = str(uid)
        st.query_params["t"] = token
    except Exception:
        pass

def _clear_auth_query_params():
    try:
        if "uid" in st.query_params: del st.query_params["uid"]
        if "t" in st.query_params: del st.query_params["t"]
    except Exception:
        pass

def hydrate_auth_from_query():
    uid_str = _qp_get_first("uid")
    tok = _qp_get_first("t")
    if uid_str and tok and not st.session_state.get("uid"):
        try: uid = int(uid_str)
        except Exception: return
        with SessionLocal() as db:
            user = db.get(User, uid)
            if user and tok == make_token(user):
                set_logged_user(user)

if "uid" not in st.session_state: st.session_state.uid=None

def current_user() -> Optional["User"]:
    uid = st.session_state.get("uid")
    if not uid: return None
    with SessionLocal() as db: return db.get(User, uid)

def is_admin_general(user: "User") -> bool:
    return (user.username or "").strip().lower() == ADMIN_USERNAME.lower()

def set_logged_user(user: "User"):
    st.session_state.uid = user.id
    st.session_state.username = user.username
    st.session_state.role = user.role
    st.session_state.congregation_id = user.congregation_id
    _set_auth_query_params(user.id, make_token(user))

def do_logout():
    st.session_state.clear()
    _clear_auth_query_params()
    st.rerun()

def login_ui():
    st.markdown("<h1 class='page-title'>AD Relatório Financeiro</h1>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username==u))
            if user and verify_password(p, user.password_hash):
                set_logged_user(user)
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

# ===================== SEED =====================
CONGREGACOES_PADRAO = [
    "Sede","Abreus","Alto Alencar","Alto da Aliança","Alto do Cruzeiro",
    "Rodeadouro","Dr. Humberto","Jatobá","Massaroca","Riacho Seco"
]

def ensure_seed():
    with SessionLocal() as db:
        if db.scalar(select(func.count(Category.id))) == 0:
            for nm,tp in [("Dízimo",TYPE_IN),("Oferta",TYPE_IN),("Missões",TYPE_IN),
                          ("Aluguel",TYPE_OUT),("Energia",TYPE_OUT),("Assistência Social",TYPE_OUT),
                          ("Produtos de Limpeza",TYPE_OUT),("Transporte",TYPE_OUT),("Material de Expediente",TYPE_OUT),
                          ("Missões (Saída)",TYPE_OUT)]:
                if not db.scalar(select(Category).where(Category.name==nm)):
                    db.add(Category(name=nm, type=tp))
        existentes=set(db.scalars(select(Congregation.name)).all())
        for n in CONGREGACOES_PADRAO:
            if n not in existentes: db.add(Congregation(name=n))
        sede = db.scalar(select(Congregation).where(Congregation.name=="Sede"))
        if db.scalar(select(User).where(User.username==ADMIN_USERNAME)) is None:
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password("123456"), role="SEDE", congregation_id=sede.id if sede else None))
        db.commit()

# ===================== HELPERS DE NEGÓCIO =====================
def categories_for_type(db: Session, kind: str) -> List[Category]:
    kinds = [kind] + LEGACY_TYPES.get(kind, [])
    cats = list(db.scalars(select(Category).where(Category.type.in_(kinds))).all())
    if kind == TYPE_IN:
        priority = {"dízimo":0,"dizimo":0,"oferta":1,"missões":2,"missoes":2}
        cats.sort(key=lambda c:(priority.get(_norm(c.name),100), _norm(c.name)))
    else:
        cats.sort(key=lambda c:_norm(c.name))
    return cats

def cong_options_for(user: "User", db: Session) -> List[Congregation]:
    if user.role == "SEDE":
        return db.scalars(select(Congregation).order_by(Congregation.name)).all()
    else:
        if user.congregation_id:
            c = db.get(Congregation, user.congregation_id)
            return [c] if c else []
        return []

def order_congs_sede_first(congs: List[Congregation]) -> List[Congregation]:
    sede=[c for c in congs if _norm(c.name)=="sede"]
    others=sorted([c for c in congs if _norm(c.name)!="sede"], key=lambda x:_norm(x.name))
    return (sede+others) if sede else others

# ===================== COLETA MENSAL =====================
def _collect_month_data(db, cong_id: int, start: date, end: date, is_all: bool = False):
    tx_in_query = select(Transaction).options(joinedload(Transaction.category)).where(
        Transaction.date >= start, Transaction.date < end, Transaction.type.in_((TYPE_IN, "RECEITA"))
    ).order_by(Transaction.date)
    if not is_all:
        tx_in_query = tx_in_query.where(Transaction.congregation_id == cong_id)
    tx_in = db.scalars(tx_in_query).all()

    tithes_query = select(Tithe).where(Tithe.date >= start, Tithe.date < end).order_by(Tithe.date)
    if not is_all:
        tithes_query = tithes_query.where(Tithe.congregation_id == cong_id)
    tithes = db.scalars(tithes_query).all()

    tx_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
        Transaction.date >= start, Transaction.date < end, Transaction.type.in_((TYPE_OUT, "DESPESA"))
    ).order_by(Transaction.date)
    if not is_all:
        tx_out_query = tx_out_query.where(Transaction.congregation_id == cong_id)
    tx_out = db.scalars(tx_out_query).all()

    def _is_dizimo_tx(t: Transaction) -> bool:
        return t.category and _norm(t.category.name) in ("dizimo","dízimo")
    def _is_oferta_tx(t: Transaction) -> bool:
        return t.category and _norm(t.category.name) == "oferta"
    def _is_mission_entry(t: Transaction) -> bool:
        return t.category and _norm(t.category.name) in ("missoes","missões")

    total_dizimos_tithe = sum(float(t.amount) for t in tithes)
    total_dizimos_trans = sum(float(t.amount) for t in tx_in if _is_dizimo_tx(t))
    total_dizimos_final = max(total_dizimos_tithe, total_dizimos_trans)

    total_ofertas = sum(float(t.amount) for t in tx_in if _is_oferta_tx(t))
    total_missoes = sum(float(t.amount) for t in tx_in if _is_mission_entry(t))
    total_entradas_outros = sum(float(t.amount) for t in tx_in if not (_is_dizimo_tx(t) or _is_oferta_tx(t) or _is_mission_entry(t)))

    total_geral_entradas_sem_missoes = total_dizimos_final + total_ofertas + total_entradas_outros
    total_saidas = sum(float(t.amount) for t in tx_out)
    saldo = total_geral_entradas_sem_missoes + total_missoes - total_saidas

    return {
        "tx_in": tx_in,
        "tithes": tithes,
        "tx_out": tx_out,
        "totals": {
            "dizimos": total_dizimos_final,
            "ofertas": total_ofertas,
            "missoes": total_missoes,
            "entradas_outros": total_entradas_outros,
            "entradas_total_sem_missoes": total_geral_entradas_sem_missoes,
            "saidas_total": total_saidas,
            "saldo": saldo
        }
    }

# ===================== PDF BUILDERS =====================
def build_dizimista_search_pdf(df: pd.DataFrame, ano_pesq: int, cong_sel: str, mes_sel: str, nome_q: str) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=portrait(A4), leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)
    table_style = TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (4, 1), (4, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ])

    story = []
    story.append(Paragraph("Relatório de Pesquisa de Dizimistas", title_style))
    story.append(Paragraph(f"Ano: {ano_pesq} | Congregação: {cong_sel} | Mês: {mes_sel}", subtitle_style))
    if nome_q.strip():
        story.append(Paragraph(f"Filtrado por: '{nome_q}'", subtitle_style))
    story.append(Spacer(1, 0.5*cm))

    data_table = [df.columns.tolist()] + df.values.tolist()
    total_value = float(df["Total no ano (R$)"].sum())
    total_row = ["", "", "", "Total Geral:", total_value, "", ""]
    data_table.append(total_row)

    for row in data_table[1:]:
        if isinstance(row[4], float):
            row[4] = format_currency(row[4])

    tbl = Table(data_table, colWidths=[3.5*cm, 3.5*cm, 2.0*cm, 2.5*cm, 2.5*cm, 2.0*cm, 2.0*cm])
    tbl.setStyle(table_style)
    story.append(tbl)
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(f"Dizimistas encontrados: **{len(df)}**", styles['Normal']))
    story.append(Paragraph(f"Total geral da pesquisa: **{format_currency(total_value)}**", styles['Normal']))
    doc.build(story)
    return buf.getvalue()

def build_full_statement_pdf(cong_id: int, cong_name: str, ref: date) -> bytes:
    buf = BytesIO()
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['Heading2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")

    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    story: List = []

    with SessionLocal() as db:
        start, end = month_bounds(ref)
        data = _collect_month_data(db, cong_id, start, end)

        entries_by_date = defaultdict(lambda: {"dizimo": 0.0, "oferta": 0.0, "missoes": 0.0, "outros": 0.0})
        for t in data["tx_in"]:
            if t.category and _norm(t.category.name) in ("dizimo","dízimo"):
                entries_by_date[t.date]["dizimo"] += float(t.amount)
            elif t.category and _norm(t.category.name) == "oferta":
                entries_by_date[t.date]["oferta"] += float(t.amount)
            elif t.category and _norm(t.category.name) in ("missoes","missões"):
                entries_by_date[t.date]["missoes"] += float(t.amount)
            else:
                entries_by_date[t.date]["outros"] += float(t.amount)
        for t in data["tithes"]:
            entries_by_date[t.date]["dizimo"] += float(t.amount)

        tx_in_data = [["Data do Culto", "Dízimo", "Oferta", "Total"]]
        for d, totals in sorted(entries_by_date.items()):
            diz = totals["dizimo"]; ofe = totals["oferta"]; total = diz + ofe
            tx_in_data.append([d.strftime("%d/%m/%Y"), format_currency(diz), format_currency(ofe), format_currency(total)])

        tithe_data = [["Data", "Nome do Dizimista", "Valor"]]
        tithe_data.extend([[t.date.strftime("%d/%m/%Y"), t.tither_name, format_currency(t.amount)] for t in data["tithes"]])

        tx_out_data = [["Data", "Categoria", "Descrição", "Valor"]]
        tx_out_data.extend([[t.date.strftime("%d/%m/%Y"), t.category.name, t.description or "", format_currency(t.amount)] for t in data["tx_out"]])

    story.append(Paragraph("Prestação de Contas Mensal", title_style))
    story.append(Paragraph(f"Congregação: {cong_name}", subtitle_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))

    story.append(Paragraph("1. Entradas (Resumo: Dízimo e Oferta)", heading_style))
    if len(tx_in_data) > 1:
        tbl_in = Table(tx_in_data, colWidths=[3.2*cm, 4.0*cm, 4.0*cm, 5.3*cm])
        tbl_in.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada registrada.", styles['Normal']))
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("2. Dizimistas (Lançamentos Nominais)", heading_style))
    if len(tithe_data) > 1:
        tbl_t = Table(tithe_data, colWidths=[3*cm, 9.5*cm, 4.5*cm])
        tbl_t.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (2, 1), (2, -1), "RIGHT"),
        ]))
        story.append(tbl_t)
    else:
        story.append(Paragraph("Nenhum dízimo registrado.", styles['Normal']))
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("3. Saídas", heading_style))
    if len(tx_out_data) > 1:
        tbl_out = Table(tx_out_data, colWidths=[2.5*cm, 4.5*cm, 7.5*cm, 3.5*cm])
        tbl_out.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]))
        story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída registrada.", styles['Normal']))
    story.append(Spacer(1, 1*cm))

    with SessionLocal() as db:
        start, end = month_bounds(ref)
        totals = _collect_month_data(db, cong_id, start, end)["totals"]
    summary_data = [
        ["Total de Dízimos", format_currency(totals["dizimos"])],
        ["Total de Ofertas", format_currency(totals.get("ofertas", 0.0))],
        ["Total de Entradas (caixa principal)", format_currency(totals["entradas_total_sem_missoes"])],
        ["Total de Saídas", format_currency(totals["saidas_total"])],
        ["Saldo do Mês", format_currency(totals["entradas_total_sem_missoes"] - totals["saidas_total"])],
    ]
    summary_table = Table(summary_data, colWidths=[8*cm, 8*cm])
    summary_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e2fbe2")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(summary_table)

    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("5. Missões (Resumo do Mês)", heading_style))
    missions_table = Table(
        [["Descrição", "Valor"], ["Total de Missões no Mês", format_currency(totals.get("missoes", 0.0))]],
        colWidths=[8*cm, 8*cm]
    )
    missions_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
    ]))
    story.append(missions_table)

    doc.build(story)
    return buf.getvalue()

def build_consolidated_pdf(agg_total: list, ref: date) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['Heading2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    table_style_main = TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e2fbe2")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ])
    story: List = []
    story.append(Paragraph("Relatório Mensal", title_style))
    story.append(Paragraph(f"Mês de Referência: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 1*cm))

    table_data = [["Congregação", "Entradas (D+O+Outras)", "Saídas", "Saldo"]]
    total_entradas = total_saidas = total_saldo = 0.0
    missions_rows = []; total_missoes = 0.0

    for c_name, entradas, saidas, saldo, missoes in agg_total:
        table_data.append([c_name, format_currency(entradas), format_currency(saidas), format_currency(saldo)])
        total_entradas += entradas; total_saidas += saidas; total_saldo += saldo
        missions_rows.append([c_name, missoes]); total_missoes += missoes

    table_data.append(["TOTAL GERAL", format_currency(total_entradas), format_currency(total_saidas), format_currency(total_saldo)])
    tbl = Table(table_data, colWidths=[5*cm, 4*cm, 4*cm, 4*cm]); tbl.setStyle(table_style_main)
    story.append(tbl)

    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("Missões por Congregação (Entradas)", heading_style))
    missions_table_data = [["Congregação", "Missões (entrada)"]]
    for r in missions_rows:
        missions_table_data.append([r[0], format_currency(r[1])])
    missions_table_data.append(["TOTAL GERAL DE MISSÕES", format_currency(total_missoes)])
    tbl_m = Table(missions_table_data, colWidths=[9*cm, 9*cm])
    tbl_m.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#eef2ff")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(tbl_m)
    doc.build(story)
    return buf.getvalue()

# ===================== PÁGINAS =====================
def render_stat_card(col, label: str, full_text: str):
    col.markdown(
        f"""
        <div class="stat-card">
          <div class="stat-label">{label}</div>
          <div class="stat-value">{full_text}</div>
          <div class="tooltip">{full_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def page_lancamentos(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

        st.markdown("<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)

        congs = cong_options_for(user, db)
        if not congs:
            st.info("Nenhuma congregação disponível."); return

        if user.role == "SEDE":
            congs_ordered = order_congs_sede_first(congs)
            cong_sel = st.selectbox("Selecione a congregação", [c.name for c in congs_ordered], key="lan_cong_sel")
            cong_obj = next(c for c in congs_ordered if c.name == cong_sel)
        else:
            cong_obj = congs[0]
        st.markdown(f"<div class='cong-title'>CONGREGAÇÃO: {cong_obj.name.upper()}</div>", unsafe_allow_html=True)

        # ---------- ENTRADA ----------
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar ENTRADA (Doação)")
        with st.form("form_entrada", clear_on_submit=True):
            st.date_input("Data do Culto", value=date.today(), key="ent_data")
            cats_in = categories_for_type(db, TYPE_IN)
            cat_names_in = [c.name for c in cats_in] or ["—"]
            desired = ["Dízimo","Oferta","Missões"]
            desired_norm = [_norm(x) for x in desired]
            top = [n for n in cat_names_in if _norm(n) in desired_norm]
            rest = [n for n in cat_names_in if _norm(n) not in desired_norm]
            cat_display = top + rest
            st.selectbox("Categoria (ordem fixa: Dízimo, Oferta, Missões)", cat_display, key="ent_cat")
            st.text_input("Descrição (opcional)", key="ent_desc")
            if _norm(st.session_state.get("ent_cat","")) == "oferta":
                st.checkbox("Oferta de missões?", key="ent_flag_missoes")
            st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="ent_valor")
            if st.form_submit_button("Salvar ENTRADA", type="primary"):
                with SessionLocal() as _db:
                    cat_name = st.session_state["ent_cat"]
                    if st.session_state.get("ent_flag_missoes"):
                        cat_name = "Missões"
                        if not _db.scalar(select(Category).where(Category.name=="Missões")):
                            _db.add(Category(name="Missões", type=TYPE_IN)); _db.commit()
                    cat_obj = _db.scalar(select(Category).where(Category.name==cat_name))
                    if not cat_obj:
                        st.error("Informe a categoria."); return
                    _db.add(Transaction(
                        date=st.session_state["ent_data"], type=TYPE_IN, category_id=cat_obj.id,
                        amount=st.session_state["ent_valor"], description=(st.session_state["ent_desc"] or None),
                        congregation_id=cong_obj.id, payment_method=None
                    ))
                    _db.commit(); st.success("Entrada registrada."); st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # ---------- DÍZIMO ----------
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Salvar DIZIMISTA")
        with st.form("form_dizimo", clear_on_submit=True):
            st.date_input("Data do Culto", value=date.today(), key="dz_data")
            st.text_input("Nome do dizimista", key="dz_nome")
            st.number_input("Valor dízimo (R$)", min_value=0.0, step=1.0, format="%.2f", key="dz_valor")
            st.selectbox("Forma de Pagamento", ["Dinheiro","PIX"], key="dz_payment_method")
            if st.form_submit_button("Salvar DIZIMISTA", type="primary"):
                nome = (st.session_state.get("dz_nome") or "").strip()
                valor = float(st.session_state.get("dz_valor") or 0.0)
                dta = st.session_state.get("dz_data") or date.today()
                if not nome:
                    st.error("Informe o nome do dizimista."); return
                with SessionLocal() as _db:
                    _db.add(Tithe(date=dta, tither_name=nome, amount=valor, congregation_id=cong_obj.id,
                                  payment_method=st.session_state.get("dz_payment_method")))
                    _db.commit(); st.success("Dízimo registrado."); st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # ---------- SAÍDA ----------
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar SAÍDA")
        with st.form("form_saida", clear_on_submit=True):
            st.date_input("Data", value=date.today(), key="sai_data")
            cats_out = categories_for_type(db, TYPE_OUT)
            st.selectbox("Tipo da saída (Categoria)", [c.name for c in cats_out] or ["—"], key="sai_cat")
            st.text_input("Descrição (opcional)", key="sai_desc")
            st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="sai_valor")
            if st.form_submit_button("Salvar SAÍDA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name==st.session_state["sai_cat"]))
                    if not cat_obj:
                        st.error("Informe o tipo de saída."); return
                    _db.add(Transaction(
                        date=st.session_state["sai_data"], type=TYPE_OUT, category_id=cat_obj.id,
                        amount=st.session_state["sai_valor"], description=(st.session_state["sai_desc"] or None),
                        congregation_id=cong_obj.id,
                    ))
                    _db.commit(); st.success("Saída registrada."); st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

def page_relatorio_entrada(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

        st.markdown("<h1 class='page-title'>Relatório de Entrada</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        congs = cong_options_for(user, db)
        if user.role == "SEDE":
            ordered = order_congs_sede_first(congs)
            esc_opt = ["Todas as congregações"] + [c.name for c in ordered]
            esc = st.selectbox("Escopo", esc_opt, key="re_escopo")
            is_all = (esc == "Todas as congregações")
            cong_obj = None if is_all else next(c for c in ordered if c.name == esc)
        else:
            cong_obj = congs[0] if congs else None; is_all = False

        if not cong_obj and not is_all:
            st.info("Sem congregação vinculada."); return
        if cong_obj:
            st.info(f"Escopo: **{cong_obj.name}**")

        data = _collect_month_data(db, cong_obj.id if cong_obj else 0, start, end, is_all)
        tot_dizimos = data['totals']['dizimos']
        tot_ofertas = data['totals']['ofertas']
        tot_missoes = data['totals']['missoes']
        tot_geral_sem_missoes = data['totals']['entradas_total_sem_missoes']
        saldo = data['totals']['saldo']

        c1,c2,c3 = st.columns(3)
        c1.metric("Total de Dízimos", format_currency(tot_dizimos))
        c2.metric("Total de Ofertas", format_currency(tot_ofertas))
        c3.metric("Total de Missões (entrada)", format_currency(tot_missoes))
        c4,c5 = st.columns(2)
        c4.metric("Total geral (D+O + Outras)", format_currency(tot_geral_sem_missoes))
        c5.metric("Saldo", format_currency(saldo))

        st.divider()

        if not is_all:
            st.subheader("Resumo por data (Dízimo e Oferta)")
            summary_by_date = defaultdict(lambda: {"dizimo": 0.0, "oferta": 0.0})
            for t in data["tx_in"]:
                if t.category and _norm(t.category.name) in ("dizimo","dízimo"):
                    summary_by_date[t.date]["dizimo"] += float(t.amount)
                elif t.category and _norm(t.category.name) == "oferta":
                    summary_by_date[t.date]["oferta"] += float(t.amount)
            for t in data["tithes"]:
                summary_by_date[t.date]["dizimo"] += float(t.amount)

            rows = []
            for d, totals in sorted(summary_by_date.items()):
                total = totals["dizimo"] + totals["oferta"]
                rows.append({
                    "Data do Culto": format_date(d),
                    "Dízimo": format_currency(totals["dizimo"]),
                    "Oferta": format_currency(totals["oferta"]),
                    "Total": format_currency(total)
                })
            df_summary = pd.DataFrame(rows)
            if not df_summary.empty:
                st.dataframe(df_summary, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem dízimos e ofertas para o período.")

        st.divider()
        st.subheader("Dizimistas no período")
        if is_all:
            all_tz = db.scalars(select(Tithe).where(Tithe.date >= start, Tithe.date < end)).all()
            by_cong = defaultdict(set)
            for t in all_tz:
                by_cong[t.congregation.name].add(_norm(t.tither_name))
            df = pd.DataFrame([{"Congregação": k, "Qtde de dizimistas": len(v)} for k,v in sorted(by_cong.items())])
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem dízimos.")
        else:
            names = {_norm(t.tither_name) for t in data["tithes"]}
            st.metric("Qtde de dizimistas (mês)", len(names))

        st.divider()
        st.subheader("Missões no período (Entradas)")
        if is_all:
            agg = defaultdict(float)
            for t in data["tx_in"]:
                if t.category and _norm(t.category.name) in ("missoes","missões"):
                    agg[t.congregation.name] += float(t.amount)
            dfm = pd.DataFrame([{"Congregação": k, "Entradas Missões": format_currency(v)} for k,v in sorted(agg.items())])
            if not dfm.empty:
                st.dataframe(dfm, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem entradas de Missões.")
        else:
            val = sum(float(t.amount) for t in data["tx_in"] if t.category and _norm(t.category.name) in ("missoes","missões"))
            st.metric("Entradas Missões", format_currency(val))

        st.divider()
        rows_csv = [{
            "Data": t.date.strftime("%Y-%m-%d"),
            "Congregação": t.congregation.name,
            "Categoria": t.category.name,
            "Valor": float(t.amount),
            "Descrição": t.description or ""
        } for t in data["tx_in"]]
        csv = pd.DataFrame(rows_csv).to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Baixar CSV das ENTRADAS do período", data=csv, file_name=f"entradas_{start.strftime('%Y-%m')}.csv", mime="text/csv")

def page_relatorio_saida(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

        st.markdown("<h1 class='page-title'>Relatório de Saída</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        congs = cong_options_for(user, db)
        if user.role == "SEDE":
            ordered = order_congs_sede_first(congs)
            esc_opt = ["Todas as congregações"] + [c.name for c in ordered]
            esc = st.selectbox("Escopo", esc_opt, key="rs_escopo")
            is_all = (esc == "Todas as congregações")
            cong_obj = None if is_all else next(c for c in ordered if c.name == esc)
        else:
            cong_obj = congs[0] if congs else None; is_all = False

        if not cong_obj and not is_all:
            st.info("Sem congregação vinculada."); return
        if cong_obj:
            st.info(f"Escopo: **{cong_obj.name}**")

        q = select(Transaction).options(joinedload(Transaction.category)).where(
            Transaction.date >= start, Transaction.date < end, Transaction.type.in_((TYPE_OUT, "DESPESA"))
        )
        if not is_all:
            q = q.where(Transaction.congregation_id == cong_obj.id)
        txs = db.scalars(q).all()

        total_saidas = sum(float(t.amount) for t in txs)
        st.metric("Total de saídas", format_currency(total_saidas))

        if is_all:
            agg = defaultdict(float)
            for t in txs:
                agg[t.congregation.name] += float(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Total de saídas": format_currency(v)} for k,v in sorted(agg.items())])
            st.subheader("Resumo por congregação")
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem saídas.")
        else:
            resumo = defaultdict(lambda: {"Quantidade":0, "Valor":0.0})
            for t in txs:
                if t.category:
                    resumo[t.category.name]["Quantidade"] += 1
                    resumo[t.category.name]["Valor"] += float(t.amount)
            df_res = pd.DataFrame([
                {"Tipo da saída": k, "Quantidade": v["Quantidade"], "Valor (R$)": v["Valor"]}
                for k,v in sorted(resumo.items(), key=lambda x: x[0].lower())
            ])
            if not df_res.empty:
                df_res["Valor (R$)"] = df_res["Valor (R$)"].map(format_currency)
                st.subheader("Resumo por tipo de saída")
                st.dataframe(df_res, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem saídas no período.")

        rows = [{
            "ID": t.id, "Data": format_date(t.date),
            "Tipo da saída": t.category.name, "Valor (R$)": float(t.amount),
            "Descrição": t.description or ""
        } for t in txs]
        df_list = pd.DataFrame(rows)
        if not df_list.empty:
            df_list["Valor (R$)"] = df_list["Valor (R$)"].map(format_currency)
            st.subheader("Lançamentos de saída")
            st.dataframe(df_list, use_container_width=True, hide_index=True, height=200)
        else:
            st.caption("Sem lançamentos para listar.")

        st.divider()
        rows_csv = [{
            "Data": t.date.strftime("%Y-%m-%d"),
            "Congregação": t.congregation.name,
            "Tipo da saída": t.category.name,
            "Valor": float(t.amount),
            "Descrição": t.description or ""
        } for t in txs]
        csv = pd.DataFrame(rows_csv).to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Baixar CSV das SAÍDAS do período", data=csv, file_name=f"saidas_{start.strftime('%Y-%m')}.csv", mime="text/csv")

def page_relatorio_dizimistas(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

        st.markdown("<h1 class='page-title'>Relatório de Dizimistas</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        congs = cong_options_for(user, db)
        if user.role == "SEDE":
            ordered = order_congs_sede_first(congs)
            esc_opt = ["Todas as congregações"] + [c.name for c in ordered]
            esc = st.selectbox("Escopo", esc_opt, key="rd_escopo")
            is_all = (esc == "Todas as congregações")
            cong_obj = None if is_all else next(c for c in ordered if c.name == esc)
        else:
            cong_obj = congs[0] if congs else None; is_all = False

        if not cong_obj and not is_all:
            st.info("Sem congregação vinculada."); return
        if cong_obj:
            st.info(f"Escopo: **{cong_obj.name}**")

        if is_all:
            all_tz = db.scalars(select(Tithe).where(Tithe.date >= start, Tithe.date < end)).all()
            by_cong = defaultdict(lambda: {"qtd":0, "valor":0.0})
            for t in all_tz:
                k = t.congregation.name
                by_cong[k]["qtd"] += 1
                by_cong[k]["valor"] += float(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Qtde de dizimistas": v["qtd"], "Total (R$)": format_currency(v["valor"])} for k,v in sorted(by_cong.items())])
            st.dataframe(df, use_container_width=True, hide_index=True, height=200)
            st.info("Selecione uma congregação específica para ver a lista nominal.")
        else:
            tithes = db.scalars(select(Tithe).where(
                Tithe.date >= start, Tithe.date < end, Tithe.congregation_id == cong_obj.id
            ).order_by(Tithe.date)).all()

            tithes_by_payment = defaultdict(lambda: {"count": 0, "total": 0.0})
            for tithe in tithes:
                method = tithe.payment_method or "Não Informado"
                tithes_by_payment[method]["count"] += 1
                tithes_by_payment[method]["total"] += float(tithe.amount)

            st.subheader("Resumo de Pagamentos de Dízimos")
            cols_metrics = st.columns(max(1, len(tithes_by_payment)))
            for i, (method, data) in enumerate(tithes_by_payment.items()):
                cols_metrics[i].metric(f"Total ({method})", format_currency(data["total"]), f"{data['count']} dízimos")

            st.divider()
            df = pd.DataFrame([{
                "Data": format_date(t.date),
                "Dizimista": t.tither_name,
                "Valor (R$)": format_currency(float(t.amount)),
                "Forma de Pagamento": t.payment_method or "Não Informado"
            } for t in tithes])
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True, height=200)
                st.metric("Total de dízimos", format_currency(sum(float(t.amount) for t in tithes)))
            else:
                st.caption("Sem dízimos neste período.")

        st.divider()
        st.subheader("Pesquisa de Dizimistas (por Ano)")
        c1, c2, c3, c4 = st.columns([1.2, 1.8, 1.4, 2.6])
        with c1:
            ano_pesq = st.number_input("Ano", value=date.today().year, step=1, format="%d", key="srch_year")
        with c2:
            if user.role == "SEDE":
                cong_opts = ["Todas"] + [c.name for c in order_congs_sede_first(congs)]
                cong_sel = st.selectbox("Congregação", cong_opts, key="srch_cong")
            else:
                cong_sel = cong_obj.name
                st.text_input("Congregação", cong_sel, disabled=True, key="srch_cong_disabled")
        with c3:
            mes_opt = ["Todos"] + MONTHS
            mes_sel = st.selectbox("Mês", mes_opt, index=0, key="srch_month")
        with c4:
            nome_q = st.text_input("Nome do dizimista (contém)", key="srch_name")

        year_start = date(int(ano_pesq), 1, 1)
        year_end = date(int(ano_pesq)+1, 1, 1)
        q = select(Tithe).options(joinedload(Tithe.congregation)).where(Tithe.date >= year_start, Tithe.date < year_end)

        if user.role == "SEDE":
            if cong_sel != "Todas":
                cong_id_sel = next(c.id for c in congs if c.name == cong_sel)
                q = q.where(Tithe.congregation_id == cong_id_sel)
        else:
            q = q.where(Tithe.congregation_id == cong_obj.id)

        if mes_sel != "Todos":
            m = MONTHS.index(mes_sel) + 1
            m_start = date(int(ano_pesq), m, 1)
            m_end = date(ano_pesq + (m==12), (m % 12) + 1, 1)
            q = q.where(Tithe.date >= m_start, Tithe.date < m_end)

        with SessionLocal() as db2:
            t_list = db2.scalars(q.order_by(Tithe.tither_name, Tithe.date)).all()

        if nome_q.strip():
            nneedle = _norm(nome_q)
            t_list = [t for t in t_list if nneedle in _norm(t.tither_name)]

        agg = {}
        for t in t_list:
            key = (_norm(t.tither_name), t.congregation_id)
            if key not in agg:
                agg[key] = {
                    "nome_display": t.tither_name,
                    "congregacao": t.congregation.name if t.congregation else "—",
                    "total_ano": 0.0,
                    "meses": set(),
                    "primeiro": t.date,
                    "ultimo": t.date,
                }
            agg[key]["total_ano"] += float(t.amount)
            agg[key]["meses"].add(t.date.month)
            if t.date < agg[key]["primeiro"]:
                agg[key]["primeiro"] = t.date
            if t.date > agg[key]["ultimo"]:
                agg[key]["ultimo"] = t.date

        rows = []
        for info in agg.values():
            meses_sorted = sorted(list(info["meses"]))
            rows.append({
                "Dizimista": info["nome_display"],
                "Congregação": info["congregacao"],
                "Qtde de meses no ano": len(meses_sorted),
                "Meses": ", ".join(MONTHS_SHORT[m-1] for m in meses_sorted) if meses_sorted else "—",
                "Total no ano (R$)": info["total_ano"],
                "Primeiro dízimo": format_date(info["primeiro"]) if info["primeiro"] else "—",
                "Último dízimo": format_date(info["ultimo"]) if info["ultimo"] else "—",
            })

        df_pesq = pd.DataFrame(rows)
        if not df_pesq.empty:
            df_pesq = df_pesq.sort_values(["Qtde de meses no ano","Dizimista"], ascending=[False, True])
            gb = GridOptionsBuilder.from_dataframe(df_pesq)
            gb.configure_grid_options(domLayout='normal')
            gb.configure_column("Dizimista", filter=True, floatingFilter=True)
            gb.configure_column("Congregação", filter=True, floatingFilter=True)
            gridOptions = gb.build()
            AgGrid(df_pesq, gridOptions=gridOptions, allow_unsafe_jscode=True, enable_enterprise_modules=True, height=300)
            tot_reg = len(df_pesq)
            tot_val = float(df_pesq["Total no ano (R$)"].sum())
            cA, cB = st.columns(2)
            cA.metric("Dizimistas encontrados", f"{tot_reg}")
            cB.metric("Total geral da pesquisa", format_currency(tot_val))
            csv = df_pesq.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Baixar CSV da pesquisa", data=csv, file_name=f"pesquisa_dizimistas_{ano_pesq}.csv", mime="text/csv")
            pdf_data = build_dizimista_search_pdf(df_pesq.assign(**{"Total no ano (R$)": df_pesq["Total no ano (R$)"]}), ano_pesq, cong_sel, mes_sel, nome_q)
            st.download_button("⬇️ Baixar PDF da pesquisa", data=pdf_data, file_name=f"pesquisa_dizimistas_{ano_pesq}.pdf", mime="application/pdf")
        else:
            st.caption("Nenhum resultado para os filtros informados.")

# ===== Missões =====
def _collect_missions_data(db: Session, start: date, end: date):
    q_in = select(Transaction).options(joinedload(Transaction.congregation)).where(
        Transaction.date >= start, Transaction.date < end, Transaction.type == TYPE_IN,
        Transaction.category.has(Category.name.in_(("Missões","missões")))
    ).order_by(Transaction.date)
    entradas_missoes = db.scalars(q_in).all()

    q_out = select(Transaction).options(joinedload(Transaction.congregation)).where(
        Transaction.date >= start, Transaction.date < end, Transaction.type == TYPE_OUT,
        Transaction.category.has(Category.name.in_(("Missões (Saída)","missões (saída)")))
    ).order_by(Transaction.date)
    saidas_missoes = db.scalars(q_out).all()
    return entradas_missoes, saidas_missoes

def build_missions_report_pdf(ref: date, entradas: list, saidas: list) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['Heading2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    table_style = TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
    ])
    story: List = []
    story.append(Paragraph("Relatório Mensal de Missões", title_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("Resumo de Entradas de Missões por Congregação", heading_style))
    if entradas:
        entradas_cong_sum = defaultdict(float)
        for t in entradas:
            entradas_cong_sum[t.congregation.name] += float(t.amount)
        entradas_data = [["Congregação", "Entradas (R$)"]]
        for cong_name, total in sorted(entradas_cong_sum.items()):
            entradas_data.append([cong_name, format_currency(total)])
        tbl_in = Table(entradas_data, colWidths=[9*cm, 9*cm]); tbl_in.setStyle(table_style)
        story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada de missões registrada.", styles['Normal']))
    story.append(Spacer(1, 0.8*cm))

    story.append(Paragraph("Saídas de Missões", heading_style))
    if saidas:
        saidas_data = [["Data", "Descrição", "Valor (R$)"]]
        for t in saidas:
            saidas_data.append([t.date.strftime("%d/%m/%Y"), t.description or "—", format_currency(float(t.amount))])
        tbl_out = Table(saidas_data, colWidths=[3*cm, 10*cm, 5*cm]); tbl_out.setStyle(table_style)
        story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída de missões registrada.", styles['Normal']))
    story.append(Spacer(1, 1*cm))

    total_entradas_missions = sum(float(t.amount) for t in entradas)
    total_saidas_missions = sum(float(t.amount) for t in saidas)
    saldo_missions = total_entradas_missions - total_saidas_missions
    story.append(Paragraph("Resumo Financeiro de Missões", heading_style))
    summary_data = [
        ["Total de Entradas de Missões", format_currency(total_entradas_missions)],
        ["Total de Saídas de Missões", format_currency(total_saidas_missions)],
        ["Saldo de Missões no Mês", format_currency(saldo_missions)],
    ]
    summary_table = Table(summary_data, colWidths=[8*cm, 8*cm])
    summary_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#eef2ff")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(summary_table)
    doc.build(story)
    return buf.getvalue()

def page_relatorio_missoes(user: "User"):
    if user.role not in ["SEDE", "TESOUREIRO MISSIONÁRIO"]:
        st.warning("🔒 Acesso negado. Apenas `SEDE` ou `TESOUREIRO MISSIONÁRIO`."); return
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

        st.markdown("<h1 class='page-title'>Relatório de Missões</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)
        st.info("Escopo: **Todas as congregações**")

        entradas_missoes, saidas_missoes = _collect_missions_data(db, start, end)
        total_entradas = sum(float(t.amount) for t in entradas_missoes)
        total_saidas = sum(float(t.amount) for t in saidas_missoes)
        saldo_missoes = total_entradas - total_saidas

        c1, c2, c3 = st.columns(3)
        c1.metric("Total de Entradas", format_currency(total_entradas))
        c2.metric("Total de Saídas", format_currency(total_saidas))
        c3.metric("Saldo do Mês", format_currency(saldo_missoes))
        st.divider()

        st.subheader("Lançar Entrada de Missões")
        with st.form("form_entrada_missoes", clear_on_submit=True):
            congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()
            cong_names = [c.name for c in order_congs_sede_first(congs_all)]
            cong_sel = st.selectbox("Selecione a congregação", cong_names, key="mis_ent_cong_sel")
            ent_data = st.date_input("Data do Culto", value=date.today(), key="mis_ent_data")
            ent_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="mis_ent_valor")
            ent_desc = st.text_input("Descrição (opcional)", key="mis_ent_desc")
            if st.form_submit_button("Salvar ENTRADA de Missões", type="primary"):
                with SessionLocal() as _db:
                    cong_obj = next(c for c in congs_all if c.name == cong_sel)
                    cat_obj = _db.scalar(select(Category).where(Category.name == "Missões"))
                    if not cat_obj:
                        st.error("Categoria 'Missões' não encontrada."); return
                    _db.add(Transaction(date=ent_data, type=TYPE_IN, category_id=cat_obj.id, amount=ent_valor,
                                        description=(ent_desc or None), congregation_id=cong_obj.id, payment_method=None))
                    _db.commit(); st.success(f"Entrada de missões para '{cong_sel}' registrada."); st.rerun()
        st.divider()

        st.subheader("Entradas de Missões por Congregação")
        if entradas_missoes:
            agg_entradas = defaultdict(float)
            for t in entradas_missoes:
                agg_entradas[t.congregation.name] += float(t.amount)
            df_entradas = pd.DataFrame([{"Congregação": k, "Total no Mês (R$)": v} for k,v in sorted(agg_entradas.items())])
            df_entradas["Total no Mês (R$)"] = df_entradas["Total no Mês (R$)"].map(format_currency)
            st.dataframe(df_entradas, use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhuma entrada de missões registrada neste período.")
        st.divider()

        st.subheader("Lançar Saída de Missões")
        with st.form("form_saida_missoes", clear_on_submit=True):
            sai_data = st.date_input("Data", value=date.today())
            sai_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f")
            sai_desc = st.text_input("Descrição")
            if st.form_submit_button("Salvar SAÍDA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name=="Missões (Saída)"))
                    if not cat_obj:
                        st.error("Categoria 'Missões (Saída)' não encontrada."); return
                    sede_cong = _db.scalar(select(Congregation).where(Congregation.name=="Sede"))
                    if not sede_cong:
                        st.error("Congregação 'Sede' não encontrada."); return
                    _db.add(Transaction(date=sai_data, type=TYPE_OUT, category_id=cat_obj.id, amount=sai_valor,
                                        description=(sai_desc or None), congregation_id=sede_cong.id))
                    _db.commit(); st.success("Saída de missões registrada."); st.rerun()
        st.divider()

        st.subheader("Saídas de Missões no Período")
        if saidas_missoes:
            df_saidas = pd.DataFrame([{"Data": format_date(t.date), "Descrição": t.description or "", "Valor (R$)": format_currency(float(t.amount))} for t in saidas_missoes])
            st.dataframe(df_saidas, use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhuma saída de missões registrada neste período.")
        st.divider()

        st.subheader("Gerar Relatório em PDF")
        st.download_button("⬇️ Baixar Relatório de Missões (PDF)",
            data=build_missions_report_pdf(ref, entradas_missoes, saidas_missoes),
            file_name=f"relatorio_missoes_{start.strftime('%Y-%m')}.pdf", mime="application/pdf")

def page_cadastro(user: "User"):
    if not is_admin_general(user):
        st.warning("🔒 Apenas o **administrador geral** (admin) pode acessar o Cadastro."); return
    with SessionLocal() as db:
        with st.sidebar:
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

        st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)

        st.subheader("Congregações")
        new_cong = st.text_input("Nova congregação", key="cad_new_cong")
        if st.button("Adicionar congregação", disabled=not new_cong.strip(), key="cad_add_cong"):
            if db.scalar(select(Congregation).where(Congregation.name == new_cong.strip())):
                st.error("Já existe congregação com esse nome.")
            else:
                db.add(Congregation(name=new_cong.strip())); db.commit()
                st.success("Congregação adicionada."); st.rerun()

        congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        users_by_cong = dict(db.execute(select(Congregation.id, func.count(User.id)).join(User, User.congregation_id==Congregation.id, isouter=True).group_by(Congregation.id)).all())
        tx_by_cong = dict(db.execute(select(Congregation.id, func.count(Transaction.id)).join(Transaction, Transaction.congregation_id==Congregation.id, isouter=True).group_by(Congregation.id)).all())
        tithes_by_cong = dict(db.execute(select(Congregation.id, func.count(Tithe.id)).join(Tithe, Tithe.congregation_id==Congregation.id, isouter=True).group_by(Congregation.id)).all())
        dfc = pd.DataFrame([{"ID": c.id, "Nome": c.name, "Usuários": int(users_by_cong.get(c.id,0)), "Lançamentos": int(tx_by_cong.get(c.id,0)), "Dízimos": int(tithes_by_cong.get(c.id,0))} for c in congs_all])
        if not dfc.empty: st.dataframe(dfc, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir congregações"):
            st.caption("Só é possível excluir congregações **sem usuários, lançamentos ou dízimos**. A congregação **Sede** não pode ser excluída.")
            eligible_ids = []
            for c in congs_all:
                if _norm(c.name) == "sede": continue
                if users_by_cong.get(c.id,0)==0 and tx_by_cong.get(c.id,0)==0 and tithes_by_cong.get(c.id,0)==0:
                    eligible_ids.append(c.id)
            if not eligible_ids:
                st.info("Nenhuma congregação elegível para exclusão.")
            else:
                ids_del_cong = st.multiselect("IDs de congregações para excluir", eligible_ids, key="cad_del_cong_ids")
                confc2 = st.text_input("Digite EXCLUIR para confirmar", key="cad_del_cong_conf")
                btn_disabled = (not ids_del_cong) or (not _confirm_ok(confc2))
                if st.button("Excluir congregações selecionadas", disabled=btn_disabled, key="cad_del_cong_btn"):
                    with SessionLocal() as _db:
                        _db.query(Congregation).filter(Congregation.id.in_(ids_del_cong)).delete(synchronize_session=False)
                        _db.commit()
                    st.success(f"{len(ids_del_cong)} congregação(ões) excluída(s)."); st.rerun()
        st.divider()

        st.subheader("Categorias")
        col1, col2 = st.columns(2)
        with col1: cat_name = st.text_input("Nome da categoria", key="cad_cat_name")
        with col2: cat_type = st.selectbox("Tipo", [TYPE_IN, TYPE_OUT], key="cad_cat_type")
        if st.button("Adicionar categoria", disabled=not cat_name.strip(), key="cad_add_cat"):
            if db.scalar(select(Category).where(Category.name==cat_name.strip())):
                st.error("Já existe categoria com esse nome.")
            else:
                db.add(Category(name=cat_name.strip(), type=cat_type)); db.commit()
                st.success("Categoria adicionada."); st.rerun()

        cats = db.scalars(select(Category).order_by(Category.type, Category.name)).all()
        usage = dict(db.execute(select(Category.id, func.count(Transaction.id)).join(Transaction, Transaction.category_id==Category.id, isouter=True).group_by(Category.id)).all())
        dfcat = pd.DataFrame([{"ID": c.id, "Nome": c.name, "Tipo": c.type, "Usos em lançamentos": int(usage.get(c.id,0))} for c in cats])
        if not dfcat.empty: st.dataframe(dfcat, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir categorias"):
            st.caption("Só é possível excluir categorias **sem lançamentos** vinculados.")
            ids_del = st.multiselect("IDs de categorias para excluir", dfcat.loc[dfcat["Usos em lançamentos"]==0,"ID"].tolist(), key="cad_del_cat_ids")
            confc = st.text_input("Digite EXCLUIR para confirmar", key="cad_del_cat_conf")
            btn_disabled = (not ids_del) or (not _confirm_ok(confc))
            if st.button("Excluir categorias selecionadas", disabled=btn_disabled, key="cad_del_cat_btn"):
                with SessionLocal() as _db:
                    _db.query(Category).filter(Category.id.in_(ids_del)).delete(synchronize_session=False)
                    _db.commit()
                st.success(f"{len(ids_del)} categoria(s) excluída(s)."); st.rerun()
        st.divider()

        st.subheader("Usuários")
        u_user = st.text_input("Usuário (login)", key="cad_user_login")
        u_pwd = st.text_input("Senha", type="password", key="cad_user_pwd")
        u_role = st.selectbox("Perfil", ["SEDE","TESOUREIRO","TESOUREIRO MISSIONÁRIO"], key="cad_user_role")
        all_congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        cong_options = ["—"] + [c.name for c in all_congs]
        cong_sel_key = "cad_user_cong"
        if u_role == "TESOUREIRO MISSIONÁRIO":
            try: index = cong_options.index("Sede")
            except ValueError: index = 0
            u_cong_name = st.selectbox("Congregação (vinculada a Saídas de Missões)", cong_options, index=index, key=cong_sel_key)
        else:
            u_cong_name = st.selectbox("Congregação", cong_options, key=cong_sel_key)

        if st.button("Criar usuário", key="cad_user_add"):
            if not u_user.strip() or not u_pwd.strip():
                st.error("Usuário e senha são obrigatórios.")
            elif db.scalar(select(User).where(User.username==u_user.strip())):
                st.error("Usuário já existe.")
            else:
                cong_id = None
                if u_role == "TESOUREIRO":
                    if u_cong_name == "—":
                        st.error("Selecione a congregação."); return
                    cong_id = next(c.id for c in all_congs if c.name == u_cong_name)
                elif u_role == "TESOUREIRO MISSIONÁRIO":
                    cong_id = db.scalar(select(Congregation.id).where(Congregation.name=="Sede"))
                db.add(User(username=u_user.strip(), password_hash=hash_password(u_pwd.strip()), role=u_role, congregation_id=cong_id))
                db.commit(); st.success("Usuário criado."); st.rerun()

        users = db.scalars(select(User).order_by(User.username)).all()
        dfu = pd.DataFrame([{"ID": u.id, "Usuário": u.username, "Perfil": u.role, "Congregação": (db.get(Congregation, u.congregation_id).name if u.congregation_id else "—")} for u in users])
        if not dfu.empty: st.dataframe(dfu, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir usuários"):
            st.caption("Não é permitido excluir o usuário atualmente logado.")
            ids_u = st.multiselect("IDs de usuários para excluir", dfu["ID"].tolist(), key="cad_del_users_ids")
            ids_u = [i for i in ids_u if i != user.id]
            confu = st.text_input("Digite EXCLUIR para confirmar", key="cad_del_users_conf")
            btn_disabled = (not ids_u) or (not _confirm_ok(confu))
            if st.button("Excluir usuários selecionados", disabled=btn_disabled, key="cad_del_users_btn"):
                with SessionLocal() as _db:
                    _db.query(User).filter(User.id.in_(ids_u)).delete(synchronize_session=False)
                    _db.commit()
                st.success(f"{len(ids_u)} usuário(s) excluído(s)."); st.rerun()

def page_visao_geral(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

        st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        congs = cong_options_for(user, db)
        ordered = order_congs_sede_first(congs)

        is_all = (user.role == "SEDE")
        if is_all:
            st.info("Escopo: **Todas as congregações**")
        elif congs:
            cong_obj = congs[0]; st.info(f"Escopo: **{cong_obj.name}**")
        else:
            st.info("Sem congregação vinculada."); return

        agg_total = []
        if is_all:
            for c in ordered:
                totals = _collect_month_data(db, c.id, start, end)["totals"]
                agg_total.append((c.name, totals["entradas_total_sem_missoes"], totals["saidas_total"], totals["saldo"], totals["missoes"]))
        elif congs:
            cong_obj = congs[0]
            totals = _collect_month_data(db, cong_obj.id, start, end)["totals"]
            agg_total.append((cong_obj.name, totals["entradas_total_sem_missoes"], totals["saidas_total"], totals["saldo"], totals["missoes"]))

        if user.role == "SEDE":
            df_rank = pd.DataFrame([{"Congregação": n, "Entradas (D+O + Outras)": v, "Saídas": s, "Saldo": sal, "Missões (entrada)": m} for (n, v, s, sal, m) in agg_total])
            if not df_rank.empty:
                df_sorted = df_rank.sort_values("Entradas (D+O + Outras)", ascending=False).reset_index(drop=True)
                top_n = min(5, len(df_sorted))
                cols = st.columns(top_n)
                for i in range(top_n):
                    row = df_sorted.iloc[i]
                    render_stat_card(cols[i], f"{i+1}º lugar", f"{row['Congregação']} — {format_currency(float(row['Entradas (D+O + Outras)']))}")
                st.divider()
                st.dataframe(
                    df_sorted.assign(**{
                        "Entradas (D+O + Outras)": df_sorted["Entradas (D+O + Outras)"].map(lambda x: format_currency(float(x))),
                        "Saídas": df_sorted["Saídas"].map(lambda x: format_currency(float(x))),
                        "Saldo": df_sorted["Saldo"].map(lambda x: format_currency(float(x))),
                        "Missões (entrada)": df_sorted["Missões (entrada)"].map(lambda x: format_currency(float(x)))
                    }),
                    use_container_width=True, hide_index=True, height=200
                )
            else:
                st.caption("Sem dados neste mês.")

        if user.role != "SEDE" and agg_total:
            st.divider(); st.subheader("Resumo Financeiro Mensal")
            df_summary_cong = pd.DataFrame([
                {"Métricas": "Entradas (D+O + Outras)", "Valor": format_currency(agg_total[0][1])},
                {"Métricas": "Saídas", "Valor": format_currency(agg_total[0][2])},
                {"Métricas": "Saldo", "Valor": format_currency(agg_total[0][3])},
                {"Métricas": "Missões (entrada)", "Valor": format_currency(agg_total[0][4])}
            ])
            st.dataframe(df_summary_cong, use_container_width=True, hide_index=True)

        if user.role == "SEDE":
            st.divider(); st.subheader("Relatório Consolidado Mensal")
            st.download_button(
                "⬇️ Baixar PDF do Relatório Geral",
                data=build_consolidated_pdf(agg_total, ref),
                file_name=f"relatorio_mensal_{start.strftime('%Y-%m')}.pdf",
                mime="application/pdf"
            )

        st.subheader("Prestação de contas (PDF completo)")
        if user.role == "SEDE":
            sel = st.selectbox("Congregação", [c.name for c in ordered], key="pc_cong_sel")
            cong_obj = next(c for c in ordered if c.name == sel)
        else:
            cong_obj = ordered[0]
        st.download_button(
            "⬇️ Baixar PDF do mês (completo)",
            data=build_full_statement_pdf(cong_obj.id, cong_obj.name, ref),
            file_name=f"prestacao_{_norm(cong_obj.name)}_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf"
        )

# ===================== MAIN =====================
def main():
    try:
        ensure_seed()
        hydrate_auth_from_query()
        user = current_user()
        if not user:
            login_ui(); return

        with st.sidebar:
            if user.role == "SEDE":
                menu_options = ["Lançamentos","Relatório de Entrada","Relatório de Saída","Relatório de Dizimistas","Relatório de Missões","Visão Geral","Cadastro"]
            elif user.role == "TESOUREIRO":
                menu_options = ["Lançamentos","Relatório de Entrada","Relatório de Saída","Relatório de Dizimistas","Visão Geral"]
            elif user.role == "TESOUREIRO MISSIONÁRIO":
                menu_options = ["Relatório de Missões"]
            else:
                menu_options = ["Visão Geral"]
            page = st.radio("Menu", options=menu_options, index=0, key="main_menu")

        if page == "Lançamentos":
            page_lancamentos(user)
        elif page == "Relatório de Entrada":
            page_relatorio_entrada(user)
        elif page == "Relatório de Saída":
            page_relatorio_saida(user)
        elif page == "Relatório de Dizimistas":
            page_relatorio_dizimistas(user)
        elif page == "Relatório de Missões":
            page_relatorio_missoes(user)
        elif page == "Visão Geral":
            page_visao_geral(user)
        elif page == "Cadastro":
            page_cadastro(user)

    except Exception as e:
        st.error("Ocorreu um erro ao renderizar a aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
