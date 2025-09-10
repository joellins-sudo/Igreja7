# main.py — Igreja Finance CHMS — v8.51 (Pesquisa de Dizimistas por nome/congregação/mês)
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Optional, List, Tuple
from collections import defaultdict, Counter
import locale as _locale
import pandas as pd
import streamlit as st

from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
from sqlalchemy.ext.declarative import declarative_base
import unicodedata as ud
import hashlib

# Biblioteca para layout da pesquisa
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# PDF
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER

# ===================== CONFIG: ADMIN =====================
ADMIN_USERNAME = "admin"  # somente este login verá/entrará no "Cadastro"

# ===================== ST CONFIG / THEME =====================
st.set_page_config(page_title="Igreja Finance CHMS", page_icon="⛪", layout="wide")

CSS = """
<style>
:root{ --brand:#0f172a; --brand-2:#1d4ed8; --accent:#16a34a; --danger:#dc2626;
 --muted:#6b7280; --bg:#f8fafc; --card:#ffffff; --ring:#e5e7eb; }
html, body { background: var(--bg); }
header[data-testid="stHeader"]{ background: linear-gradient(180deg,#ffffff, #f6f9ff) !important; border-bottom:1px solid var(--ring); }
.block-container{ padding-top: .9rem !important; }
[data-testid="stSidebar"]{ background: linear-gradient(180deg,#ffffff,#fbfdff); border-right:1px solid var(--ring); }
[data-testid="stSidebar"] img{ border-radius: .6rem; border:1px solid var(--ring); box-shadow: 0 4px 16px rgba(0,0,0,.06); }
.stRadio > div { gap: .5rem; }
.stRadio label{ padding:.32rem .56rem; border-radius:.55rem; font-weight:700; }
.stRadio [role="radio"][aria-checked="true"] label{ background:#eaf2ff; border:1px solid #c7dbff; color:#0f172a; }
.page-title{ font-family:'Nunito','Inter',system-ui; font-size:36px; font-weight:900; letter-spacing:.4px; color: var(--brand); margin:.25rem 0 1rem; }
h2, .stMarkdown h2, .st-subheader{ color:#0f172a!important; font-weight:900 !important; }
.st-container-card{ border: 1px solid var(--ring); border-radius: 1rem; padding: 1rem; margin-bottom: 1.15rem; background: var(--card);
 box-shadow: 0 10px 30px rgb(31 58 138 / .06), 0 2px 8px rgb(0 0 0 / .04); position: relative; }
.st-container-card::before{ content:""; position: absolute; left:0; top:0; bottom:0; width:6px; background: linear-gradient(180deg,var(--brand-2), var(--brand));
 border-top-left-radius:1rem; border-bottom-left-radius:1rem; }
div[data-testid="stMetric"]{ padding: .75rem .9rem; border:1px solid var(--ring); border-radius:.9rem; background:var(--card); box-shadow:0 1px 2px rgb(0 0 0 / .06); }
div[data-testid="stMetricLabel"]{ color:#334155; font-weight:800; }
div[data-testid="stMetricValue"]{ font-size:26px!important; line-height:1.15!important; white-space:nowrap!important; font-weight:900!important; }
.stat-card{ border:1px solid var(--ring); border-radius:.9rem; background:var(--card); padding:.9rem 1rem; box-shadow:0 1px 2px rgba(0,0,0,.06);
 transition: transform .15s ease, box-shadow .15s ease; position:relative; cursor:default; height:86px; display:flex; flex-direction:column; justify-content:center; }
.stat-card:hover{ transform: translateY(-2px); box-shadow: 0 12px 22px rgba(31,58,138,.12), 0 4px 8px rgba(0,0,0,.08); }
.stat-label{ font-size:.92rem; color:#334155; font-weight:800; margin-bottom:.18rem; }
.stat-value{ font-size:1.25rem; font-weight:900; color:#111827; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.stat-card .tooltip{ display:none; position:absolute; left:12px; top:calc(100% + 6px); background:#0f172a; color:#fff; font-weight:700; padding:.45rem .6rem;
 border-radius:.5rem; white-space:nowrap; z-index:100; box-shadow: 0 8px 24px rgba(0,0,0,.18); }
.stat-card:hover .tooltip{ display:block; }
label, .stTextInput label, .stSelectbox label, .stNumberInput label{ color:#0f172a; font-weight:800; }
.stTextInput input, .stNumberInput input, .stDateInput input{ border:1px solid var(--ring)!important; border-radius:.65rem!important; background:#fff!important; }
.stSelectbox [data-baseweb="select"]>div{ border:1px solid var(--ring)!important; border-radius:.65rem!important; background:#fff!important; }
.stTextInput input:focus, .stNumberInput input:focus, .stDateInput input:focus{ outline:none!important; border-color:#93c5fd!important; box-shadow:0 0 0 4px rgba(37,99,235,.18)!important; }
.stButton>button{ border:1px solid #2563eb!important; color:#fff!important; background: linear-gradient(180deg,#3b82f6,#2563eb)!important; font-weight:900!important;
 border-radius:.7rem!important; padding:.52rem .95rem!important; box-shadow:0 2px 6px rgba(37,99,235,.25)!important; }
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

# ===================== LOCALE (fallback) =====================
def _set_locale_ptbr():
    for loc in ("pt_BR.utf8", "pt_BR.UTF-8", "pt_BR", "Portuguese_Brazil.1252"):
        try:
            _locale.setlocale(_locale.LC_TIME, loc); return
        except Exception:
            continue
_set_locale_ptbr()

# ===================== UTILS =====================
MONTHS = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
MONTHS_SHORT = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

def format_currency(value: float) -> str:
    try: v = float(value or 0.0)
    except Exception: v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def format_date(d: date) -> str:
    return d.strftime("%d/%m/%Y")

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = ud.normalize("NFD", s)
    return "".join(c for c in s if ud.category(c) != "Mn").replace(" ", "")

def month_bounds(ref: date) -> Tuple[date, date]:
    start = ref.replace(day=1)
    end = date(start.year + (start.month == 12), (start.month % 12) + 1, 1)
    return start, end

def get_month_selector(label: str = "Mês de referência") -> date:
    today = date.today()
    colm, coly = st.columns([2, 1])
    with colm:
        m = st.selectbox(f"{label} — Mês", list(range(1, 13)), index=today.month-1, format_func=lambda i: MONTHS[i-1])
    with coly:
        y = st.number_input("Ano", value=today.year, step=1, format="%d")
    return date(int(y), int(m), 1)

# ===================== DB BASE & MODELS (AGORA PRIMEIRO!) =====================
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)  # 'SEDE' ou 'TESOUREIRO'
    congregation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("congregations.id"))
    congregation: Mapped[Optional["Congregation"]] = relationship(back_populates="users")

class Congregation(Base):
    __tablename__ = "congregations"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    users: Mapped[List["User"]] = relationship(back_populates="congregation")
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="congregation")
    tithes: Mapped[List["Tithe"]] = relationship(back_populates="congregation")

class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    type: Mapped[str] = mapped_column(String)  # 'DOAÇÃO' ou 'SAÍDA'
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="category")

class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    type: Mapped[str] = mapped_column(String)  # 'DOAÇÃO' ou 'SAÍDA'
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    amount: Mapped[float] = mapped_column(Float)
    description: Mapped[Optional[str]] = mapped_column(String)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)
    category: Mapped["Category"] = relationship(back_populates="transactions", lazy="joined")
    congregation: Mapped["Congregation"] = relationship(back_populates="transactions")

class Tithe(Base):
    __tablename__ = "tithes"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    tither_name: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)
    congregation: Mapped["Congregation"] = relationship(back_populates="tithes")

# ===================== ENGINE / SESSION (SÓ DEPOIS DOS MODELS) =====================
@st.cache_resource
def get_engine():
    db_url = st.secrets.get("DATABASE_URL", os.environ.get("DATABASE_URL"))
    if not db_url:
        db_url = "sqlite:///app.db"  # efêmero no Streamlit Cloud
    return create_engine(db_url, pool_pre_ping=True)

@st.cache_resource
def get_sessionmaker():
    engine = get_engine()
    Base.metadata.create_all(engine)  # garante criação após declarar os models
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

SessionLocal = get_sessionmaker()

# ===================== AUTH =====================
def hash_password(password: str) -> str:
    salt = os.urandom(16)
    pwdhash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return salt.hex() + ':' + pwdhash.hex()

def verify_password(password: str, stored_hash: str) -> bool:
    salt_hex, pwdhash_hex = stored_hash.split(':')
    salt = bytes.fromhex(salt_hex)
    pwdhash = bytes.fromhex(pwdhash_hex)
    new_hash = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return new_hash == pwdhash

# ===================== SEED =====================
TYPE_IN  = "DOAÇÃO"
TYPE_OUT = "SAÍDA"
LEGACY_TYPES = {"DOAÇÃO": ["RECEITA"], "SAÍDA": ["DESPESA"]}

CONGREGACOES_PADRAO = [
    "Sede", "Rodeadouro", "Dr. Humberto", "Jatobá", "Massaroca", "Riacho Seco",
    "Pedro Raimundo", "Lagoa do Salitre", "Lagoa da Areia", "Sítio Roçado",
    "Fazenda Bebedouro", "Junco", "Rua Vermelha", "Manga II",
    "Campos Casa", "Campos Terreno", "Alto Alencar", "Alto da Aliança",
    "Alto do Cruzeiro", "Amf Empreendimento",
    "Antônio Guilhermino I", "Antônio Guilhermino II", "Antônio Guilhermino III",
    "Abreus", "Argemiro", "Araras", "Baixo Salitre", "Bairro Vermelho",
    "Cacimba do Silva", "Campo dos Cavalos", "Campim de Raiz",
    "Carnaíba Carneiros", "Carnaíba Casa Pastoral", "Carnaíba Serra dos Espinhos",
    "Cipó Mandacaru", "Codevasf", "Fazenda Olaria", "Itaberaba",
    "Jardim Alvorada", "Jardim das Acácias", "Jardim Europa",
    "Jardim Primavera", "Jardim Vitória", "Jazida 7",
    "João Paulo II", "João Paulo II 2", "João Paulo II A",
    "João Paulo II Jp II Terreno Lado Templo", "João Paulo II Templo",
    "Juazeiro"
]

def ensure_seed():
    # cinturão de segurança: se por algum motivo chamarem ensure_seed cedo demais
    engine = get_engine()
    Base.metadata.create_all(engine)

    with SessionLocal() as db:
        # Categorias padrão
        if db.scalar(select(func.count(Category.id))) == 0:
            for nm, tp in [
                ("Dízimo", TYPE_IN), ("Oferta", TYPE_IN), ("Missões", TYPE_IN),
                ("Aluguel", TYPE_OUT), ("Energia", TYPE_OUT), ("Assistência Social", TYPE_OUT),
                ("Produtos de Limpeza", TYPE_OUT), ("Transporte", TYPE_OUT), ("Material de Expediente", TYPE_OUT),
            ]:
                if not db.scalar(select(Category).where(Category.name == nm)):
                    db.add(Category(name=nm, type=tp))
        # Congregações padrão (idempotente)
        existentes = set(db.scalars(select(Congregation.name)).all())
        faltantes = [n for n in CONGREGACOES_PADRAO if n not in existentes]
        if faltantes:
            db.add_all(Congregation(name=n) for n in faltantes)
        # Admin + Sede
        sede_cong = db.scalar(select(Congregation).where(Congregation.name == "Sede"))
        if sede_cong is None:
            sede_cong = Congregation(name="Sede")
            db.add(sede_cong); db.flush()
        if db.scalar(select(User).where(User.username == ADMIN_USERNAME)) is None:
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password("123456"),
                        role="SEDE", congregation_id=sede_cong.id))
        db.commit()

# ===================== SESSION / LOGIN =====================
if "uid" not in st.session_state:
    st.session_state.uid = None

def current_user() -> Optional["User"]:
    uid = st.session_state.get("uid")
    if not uid:
        return None
    with SessionLocal() as db:
        return db.get(User, uid)

def login_ui():
    col_logo, col_title = st.columns([1, 3])
    if os.path.exists(LOGO_PATH := os.path.join(os.path.dirname(__file__), "assets", "logo.png")):
        with col_logo: st.image(LOGO_PATH, use_container_width=True)
    with col_title:
        st.markdown("<h1 class='page-title'>Igreja Finance CHMS</h1>", unsafe_allow_html=True)

    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username == u))
            if user and verify_password(p, user.password_hash):
                st.session_state.uid = user.id; st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

# ===================== HELPERS =====================
def is_admin_general(user: "User") -> bool:
    return (user.username or "").strip().lower() == ADMIN_USERNAME.lower()

def categories_for_type(db: Session, kind: str) -> List[Category]:
    kinds = [kind] + LEGACY_TYPES.get(kind, [])
    return db.scalars(select(Category).where(Category.type.in_(kinds)).order_by(Category.name)).all()

def cong_options_for(user: "User", db: Session) -> List[Congregation]:
    if user.role == "SEDE":
        return db.scalars(select(Congregation).order_by(Congregation.name)).all()
    else:
        if user.congregation_id:
            c = db.get(Congregation, user.congregation_id)
            return [c] if c else []
        return []

def order_congs_sede_first(congs: List[Congregation]) -> List[Congregation]:
    sede = [c for c in congs if _norm(c.name) == "sede"]
    others = sorted([c for c in congs if _norm(c.name) != "sede"], key=lambda x: _norm(x.name))
    return (sede + others) if sede else others

# ===================== (Páginas) — o restante do seu código original =====================
# As páginas abaixo permanecem iguais ao arquivo que você me mandou.
# Para caber aqui e focar no conserto, não repito tudo; copie do seu arquivo anterior
# a partir daqui SEM alterar. Se preferir, eu mando o arquivo completo novamente.
# ----
# MAS se você quiser tudo num arquivo só, me avise que eu colo 100% aqui de novo.
# ----
# Dica: como só arrumamos a ORDEM de definição (models -> engine -> create_all),
# seu app volta a funcionar sem o erro "no such table".

# ============ COLE AQUI O RESTO DAS SUAS FUNÇÕES DE PÁGINA E PDF SEM MUDANÇAS ============
# (page_lancamentos, _collect_month_data, page_relatorio_entrada, page_relatorio_saida,
#  page_relatorio_dizimistas, build_full_statement_pdf, build_consolidated_pdf, render_stat_card,
#  page_visao_geral, page_cadastro)

# ===================== MAIN =====================
def main():
    try:
        ensure_seed()
        user = current_user()
        if not user:
            login_ui(); return

        with SessionLocal() as db:
            pass  # apenas para “esquentar” a conexão

        with st.sidebar:
            base_menu = ["Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Dizimistas", "Visão Geral"]
            menu = base_menu + (["Cadastro"] if is_admin_general(user) else [])
            page = st.radio("Menu", options=menu, index=0)

        # Chame aqui as suas funções de página como já estava no seu código:
        if page == "Lançamentos": 
            page_lancamentos(user)
        elif page == "Relatório de Entrada": 
            page_relatorio_entrada(user)
        elif page == "Relatório de Saída": 
            page_relatorio_saida(user)
        elif page == "Relatório de Dizimistas": 
            page_relatorio_dizimistas(user)
        elif page == "Visão Geral": 
            page_visao_geral(user)
        elif page == "Cadastro": 
            page_cadastro(user)

    except Exception as e:
        st.error("Ocorreu um erro ao renderizar a aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
