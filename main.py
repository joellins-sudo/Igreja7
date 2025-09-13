# main.py — AD Relatório Financeiro — v10.0 (com login persistente + timeout, edição intuitiva, precisão Decimal)
from __future__ import annotations

import os
from datetime import date, timedelta, datetime
from typing import Optional, List, Tuple
from collections import defaultdict
import locale as _locale
import pandas as pd
import streamlit as st

from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
from sqlalchemy.ext.declarative import declarative_base
import unicodedata as ud
import hashlib
import json, base64, hmac, time

# Biblioteca para layout da pesquisa / edição em tabela
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# PDF
from io import BytesIO
from reportlab.lib.pagesizes import A4, portrait
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER

# Precisão financeira e fuso Bahia
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

# ===================== CONFIG: ADMIN =====================
ADMIN_USERNAME = "admin"  # somente este login verá/entrará no "Cadastro"

# Timeout de inatividade (minutos)
INACTIVITY_MINUTES = int(
    st.secrets.get("INACTIVITY_MINUTES", os.environ.get("INACTIVITY_MINUTES", 20))
)

# ===================== ST CONFIG / THEME =====================
st.set_page_config(page_title="AD Relatório Financeiro", page_icon="⛪", layout="wide")

CSS = """
<style>
:root{
  --brand:#0f172a; --brand-2:#1d4ed8; --accent:#16a34a; --danger:#dc2626; --muted:#6b7280;
  --bg:#f8fafc; --card:#ffffff; --ring:#e5e7eb;
}
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
.st-container-card{ border: 1px solid var(--ring); border-radius: 1rem; padding: 1rem; margin-bottom: 1.15rem; background: var(--card); box-shadow: 0 10px 30px rgb(31 58 138 / .06), 0 2px 8px rgb(0 0 0 / .04); position: relative; }
.st-container-card::before{ content:""; position: absolute; left:0; top:0; bottom:0; width:6px; background: linear-gradient(180deg,var(--brand-2), var(--brand)); border-top-left-radius:1rem; border-bottom-left-radius:1rem; }
div[data-testid="stMetric"]{ padding: .75rem .9rem; border:1px solid var(--ring); border-radius:.9rem; background:var(--card); box-shadow:0 1px 2px rgb(0 0 0 / .06); }
div[data-testid="stMetricLabel"]{ color:#334155; font-weight:800; }
div[data-testid="stMetricValue"]{ font-size:26px!important; line-height:1.15!important; white-space:nowrap!important; font-weight:900!important; }
.stat-card{ border:1px solid var(--ring); border-radius:.9rem; background:var(--card); padding:.9rem 1rem; box-shadow:0 1px 2px rgba(0,0,0,.06); transition: transform .15s ease, box-shadow .15s ease; position:relative; cursor:default; height:86px; display:flex; flex-direction:column; justify-content:center; }
.stat-card:hover{ transform: translateY(-2px); box-shadow: 0 12px 22px rgba(31,58,138,.12), 0 4px 8px rgba(0,0,0,.08); }
.stat-label{ font-size:.92rem; color:#334155; font-weight:800; margin-bottom:.18rem; }
.stat-value{ font-size:1.25rem; font-weight:900; color:#111827; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.stat-card .tooltip{ display:none; position:absolute; left:12px; top:calc(100% + 6px); background:#0f172a; color:#fff; font-weight:700; padding:.45rem .6rem; border-radius:.5rem; white-space:nowrap; z-index:100; box-shadow: 0 8px 24px rgba(0,0,0,.18); }
.stat-card:hover .tooltip{ display:block; }
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
.err-red{ color:#b91c1c; font-weight:900; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
LOGO_PATH = os.path.join(ASSETS_DIR, "logo.png")

# ===================== LOCALE (fallback) =====================
def _set_locale_ptbr():
    for loc in ("pt_BR.utf8", "pt_BR.UTF-8", "pt_BR", "Portuguese_Brazil.1252"):
        try:
            _locale.setlocale(_locale.LC_TIME, loc); return
        except Exception:
            continue
_set_locale_ptbr()

# ===================== UTILS =====================
BAHIA_TZ = ZoneInfo("America/Bahia")
def today_bahia() -> date:
    return datetime.now(BAHIA_TZ).date()

MONTHS = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
MONTHS_SHORT = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

def money(x) -> Decimal:
    try:
        return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.00")

def format_currency(value) -> str:
    try:
        d = money(value)
    except Exception:
        d = Decimal("0.00")
    s = f"{d:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

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
    today = today_bahia()
    colm, coly = st.columns([2, 1])
    with colm:
        m = st.selectbox(f"{label} — Mês", list(range(1, 13)), index=today.month-1, format_func=lambda i: MONTHS[i-1])
    with coly:
        y = st.number_input("Ano", value=today.year, step=1, format="%d")
    return date(int(y), int(m), 1)

def _confirm_ok(val: str) -> bool:
    """Confirmação mais tolerante para 'EXCLUIR'."""
    return str(val or "").strip().upper() == "EXCLUIR"

# ===================== DB BASE & MODELS =====================
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)  # 'SEDE', 'TESOUREIRO', 'TESOUREIRO MISSIONÁRIO'
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

# ===================== ENGINE / SESSION =====================
@st.cache_resource
def get_engine():
    db_url = st.secrets.get("DATABASE_URL", os.environ.get("DATABASE_URL"))
    if not db_url:
        db_url = "sqlite:///database.db"  # persistente no workspace do app
    return create_engine(db_url, pool_pre_ping=True)

@st.cache_resource
def get_sessionmaker():
    engine = get_engine()
    Base.metadata.create_all(engine)  # garante criação após declarar os models
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

SessionLocal = get_sessionmaker()

# ===================== AUTH (hash) =====================
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

# ===================== AUTH COOKIE (persistência de login + inatividade) =====================
COOKIE_NAME = "chms_auth"
COOKIE_LAST = "chms_last"
APP_SECRET = st.secrets.get("APP_SECRET") or os.environ.get("APP_SECRET") or "troque-esta-chave"

def _make_token(payload: dict, exp_days: int = 30) -> str:
    data = payload.copy()
    data["exp"] = int(time.time()) + exp_days*24*3600
    js = json.dumps(data, separators=(",",":")).encode()
    b = base64.urlsafe_b64encode(js).decode()
    sig = hmac.new(APP_SECRET.encode(), b.encode(), hashlib.sha256).hexdigest()
    return f"{b}.{sig}"

def _read_token(tok: str | None) -> Optional[dict]:
    if not tok or "." not in tok:
        return None
    b, sig = tok.rsplit(".", 1)
    expected = hmac.new(APP_SECRET.encode(), b.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        js = base64.urlsafe_b64decode(b.encode())
        data = json.loads(js)
    except Exception:
        return None
    if int(data.get("exp", 0)) < int(time.time()):
        return None
    return data

# ⚠️ NÃO cachear: evita CachedWidgetWarning
def get_cookie_manager():
    import extra_streamlit_components as stx
    return stx.CookieManager()

def logout():
    # Marca para pular auto-login por cookie neste ciclo
    st.session_state.uid = None
    st.session_state["skip_cookie_login"] = True
    try:
        cm = get_cookie_manager()
        # Apaga cookies (delete + sobrescreve expirado para garantir)
        try:
            cm.delete(COOKIE_NAME)
            cm.delete(COOKIE_LAST)
        except Exception:
            pass
        past = datetime.utcnow() - timedelta(days=1)
        try:
            cm.set(COOKIE_NAME, "LOGOUT", expires_at=past)
            cm.set(COOKIE_LAST, "0", expires_at=past)
        except Exception:
            pass
    except Exception:
        pass
    st.rerun()

def _touch_activity(cm):
    try:
        cm.set(COOKIE_LAST, str(time.time()), expires_at=datetime.utcnow()+timedelta(days=30))
    except Exception:
        pass

def _check_inactivity_and_maybe_logout(cm):
    try:
        last = cm.get(COOKIE_LAST)
        if last:
            try:
                last_ts = float(last)
            except Exception:
                last_ts = time.time()
            if time.time() - last_ts > INACTIVITY_MINUTES * 60:
                logout()
        _touch_activity(cm)
    except Exception:
        pass

# ===================== SEED =====================
TYPE_IN = "DOAÇÃO"
TYPE_OUT = "SAÍDA"
LEGACY_TYPES = {"DOAÇÃO": ["RECEITA"], "SAÍDA": ["DESPESA"]}

CONGREGACOES_PADRAO = [
    "Sede", "Rodeadouro", "Dr. Humberto", "Jatobá", "Massaroca", "Riacho Seco", "Pedro Raimundo",
    "Lagoa do Salitre", "Lagoa da Areia", "Sítio Roçado", "Fazenda Bebedouro", "Junco", "Rua Vermelha",
    "Manga II", "Campos Casa", "Campos Terreno", "Alto Alencar", "Alto da Aliança", "Alto do Cruzeiro",
    "Amf Empreendimento", "Antônio Guilhermino I", "Antônio Guilhermino II", "Antônio Guilhermino III",
    "Abreus", "Argemiro", "Araras", "Baixo Salitre", "Bairro Vermelho", "Cacimba do Silva",
    "Campo dos Cavalos", "Campim de Raiz", "Carnaíba Carneiros", "Carnaíba Casa Pastoral",
    "Carnaíba Serra dos Espinhos", "Cipó Mandacaru", "Codevasf", "Fazenda Olaria", "Itaberaba",
    "Jardim Alvorada", "Jardim das Acácias", "Jardim Europa", "Jardim Primavera", "Jardim Vitória",
    "Jazida 7", "João Paulo II", "João Paulo II 2", "João Paulo II A",
    "João Paulo II Jp II Terreno Lado Templo", "João Paulo II Templo", "Juazeiro"
]

def ensure_seed():
    engine = get_engine()
    Base.metadata.create_all(engine)
    with SessionLocal() as db:
        # Categorias
        if db.scalar(select(func.count(Category.id))) == 0:
            for nm, tp in [
                ("Dízimo", TYPE_IN), ("Oferta", TYPE_IN), ("Missões", TYPE_IN),
                ("Aluguel", TYPE_OUT), ("Energia", TYPE_OUT), ("Assistência Social", TYPE_OUT),
                ("Produtos de Limpeza", TYPE_OUT), ("Transporte", TYPE_OUT), ("Material de Expediente", TYPE_OUT),
            ]:
                if not db.scalar(select(Category).where(Category.name == nm)):
                    db.add(Category(name=nm, type=tp))
        if not db.scalar(select(Category).where(Category.name == "Missões (Saída)")):
            db.add(Category(name="Missões (Saída)", type=TYPE_OUT))
        # Congregações
        existentes = set(db.scalars(select(Congregation.name)).all())
        faltantes = [n for n in CONGREGACOES_PADRAO if n not in existentes]
        if faltantes:
            db.add_all(Congregation(name=n) for n in faltantes)
            db.flush()
        sede_cong = db.scalar(select(Congregation).where(Congregation.name == "Sede"))
        if sede_cong is None:
            sede_cong = Congregation(name="Sede")
            db.add(sede_cong)
            db.flush()
        # Admin
        if db.scalar(select(User).where(User.username == ADMIN_USERNAME)) is None:
            db.add(User(
                username=ADMIN_USERNAME,
                password_hash=hash_password("123456"),
                role="SEDE",
                congregation_id=sede_cong.id,
            ))
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
    if os.path.exists(LOGO_PATH):
        with col_logo:
            st.image(LOGO_PATH, use_container_width=True)
    with col_title:
        st.markdown("<h1 class='page-title'>AD Relatório Financeiro</h1>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username == u))
            if user and verify_password(p, user.password_hash):
                st.session_state.uid = user.id
                st.session_state.pop("skip_cookie_login", None)
                # grava cookie persistente (30 dias)
                try:
                    cm = get_cookie_manager()
                    token = _make_token({"uid": int(user.id)})
                    cm.set(COOKIE_NAME, token, expires_at=datetime.utcnow()+timedelta(days=30))
                    cm.set(COOKIE_LAST, str(time.time()), expires_at=datetime.utcnow()+timedelta(days=30))
                except Exception:
                    st.warning("Login salvo só nesta sessão. Instale 'extra-streamlit-components' para lembrar o login.")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

# ===================== HELPERS =====================
def is_admin_general(user: "User") -> bool:
    return (user.username or "").strip().lower() == ADMIN_USERNAME.lower()

def categories_for_type(db: Session, kind: str) -> List[Category]:
    kinds = [kind] + LEGACY_TYPES.get(kind, [])
    cats = list(db.scalars(select(Category).where(Category.type.in_(kinds))).all())
    if kind == TYPE_IN:
        priority = {"dízimo": 0, "dizimo": 0, "oferta": 1, "missões": 2, "missoes": 2}
        def sort_key(c: Category):
            n = _norm(c.name)
            base = priority.get(n, 100)
            return (base, n)
        cats.sort(key=sort_key)
    else:
        cats.sort(key=lambda c: _norm(c.name))
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
    sede = [c for c in congs if _norm(c.name) == "sede"]
    others = sorted([c for c in congs if _norm(c.name) != "sede"], key=lambda x: _norm(x.name))
    return (sede + others) if sede else others

def sidebar_common(user: "User"):
    with st.sidebar:
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, use_column_width=True)
        st.write(f"👤 **{user.username}** — *{user.role}*")
        st.button("Sair", on_click=logout)

# ===================== FUNÇÕES DE EDIÇÃO/EXCLUSÃO (Transactions/Tithes) =====================
def _update_transaction_value(tx_id: int, new_value: float, cong_restrict_id: Optional[int] = None) -> bool:
    try:
        with SessionLocal() as db:
            q = db.query(Transaction).filter(Transaction.id == tx_id)
            if cong_restrict_id is not None:
                q = q.filter(Transaction.congregation_id == cong_restrict_id)
            obj = q.first()
            if not obj:
                return False
            obj.amount = float(money(new_value))
            db.commit()
        return True
    except Exception:
        return False

def _delete_transaction(tx_id: int, cong_restrict_id: Optional[int] = None) -> bool:
    try:
        with SessionLocal() as db:
            q = db.query(Transaction).filter(Transaction.id == tx_id)
            if cong_restrict_id is not None:
                q = q.filter(Transaction.congregation_id == cong_restrict_id)
            if q.delete(synchronize_session=False) == 0:
                return False
            db.commit()
        return True
    except Exception:
        return False

def _update_tithe_value(tithe_id: int, new_value: float, cong_restrict_id: Optional[int] = None) -> bool:
    try:
        with SessionLocal() as db:
            q = db.query(Tithe).filter(Tithe.id == tithe_id)
            if cong_restrict_id is not None:
                q = q.filter(Tithe.congregation_id == cong_restrict_id)
            obj = q.first()
            if not obj:
                return False
            obj.amount = float(money(new_value))
            db.commit()
        return True
    except Exception:
        return False

# ===================== COLETA =====================
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
        return t.category and _norm(t.category.name) in ("dizimo", "dízimo")
    def _is_oferta_tx(t: Transaction) -> bool:
        return t.category and _norm(t.category.name) == "oferta"
    def _is_mission_entry(t: Transaction) -> bool:
        return t.category and _norm(t.category.name) in ("missoes","missões")

    total_dizimos_tithe = sum((money(t.amount) for t in tithes), Decimal("0.00"))
    total_dizimos_trans  = sum((money(t.amount) for t in tx_in if _is_dizimo_tx(t)), Decimal("0.00"))
    total_dizimos_final  = max(total_dizimos_tithe, total_dizimos_trans)

    total_ofertas        = sum((money(t.amount) for t in tx_in if _is_oferta_tx(t)), Decimal("0.00"))
    total_missoes        = sum((money(t.amount) for t in tx_in if _is_mission_entry(t)), Decimal("0.00"))
    total_entradas_outros= sum((money(t.amount) for t in tx_in if not (_is_dizimo_tx(t) or _is_oferta_tx(t) or _is_mission_entry(t))), Decimal("0.00"))

    total_geral_entradas_sem_missoes = total_dizimos_final + total_ofertas + total_entradas_outros
    total_saidas        = sum((money(t.amount) for t in tx_out), Decimal("0.00"))
    saldo               = total_geral_entradas_sem_missoes + total_missoes - total_saidas

    return {
        "tx_in": tx_in,
        "tithes": tithes,
        "tx_out": tx_out,
        "totals": {
            "dizimos": total_dizimos_final,
            "dizimos_tithes": total_dizimos_tithe,
            "dizimos_trans": total_dizimos_trans,
            "ofertas": total_ofertas,
            "missoes": total_missoes,
            "entradas_outros": total_entradas_outros,
            "entradas_total_sem_missoes": total_geral_entradas_sem_missoes,
            "saidas_total": total_saidas,
            "saldo": saldo
        }
    }

# ===================== PAGE: LANÇAMENTOS =====================
def page_lancamentos(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

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

        # ---------- ENTRADA (DOAÇÃO) ----------
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar ENTRADA (Doação)")
        with st.form("form_entrada", clear_on_submit=True):
            for key, default in [("ent_data", today_bahia()), ("ent_valor", 0.0), ("ent_desc", ""), ("ent_flag_missoes", False)]:
                st.session_state.setdefault(key, default)

            c1,c2,c3 = st.columns([1.1,1.4,2])
            with c1:
                st.date_input("Data do Culto", value=st.session_state["ent_data"], key="ent_data", format="DD/MM/YYYY")
            with c2:
                cats_in = categories_for_type(db, TYPE_IN)
                cat_names_in = [c.name for c in cats_in] or ["—"]
                desired = ["Dízimo", "Oferta", "Missões"]
                desired_norm = [_norm(x) for x in desired]
                top = [n for n in cat_names_in if _norm(n) in desired_norm]
                rest = [n for n in cat_names_in if _norm(n) not in desired_norm]
                cat_display = top + rest
                st.selectbox("Categoria (ordem fixa: Dízimo, Oferta, Missões)", cat_display, key="ent_cat")
            with c3:
                st.text_input("Descrição (opcional)", key="ent_desc")

            if _norm(st.session_state.get("ent_cat","")) == "oferta":
                st.checkbox("Oferta de missões?", key="ent_flag_missoes")

            st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="ent_valor")

            if st.form_submit_button("Salvar ENTRADA", type="primary"):
                with SessionLocal() as _db:
                    cat_name = st.session_state["ent_cat"]
                    if st.session_state.get("ent_flag_missoes"):
                        cat_name = "Missões"
                        if not _db.scalar(select(Category).where(Category.name == "Missões")):
                            _db.add(Category(name="Missões", type=TYPE_IN)); _db.commit()

                    cat_obj = _db.scalar(select(Category).where(Category.name == cat_name))
                    if not cat_obj:
                        st.error("Informe a categoria."); return

                    _db.add(Transaction(
                        date=st.session_state["ent_data"],
                        type=TYPE_IN,
                        category_id=cat_obj.id,
                        amount=float(money(st.session_state["ent_valor"])),
                        description=(st.session_state["ent_desc"] or None),
                        congregation_id=cong_obj.id,
                        payment_method=None
                    ))
                    _db.commit()
                    st.success("Entrada registrada.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # ---------- DÍZIMOS ----------
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Salvar DIZIMISTA")
        with st.form("form_dizimo", clear_on_submit=True):
            for k,dft in [("dz_data", today_bahia()), ("dz_nome",""), ("dz_valor",0.0)]:
                st.session_state.setdefault(k,dft)

            c1,c2,c3 = st.columns([1.1,2.2,1.1])
            with c1:
                st.date_input("Data do Culto", value=st.session_state["dz_data"], key="dz_data", format="DD/MM/YYYY")
            with c2:
                st.text_input("Nome do dizimista", key="dz_nome")
            with c3:
                st.number_input("Valor dízimo (R$)", min_value=0.0, step=1.0, format="%.2f", key="dz_valor")

            st.selectbox("Forma de Pagamento", ["Dinheiro", "PIX"], key="dz_payment_method")

            if st.form_submit_button("Salvar DIZIMISTA", type="primary"):
                nome = (st.session_state.get("dz_nome") or "").strip()
                valor = float(money(st.session_state.get("dz_valor") or 0.0))
                dta = st.session_state.get("dz_data") or today_bahia()
                if not nome:
                    st.error("Informe o nome do dizimista."); return
                with SessionLocal() as _db:
                    _db.add(Tithe(
                        date=dta, tither_name=nome, amount=valor, congregation_id=cong_obj.id,
                        payment_method=st.session_state.get("dz_payment_method")
                    ))
                    _db.commit()
                    st.success("Dízimo registrado.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # ---------- SAÍDA ----------
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar SAÍDA")
        with st.form("form_saida", clear_on_submit=True):
            for key, default in [("sai_data", today_bahia()), ("sai_valor", 0.0), ("sai_desc", "")]:
                st.session_state.setdefault(key, default)

            c1,c2,c3 = st.columns([1.1,1.4,2])
            with c1:
                st.date_input("Data", value=st.session_state["sai_data"], key="sai_data", format="DD/MM/YYYY")
            with c2:
                cats_out = categories_for_type(db, TYPE_OUT)
                cat_names_out = [c.name for c in cats_out] or ["—"]
                st.selectbox("Tipo da saída (Categoria)", cat_names_out, key="sai_cat")
            with c3:
                st.text_input("Descrição (opcional)", key="sai_desc")

            st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="sai_valor")

            if st.form_submit_button("Salvar SAÍDA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name == st.session_state["sai_cat"]))
                    if not cat_obj:
                        st.error("Informe o tipo de saída."); return
                    _db.add(Transaction(
                        date=st.session_state["sai_data"], type=TYPE_OUT, category_id=cat_obj.id,
                        amount=float(money(st.session_state["sai_valor"])), description=(st.session_state["sai_desc"] or None),
                        congregation_id=cong_obj.id,
                    ))
                    _db.commit()
                    st.success("Saída registrada.")
        st.markdown('</div>', unsafe_allow_html=True)

# ===================== PAGE: RELATÓRIO DE ENTRADA =====================
def page_relatorio_entrada(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

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

        # Alerta de divergência de dízimos
        if data['totals']['dizimos_tithes'] != data['totals']['dizimos_trans']:
            st.markdown("<div class='err-red'>⚠️ valores dos dízimos não confere com o valores doados pelos dizimistas — favor verificar.</div>", unsafe_allow_html=True)

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
            summary_by_date = defaultdict(lambda: {"dizimo": Decimal("0.00"), "oferta": Decimal("0.00")})
            # Transactions
            for t in data["tx_in"]:
                if t.category and _norm(t.category.name) in ("dizimo", "dízimo"):
                    summary_by_date[t.date]["dizimo"] += money(t.amount)
                elif t.category and _norm(t.category.name) == "oferta":
                    summary_by_date[t.date]["oferta"] += money(t.amount)
            # Tithes (contabiliza no total do dia, porém edição dos nominais é na aba de Dizimistas)
            for t in data["tithes"]:
                summary_by_date[t.date]["dizimo"] += money(t.amount)

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
                gb = GridOptionsBuilder.from_dataframe(df_summary)
                gb.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
                grid = AgGrid(
                    df_summary, gridOptions=gb.build(),
                    data_return_mode=DataReturnMode.FILTERED, update_mode=GridUpdateMode.SELECTION_CHANGED,
                    use_container_width=True, height=220, allow_unsafe_jscode=True
                )
                sel = grid.selected_rows
                if sel:
                    day = datetime.strptime(sel[0]["Data do Culto"], "%d/%m/%Y").date()
                    st.markdown(f"**Lançamentos (Transactions) em {format_date(day)} — clique e edite**")
                    lista = [t for t in data["tx_in"] if t.date == day and t.category and _norm(t.category.name) in ("dizimo","dízimo","oferta")]
                    df_tx = pd.DataFrame([{
                        "ID": t.id,
                        "Categoria": t.category.name if t.category else "(Sem categoria)",
                        "Valor (R$)": format_currency(t.amount),
                        "Descrição": t.description or ""
                    } for t in lista])
                    if not df_tx.empty:
                        gb2 = GridOptionsBuilder.from_dataframe(df_tx)
                        gb2.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
                        grid2 = AgGrid(
                            df_tx, gridOptions=gb2.build(),
                            data_return_mode=DataReturnMode.FILTERED, update_mode=GridUpdateMode.SELECTION_CHANGED,
                            use_container_width=True, height=230, allow_unsafe_jscode=True
                        )
                        sel2 = grid2.selected_rows
                        if sel2:
                            rid = int(sel2[0]["ID"])
                            obj = next((t for t in lista if t.id == rid), None)
                            c1,c2,c3 = st.columns([1.4,1,1])
                            with c1:
                                novo = st.number_input("Novo valor (R$)", min_value=0.0, value=float(money(obj.amount)) if obj else 0.0, step=1.0, format="%.2f", key=f"re_new_{rid}")
                            with c2:
                                if st.button("Alterar valor", key=f"re_upd_{rid}"):
                                    if _update_transaction_value(rid, float(money(novo)), cong_restrict_id=cong_obj.id):
                                        st.success("Valor atualizado."); st.rerun()
                                    else:
                                        st.error("Falha ao atualizar.")
                            with c3:
                                if st.button("Excluir", key=f"re_del_{rid}"):
                                    if _delete_transaction(rid, cong_restrict_id=cong_obj.id):
                                        st.success("Excluído."); st.rerun()
                                    else:
                                        st.error("Falha ao excluir.")
                    else:
                        st.caption("Sem lançamentos de Dízimo/Oferta nesse dia.")
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
            agg = defaultdict(Decimal)
            for t in data["tx_in"]:
                if t.category and _norm(t.category.name) in ("missoes","missões"):
                    agg[t.congregation.name] += money(t.amount)
            dfm = pd.DataFrame([{"Congregação": k, "Entradas Missões": format_currency(v)} for k,v in sorted(agg.items())])
            if not dfm.empty:
                st.dataframe(dfm, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem entradas de Missões.")
        else:
            val = sum((money(t.amount) for t in data["tx_in"] if t.category and _norm(t.category.name) in ("missoes","missões")), Decimal("0.00"))
            st.metric("Entradas Missões", format_currency(val))

        st.divider()
        rows_csv = [{
            "Data": t.date.strftime("%d/%m/%Y"),
            "Congregação": t.congregation.name,
            "Categoria": t.category.name,
            "Valor": f"{money(t.amount):.2f}".replace(".", ","),
            "Descrição": t.description or ""
        } for t in data["tx_in"]]
        csv = pd.DataFrame(rows_csv).to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Baixar CSV das ENTRADAS do período", data=csv, file_name=f"entradas_{start.strftime('%Y-%m')}.csv", mime="text/csv")

        # >>> Removidas as antigas tabelas de exclusão por congregação conforme solicitado <<<

# ===================== PAGE: RELATÓRIO DE SAÍDA =====================
def page_relatorio_saida(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

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

        total_saidas = sum((money(t.amount) for t in txs), Decimal("0.00"))
        st.metric("Total de saídas", format_currency(total_saidas))

        st.divider()
        if is_all:
            agg = defaultdict(Decimal)
            for t in txs:
                agg[t.congregation.name] += money(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Total de saídas": format_currency(v)} for k,v in sorted(agg.items())])
            st.subheader("Resumo por congregação")
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem saídas.")
        else:
            st.subheader("Resumo por data (Saídas)")
            by_date = defaultdict(lambda: {"qtd":0, "total":Decimal("0.00")})
            for t in txs:
                by_date[t.date]["qtd"] += 1
                by_date[t.date]["total"] += money(t.amount)
            rows = []
            for d, v in sorted(by_date.items()):
                rows.append({"Data": format_date(d), "Qtde": v["qtd"], "Total (R$)": format_currency(v["total"])})
            df_sum = pd.DataFrame(rows)
            if not df_sum.empty:
                gb = GridOptionsBuilder.from_dataframe(df_sum)
                gb.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
                grid = AgGrid(
                    df_sum, gridOptions=gb.build(), data_return_mode=DataReturnMode.FILTERED,
                    update_mode=GridUpdateMode.SELECTION_CHANGED, height=220, allow_unsafe_jscode=True
                )
                sel = grid.selected_rows
                if sel:
                    sel_date = datetime.strptime(sel[0]["Data"], "%d/%m/%Y").date()
                    lista = [t for t in txs if t.date == sel_date]
                    st.markdown(f"**Lançamentos em {format_date(sel_date)} — clique e edite**")
                    df_list = pd.DataFrame([{
                        "ID": t.id,
                        "Categoria": t.category.name if t.category else "—",
                        "Descrição": t.description or "",
                        "Valor (R$)": format_currency(t.amount)
                    } for t in lista])
                    gb2 = GridOptionsBuilder.from_dataframe(df_list)
                    gb2.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
                    grid2 = AgGrid(
                        df_list, gridOptions=gb2.build(),
                        data_return_mode=DataReturnMode.FILTERED, update_mode=GridUpdateMode.SELECTION_CHANGED,
                        height=230, allow_unsafe_jscode=True
                    )
                    sel2 = grid2.selected_rows
                    if sel2:
                        rid = int(sel2[0]["ID"])
                        obj = next((t for t in lista if t.id == rid), None)
                        c1,c2,c3 = st.columns([1.2,1,1])
                        with c1:
                            novo = st.number_input("Novo valor (R$)", min_value=0.0, value=float(money(obj.amount)) if obj else 0.0, step=1.0, format="%.2f", key=f"rs_new_{rid}")
                        with c2:
                            if st.button("Alterar valor", key=f"rs_upd_{rid}"):
                                if _update_transaction_value(rid, float(money(novo)), cong_restrict_id=cong_obj.id):
                                    st.success("Valor atualizado."); st.rerun()
                                else:
                                    st.error("Falha ao atualizar.")
                        with c3:
                            if st.button("Excluir", key=f"rs_del_{rid}"):
                                if _delete_transaction(rid, cong_restrict_id=cong_obj.id):
                                    st.success("Excluído."); st.rerun()
                                else:
                                    st.error("Falha ao excluir.")
            else:
                st.caption("Sem saídas no período.")

        st.divider()
        rows = [{
            "Data": t.date.strftime("%d/%m/%Y"),
            "Congregação": t.congregation.name,
            "Tipo da saída": t.category.name if t.category else "",
            "Valor": f"{money(t.amount):.2f}".replace(".", ","),
            "Descrição": t.description or ""
        } for t in txs]
        csv = pd.DataFrame(rows).to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Baixar CSV das SAÍDAS do período",
            data=csv, file_name=f"saidas_{start.strftime('%Y-%m')}.csv", mime="text/csv"
        )

        # >> Removidas as seções “Missões no período (Saídas) / Saídas Missões” para congregações conforme pedido <<

# ===================== RELATÓRIO DE MISSÕES (GERAL — SEDE / TES. MISSIONÁRIO) =====================
def _collect_missions_data(db: Session, start: date, end: date, cong_id: Optional[int] = None):
    q_in = select(Transaction).options(joinedload(Transaction.congregation)).where(
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type == TYPE_IN,
        Transaction.category.has(Category.name.in_(("Missões", "missões")))
    ).order_by(Transaction.date)
    if cong_id is not None:
        q_in = q_in.where(Transaction.congregation_id == cong_id)
    entradas_missoes = db.scalars(q_in).all()
    
    q_out = select(Transaction).options(joinedload(Transaction.congregation)).where(
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type == TYPE_OUT,
        Transaction.category.has(Category.name.in_(("Missões (Saída)", "missões (saída)")))
    ).order_by(Transaction.date)
    if cong_id is not None:
        q_out = q_out.where(Transaction.congregation_id == cong_id)
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
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ])

    story: List = []
    story.append(Paragraph("Relatório Mensal de Missões", title_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("Resumo de Entradas de Missões por Congregação", heading_style))
    if entradas:
        entradas_cong_sum = defaultdict(Decimal)
        for t in entradas:
            entradas_cong_sum[t.congregation.name] += money(t.amount)
        entradas_data = [["Congregação", "Entradas (R$)"]]
        for cong_name, total in sorted(entradas_cong_sum.items()):
            entradas_data.append([cong_name, format_currency(total)])
        tbl_in = Table(entradas_data, colWidths=[9*cm, 9*cm]); tbl_in.setStyle(table_style); story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada de missões registrada.", styles['Normal']))

    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("Saídas de Missões", heading_style))
    if saidas:
        saidas_data = [["Data", "Descrição", "Valor (R$)"]]
        for t in saidas:
            saidas_data.append([t.date.strftime("%d/%m/%Y"), t.description or "—", format_currency(money(t.amount))])
        tbl_out = Table(saidas_data, colWidths=[3*cm, 10*cm, 5*cm]); tbl_out.setStyle(table_style); story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída de missões registrada.", styles['Normal']))

    story.append(Spacer(1, 1*cm))
    total_entradas_missions = sum((money(t.amount) for t in entradas), Decimal("0.00"))
    total_saidas_missions = sum((money(t.amount) for t in saidas), Decimal("0.00"))
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
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#eef2ff")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(summary_table)

    doc.build(story)
    return buf.getvalue()

def page_relatorio_missoes(user: "User"):
    if user.role not in ["SEDE", "TESOUREIRO MISSIONÁRIO"]:
        st.warning(f"🔒 Acesso negado. Apenas usuários `SEDE` ou `TESOUREIRO MISSIONÁRIO` podem acessar este relatório.")
        return
    
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown("<h1 class='page-title'>Relatório de Missões</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        st.info("Escopo: **Todas as congregações**")
        
        entradas_missoes, saidas_missoes = _collect_missions_data(db, start, end)

        total_entradas = sum((money(t.amount) for t in entradas_missoes), Decimal("0.00"))
        total_saidas = sum((money(t.amount) for t in saidas_missoes), Decimal("0.00"))
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
            ent_data = st.date_input("Data do Culto", value=today_bahia(), key="mis_ent_data", format="DD/MM/YYYY")
            ent_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="mis_ent_valor")
            ent_desc = st.text_input("Descrição (opcional)", key="mis_ent_desc")
            if st.form_submit_button("Salvar ENTRADA de Missões", type="primary"):
                with SessionLocal() as _db:
                    cong_obj = next(c for c in congs_all if c.name == cong_sel)
                    cat_obj = _db.scalar(select(Category).where(Category.name == "Missões"))
                    if not cat_obj:
                        st.error("Categoria 'Missões' não encontrada. Contate o administrador."); return
                    _db.add(Transaction(
                        date=ent_data,
                        type=TYPE_IN,
                        category_id=cat_obj.id,
                        amount=float(money(ent_valor)),
                        description=(ent_desc or None),
                        congregation_id=cong_obj.id,
                        payment_method=None
                    ))
                    _db.commit()
                    st.success(f"Entrada de missões para '{cong_sel}' registrada.")
                    st.rerun()
        
        st.divider()

        st.subheader("Entradas de Missões no Período")
        if entradas_missoes:
            df_e = pd.DataFrame([{
                "ID": t.id, "Data": format_date(t.date), "Congregação": t.congregation.name,
                "Descrição": t.description or "", "Valor (R$)": format_currency(t.amount)
            } for t in entradas_missoes])
            gb_e = GridOptionsBuilder.from_dataframe(df_e)
            gb_e.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
            grid_e = AgGrid(df_e, gridOptions=gb_e.build(), update_mode=GridUpdateMode.SELECTION_CHANGED,
                            data_return_mode=DataReturnMode.FILTERED, height=260, allow_unsafe_jscode=True)
            sel_e = grid_e.selected_rows
            if sel_e:
                rid = int(sel_e[0]["ID"])
                obj = next((t for t in entradas_missoes if t.id == rid), None)
                st.write(f"Selecionado: **ID {rid}** — {obj.congregation.name if obj else ''} — {format_currency(obj.amount) if obj else ''}")
                c1,c2,c3 = st.columns([1.2,1,1])
                with c1:
                    novo = st.number_input("Novo valor (R$)", min_value=0.0, value=float(money(obj.amount)) if obj else 0.0, step=1.0, format="%.2f", key=f"mis_e_val_{rid}")
                with c2:
                    if st.button("Alterar valor", key=f"mis_e_upd_{rid}"):
                        if _update_transaction_value(rid, float(money(novo))):
                            st.success("Valor atualizado."); st.rerun()
                        else:
                            st.error("Falha ao atualizar.")
                with c3:
                    if st.button("Excluir", key=f"mis_e_del_{rid}"):
                        if _delete_transaction(rid):
                            st.success("Excluído."); st.rerun()
                        else:
                            st.error("Falha ao excluir.")
        else:
            st.caption("Nenhuma entrada de missões registrada neste período.")

        st.divider()
        st.subheader("Lançar Saída de Missões")
        with st.form("form_saida_missoes", clear_on_submit=True):
            sai_data = st.date_input("Data", value=today_bahia(), format="DD/MM/YYYY")
            sai_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f")
            sai_desc = st.text_input("Descrição")
            if st.form_submit_button("Salvar SAÍDA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name == "Missões (Saída)"))
                    if not cat_obj:
                        st.error("Categoria 'Missões (Saída)' não encontrada. Contate o administrador."); return
                    sede_cong = _db.scalar(select(Congregation).where(Congregation.name == "Sede"))
                    if not sede_cong:
                         st.error("Congregação 'Sede' não encontrada."); return
                    _db.add(Transaction(
                        date=sai_data, type=TYPE_OUT, category_id=cat_obj.id,
                        amount=float(money(sai_valor)), description=(sai_desc or None),
                        congregation_id=sede_cong.id,
                    ))
                    _db.commit()
                    st.success("Saída de missões registrada.")
                    st.rerun()

        st.divider()
        st.subheader("Saídas de Missões no Período")
        if saidas_missoes:
            df_s = pd.DataFrame([{
                "ID": t.id, "Data": format_date(t.date), "Descrição": t.description or "", "Valor (R$)": format_currency(float(t.amount))
            } for t in saidas_missoes])
            gb_s = GridOptionsBuilder.from_dataframe(df_s)
            gb_s.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
            grid_s = AgGrid(df_s, gridOptions=gb_s.build(), update_mode=GridUpdateMode.SELECTION_CHANGED,
                            data_return_mode=DataReturnMode.FILTERED, height=240, allow_unsafe_jscode=True)
            sel_s = grid_s.selected_rows
            if sel_s:
                rid = int(sel_s[0]["ID"])
                obj = next((t for t in saidas_missoes if t.id == rid), None)
                st.write(f"Selecionado: **ID {rid}** — {format_currency(obj.amount) if obj else ''}")
                c1,c2,c3 = st.columns([1.2,1,1])
                with c1:
                    novo = st.number_input("Novo valor (R$)", min_value=0.0, value=float(money(obj.amount)) if obj else 0.0, step=1.0, format="%.2f", key=f"mis_s_val_{rid}")
                with c2:
                    if st.button("Alterar valor", key=f"mis_s_upd_{rid}"):
                        if _update_transaction_value(rid, float(money(novo))):
                            st.success("Valor atualizado."); st.rerun()
                        else:
                            st.error("Falha ao atualizar.")
                with c3:
                    if st.button("Excluir", key=f"mis_s_del_{rid}"):
                        if _delete_transaction(rid):
                            st.success("Excluído."); st.rerun()
                        else:
                            st.error("Falha ao excluir.")
        else:
            st.caption("Nenhuma saída de missões registrada neste período.")
        
        st.divider()
        st.subheader("Gerar Relatório em PDF")
        st.download_button(
            "⬇️ Baixar Relatório de Missões (PDF)",
            data=build_missions_report_pdf(ref, entradas_missoes, saidas_missoes),
            file_name=f"relatorio_missoes_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf"
        )

# ===================== RELATÓRIO DE MISSÕES (CONGREGAÇÃO — TESOUREIRO) =====================
def page_relatorio_missoes_congregacao(user: "User"):
    if user.role != "TESOUREIRO":
        st.warning("🔒 Disponível apenas para perfil TESOUREIRO.")
        return
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)
        st.markdown("<h1 class='page-title'>Relatório de Missões (Congregação)</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        congs = cong_options_for(user, db)
        if not congs:
            st.info("Sem congregação vinculada."); return
        cong_obj = congs[0]
        st.info(f"Escopo: **{cong_obj.name}**")

        entradas_missoes, _ = _collect_missions_data(db, start, end, cong_id=cong_obj.id)

        total_entradas = sum((money(t.amount) for t in entradas_missoes), Decimal("0.00"))
        st.metric("Total de Entradas de Missões", format_currency(total_entradas))

        st.subheader("Entradas de Missões no Período (clique para editar)")
        if entradas_missoes:
            df = pd.DataFrame([{
                "ID": t.id, "Data": format_date(t.date), "Descrição": t.description or "", "Valor (R$)": format_currency(t.amount)
            } for t in entradas_missoes])
            gb = GridOptionsBuilder.from_dataframe(df)
            gb.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
            grid = AgGrid(df, gridOptions=gb.build(), update_mode=GridUpdateMode.SELECTION_CHANGED,
                          data_return_mode=DataReturnMode.FILTERED, height=260, allow_unsafe_jscode=True)
            sel = grid.selected_rows
            if sel:
                rid = int(sel[0]["ID"])
                obj = next((t for t in entradas_missoes if t.id == rid), None)
                c1,c2,c3 = st.columns([1.2,1,1])
                with c1:
                    novo = st.number_input("Novo valor (R$)", min_value=0.0, value=float(money(obj.amount)) if obj else 0.0, step=1.0, format="%.2f", key=f"mis_cong_val_{rid}")
                with c2:
                    if st.button("Alterar valor", key=f"mis_cong_upd_{rid}"):
                        if _update_transaction_value(rid, float(money(novo)), cong_restrict_id=cong_obj.id):
                            st.success("Valor atualizado."); st.rerun()
                        else:
                            st.error("Falha ao atualizar.")
                with c3:
                    if st.button("Excluir", key=f"mis_cong_del_{rid}"):
                        if _delete_transaction(rid, cong_restrict_id=cong_obj.id):
                            st.success("Excluído."); st.rerun()
                        else:
                            st.error("Falha ao excluir.")
        else:
            st.caption("Sem entradas de missões neste período.")

# ===================== RELATÓRIO DE DIZIMISTAS =====================
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

    title = "Relatório de Pesquisa de Dizimistas"
    sub = f"Ano: {ano_pesq} | Congregação: {cong_sel} | Mês: {mes_sel}"
    if (nome_q or "").strip():
        sub += f" | Filtro: '{nome_q}'"

    story = [Paragraph(title, title_style), Paragraph(sub, subtitle_style), Spacer(1, 0.5*cm)]

    data_table = [df.columns.tolist()] + df.values.tolist()
    total_value = float(df["Total no ano (R$)"].sum())
    total_row = ["", "", "", "Total Geral:", total_value, "", ""]
    data_table.append(total_row)
    for row in data_table[1:]:
        # coluna 4 é o total
        try:
            if isinstance(row[4], float):
                row[4] = format_currency(row[4])
        except Exception:
            pass

    tbl = Table(data_table, colWidths=[3.5*cm, 3.5*cm, 2.0*cm, 2.5*cm, 2.5*cm, 2.0*cm, 2.0*cm])
    tbl.setStyle(table_style)
    story.append(tbl)
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(f"Dizimistas encontrados: **{len(df)}**", styles['Normal']))
    story.append(Paragraph(f"Total geral da pesquisa: **{format_currency(total_value)}**", styles['Normal']))

    doc.build(story)
    return buf.getvalue()

def build_dizimistas_nominal_pdf(tithes: List[Tithe], titulo: str, subtitulo: str) -> bytes:
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
        ("ALIGN", (2, 1), (2, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ])
    story = [Paragraph(titulo, title_style), Paragraph(subtitulo, subtitle_style)]
    data = [["Data", "Dizimista", "Valor (R$)", "Forma"]]
    total = Decimal("0.00")
    for t in tithes:
        data.append([t.date.strftime("%d/%m/%Y"), t.tither_name, format_currency(t.amount), t.payment_method or "—"])
        total += money(t.amount)
    data.append(["", "TOTAL", format_currency(total), ""])
    tbl = Table(data, colWidths=[3*cm, 9*cm, 3*cm, 3*cm]); tbl.setStyle(table_style); story.append(tbl)
    doc.build(story); return buf.getvalue()

def page_relatorio_dizimistas(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

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
            # Visão geral + PDF geral
            all_tz = db.scalars(select(Tithe).where(Tithe.date >= start, Tithe.date < end).order_by(Tithe.congregation_id, Tithe.date)).all()
            by_cong = defaultdict(lambda: {"qtd":0, "valor":Decimal("0.00")})
            for t in all_tz:
                k = t.congregation.name
                by_cong[k]["qtd"] += 1
                by_cong[k]["valor"] += money(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Qtde de dízimos": v["qtd"], "Total (R$)": format_currency(v["valor"])} for k,v in sorted(by_cong.items())])
            st.dataframe(df, use_container_width=True, hide_index=True, height=200)
            st.download_button(
                "⬇️ Baixar PDF — Lista nominal (todas as congregações)",
                data=build_dizimistas_nominal_pdf(all_tz, "Lista de Dizimistas (Mensal)", f"Referente a {ref.strftime('%B de %Y')} — Todas as congregações"),
                file_name=f"dizimistas_geral_{start.strftime('%Y-%m')}.pdf",
                mime="application/pdf"
            )
        else:
            # Tabela intuitiva por congregação com edição/exclusão
            tithes = db.scalars(select(Tithe).where(
                Tithe.date >= start, Tithe.date < end, Tithe.congregation_id == cong_obj.id
            ).order_by(Tithe.date)).all()

            st.subheader("Dízimos do período (clique para editar)")
            df = pd.DataFrame([{
                "ID": t.id,
                "Data": format_date(t.date),
                "Dizimista": t.tither_name,
                "Valor (R$)": format_currency(float(t.amount)),
                "Forma": t.payment_method or "—"
            } for t in tithes])
            if not df.empty:
                gb = GridOptionsBuilder.from_dataframe(df)
                gb.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
                grid = AgGrid(df, gridOptions=gb.build(), update_mode=GridUpdateMode.SELECTION_CHANGED,
                              data_return_mode=DataReturnMode.FILTERED, height=280, allow_unsafe_jscode=True)
                sel = grid.selected_rows
                if sel:
                    rid = int(sel[0]["ID"])
                    obj = next((t for t in tithes if t.id == rid), None)
                    c1,c2,c3 = st.columns([1.2,1,1])
                    with c1:
                        novo = st.number_input("Novo valor (R$)", min_value=0.0, value=float(money(obj.amount)) if obj else 0.0, step=1.0, format="%.2f", key=f"rd_val_{rid}")
                    with c2:
                        if st.button("Alterar valor", key=f"rd_upd_{rid}"):
                            if _update_tithe_value(rid, float(money(novo)), cong_restrict_id=cong_obj.id):
                                st.success("Valor atualizado."); st.rerun()
                            else:
                                st.error("Falha ao atualizar.")
                    with c3:
                        if st.button("Excluir", key=f"rd_del_{rid}"):
                            with SessionLocal() as _db:
                                if _db.query(Tithe).filter(Tithe.id==rid, Tithe.congregation_id==cong_obj.id).delete(synchronize_session=False):
                                    _db.commit(); st.success("Excluído."); st.rerun()
                                else:
                                    st.error("Falha ao excluir.")
                st.download_button(
                    "⬇️ Baixar PDF — Lista nominal (sua congregação)",
                    data=build_dizimistas_nominal_pdf(tithes, "Lista de Dizimistas (Mensal)", f"Referente a {ref.strftime('%B de %Y')} — {cong_obj.name}"),
                    file_name=f"dizimistas_{_norm(cong_obj.name)}_{start.strftime('%Y-%m')}.pdf",
                    mime="application/pdf"
                )
            else:
                st.caption("Sem dízimos neste período.")

        # ===== PESQUISA AVANÇADA POR ANO =====
        # De acordo com o pedido: congregações NÃO podem pesquisar. Mantemos só para SEDE (opcional).
        if user.role == "SEDE":
            st.divider()
            st.subheader("Pesquisa de Dizimistas (por Ano)")
            c1, c2, c3, c4 = st.columns([1.2, 1.8, 1.4, 2.6])
            with c1:
                ano_pesq = st.number_input("Ano", value=today_bahia().year, step=1, format="%d", key="srch_year")
            with c2:
                cong_opts = ["Todas"] + [c.name for c in order_congs_sede_first(congs)]
                cong_sel = st.selectbox("Congregação", cong_opts, key="srch_cong")
            with c3:
                mes_opt = ["Todos"] + MONTHS
                mes_sel = st.selectbox("Mês", mes_opt, index=0, key="srch_month")
            with c4:
                nome_q = st.text_input("Nome do dizimista (contém)", key="srch_name")

            year_start = date(int(ano_pesq), 1, 1)
            year_end = date(int(ano_pesq)+1, 1, 1)
            q = select(Tithe).options(joinedload(Tithe.congregation)).where(Tithe.date >= year_start, Tithe.date < year_end)

            if cong_sel != "Todas":
                cong_id_sel = next(c.id for c in congs if c.name == cong_sel)
                q = q.where(Tithe.congregation_id == cong_id_sel)

            if mes_sel != "Todos":
                m = MONTHS.index(mes_sel) + 1
                m_start = date(int(ano_pesq), m, 1)
                m_end = date(ano_pesq + (m==12), (m % 12) + 1, 1)
                q = q.where(Tithe.date >= m_start, Tithe.date < m_end)

            with SessionLocal() as db2:
                t_list = db2.scalars(q.order_by(Tithe.tither_name, Tithe.date)).all()

            if (nome_q or "").strip():
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
            for k, info in agg.items():
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
                gb = GridOptionsBuilder.from_dataframe(df_pesq)
                gb.configure_grid_options(domLayout='normal')
                gb.configure_column("Dizimista", filter=True, floatingFilter=True)
                gb.configure_column("Congregação", filter=True, floatingFilter=True)
                gridOptions = gb.build()
                AgGrid(df_pesq, gridOptions=gridOptions, data_return_mode='AS_INPUT', update_mode='MODEL_CHANGED',
                       allow_unsafe_jscode=True, enable_enterprise_modules=True, height=300)
                tot_reg = len(df_pesq)
                tot_val = float(df_pesq["Total no ano (R$)"].sum())
                cA, cB = st.columns(2)
                cA.metric("Dizimistas encontrados", f"{tot_reg}")
                cB.metric("Total geral da pesquisa", format_currency(tot_val))
                csv = df_pesq.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "⬇️ Baixar CSV da pesquisa",
                    data=csv, file_name=f"pesquisa_dizimistas_{ano_pesq}.csv", mime="text/csv"
                )
                pdf_data = build_dizimista_search_pdf(df_pesq.assign(**{"Total no ano (R$)": df_pesq["Total no ano (R$)"]}), ano_pesq, cong_sel, mes_sel, nome_q)
                st.download_button(
                    "⬇️ Baixar PDF da pesquisa",
                    data=pdf_data, file_name=f"pesquisa_dizimistas_{ano_pesq}.pdf", mime="application/pdf"
                )
            else:
                st.caption("Nenhum resultado para os filtros informados.")

# ===================== PDF: PRESTAÇÃO DE CONTAS =====================
def build_full_statement_pdf(cong_id: int, cong_name: str, ref: date) -> bytes:
    buf = BytesIO()
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['Heading2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    table_style = TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ])
    tithe_table_style = TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (2, 1), (2, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ])

    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    story: List = []

    with SessionLocal() as db:
        start, end = month_bounds(ref)
        data = _collect_month_data(db, cong_id, start, end)

        entries_by_date = defaultdict(lambda: {"dizimo": Decimal("0.00"), "oferta": Decimal("0.00"), "missoes": Decimal("0.00"), "outros": Decimal("0.00")})
        for t in data["tx_in"]:
            if t.category and _norm(t.category.name) in ("dizimo", "dízimo"):
                entries_by_date[t.date]["dizimo"] += money(t.amount)
            elif t.category and _norm(t.category.name) == "oferta":
                entries_by_date[t.date]["oferta"] += money(t.amount)
            elif t.category and _norm(t.category.name) in ("missoes","missões"):
                entries_by_date[t.date]["missoes"] += money(t.amount)
            else:
                entries_by_date[t.date]["outros"] += money(t.amount)
        for t in data["tithes"]:
            entries_by_date[t.date]["dizimo"] += money(t.amount)

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

    # 1. Entradas
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
        story.append(Paragraph("Nenhuma entrada registrada.", getSampleStyleSheet()['Normal']))
    story.append(Spacer(1, 0.5*cm))

    # 2. Dizimistas
    story.append(Paragraph("2. Dizimistas (Lançamentos Nominais)", heading_style))
    if len(tithe_data) > 1:
        tbl_tithe = Table(tithe_data, colWidths=[3*cm, 9.5*cm, 4.5*cm])
        tbl_tithe.setStyle(tithe_table_style)
        story.append(tbl_tithe)
    else:
        story.append(Paragraph("Nenhum dízimo registrado.", getSampleStyleSheet()['Normal']))
    story.append(Spacer(1, 0.5*cm))

    # 3. Saídas
    story.append(Paragraph("3. Saídas", heading_style))
    if len(tx_out_data) > 1:
        tbl_out = Table(tx_out_data, colWidths=[2.5*cm, 4.5*cm, 7.5*cm, 3.5*cm])
        tbl_out.setStyle(table_style)
        story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída registrada.", getSampleStyleSheet()['Normal']))
    story.append(Spacer(1, 1*cm))

    # 4. Resumo Financeiro do Mês
    story.append(Paragraph("4. Resumo Financeiro do Mês", heading_style))
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
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e2fbe2")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 0.8*cm))

    # 5. Missões (Resumo do Mês)
    story.append(Paragraph("5. Missões (Resumo do Mês)", heading_style))
    missions_table = Table(
        [["Descrição", "Valor"], ["Total de Missões no Mês", format_currency(totals.get("missoes", 0.0))]],
        colWidths=[8*cm, 8*cm]
    )
    missions_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ]))
    story.append(missions_table)

    doc.build(story)
    return buf.getvalue()

# ===================== RELATÓRIO CONSOLIDADO (Visão Geral) =====================
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
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e2fbe2")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ])
    table_style_missions = TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#eef2ff")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ])

    story: List = []
    story.append(Paragraph("Relatório Mensal", title_style))
    story.append(Paragraph(f"Mês de Referência: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 1*cm))

    table_data = [["Congregação", "Entradas (D+O+Outras)", "Saídas", "Saldo"]]
    total_entradas = total_saidas = total_saldo = Decimal("0.00")
    missions_rows = []; total_missoes = Decimal("0.00")

    for c_name, entradas, saidas, saldo, missoes in agg_total:
        table_data.append([c_name, format_currency(entradas), format_currency(saidas), format_currency(saldo)])
        total_entradas += money(entradas); total_saidas += money(saidas); total_saldo += money(saldo)
        missions_rows.append([c_name, money(missoes)]); total_missoes += money(missoes)

    table_data.append(["TOTAL GERAL", format_currency(total_entradas), format_currency(total_saidas), format_currency(total_saldo)])
    tbl = Table(table_data, colWidths=[5*cm, 4*cm, 4*cm, 4*cm]); tbl.setStyle(table_style_main)
    story.append(tbl)

    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("Missões por Congregação (Entradas)", heading_style))
    missions_table_data = [["Congregação", "Missões (entrada)"]]
    for r in missions_rows:
        missions_table_data.append([r[0], format_currency(r[1])])
    missions_table_data.append(["TOTAL GERAL DE MISSÕES", format_currency(total_missoes)])
    tbl_m = Table(missions_table_data, colWidths=[9*cm, 9*cm]); tbl_m.setStyle(table_style_missions)
    story.append(tbl_m)

    doc.build(story)
    return buf.getvalue()

# ===================== HELPER: STAT CARD =====================
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

# ===================== PAGE: VISÃO GERAL =====================
def page_visao_geral(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)
        
        congs = cong_options_for(user, db)
        ordered = order_congs_sede_first(congs)
        
        is_all = (user.role == "SEDE")
        if is_all:
            st.info("Escopo: **Todas as congregações**")
        elif congs:
            cong_obj = congs[0]
            st.info(f"Escopo: **{cong_obj.name}**")
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
                    label = f"{i+1}º lugar"
                    text = f"{row['Congregação']} — {format_currency(float(row['Entradas (D+O + Outras)']))}"
                    render_stat_card(cols[i], label, text)
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
            st.divider()
            st.subheader("Resumo Financeiro Mensal")
            df_summary_cong = pd.DataFrame([{"Métricas": "Entradas (D+O + Outras)", "Valor": format_currency(agg_total[0][1])},
                                            {"Métricas": "Saídas", "Valor": format_currency(agg_total[0][2])},
                                            {"Métricas": "Saldo", "Valor": format_currency(agg_total[0][3])},
                                            {"Métricas": "Missões (entrada)", "Valor": format_currency(agg_total[0][4])}])
            st.dataframe(df_summary_cong, use_container_width=True, hide_index=True)

        if user.role == "SEDE":
            st.divider()
            st.subheader("Relatório Consolidado Mensal")
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

# ===================== PAGE: CADASTRO (ADMIN) =====================
def page_cadastro(user: "User"):
    if not is_admin_general(user):
        st.warning("🔒 Apenas o **administrador geral** (admin) pode acessar o Cadastro.")
        return
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)

        # Congregações
        st.subheader("Congregações")
        new_cong = st.text_input("Nova congregação", key="cad_new_cong")
        if st.button("Adicionar congregação", disabled=not new_cong.strip(), key="cad_add_cong"):
            if db.scalar(select(Congregation).where(Congregation.name == new_cong.strip())):
                st.error("Já existe congregação com esse nome.")
            else:
                db.add(Congregation(name=new_cong.strip())); db.commit()
                st.success("Congregação adicionada."); st.rerun()

        congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        users_by_cong = dict(db.execute(select(Congregation.id, func.count(User.id))
                               .join(User, User.congregation_id == Congregation.id, isouter=True)
                               .group_by(Congregation.id)).all())
        tx_by_cong = dict(db.execute(select(Congregation.id, func.count(Transaction.id))
                         .join(Transaction, Transaction.congregation_id == Congregation.id, isouter=True)
                         .group_by(Congregation.id)).all())
        tithes_by_cong = dict(db.execute(select(Congregation.id, func.count(Tithe.id))
                                 .join(Tithe, Tithe.congregation_id == Congregation.id, isouter=True)
                                 .group_by(Congregation.id)).all())
        dfc = pd.DataFrame([{
            "ID": c.id, "Nome": c.name,
            "Usuários": int(users_by_cong.get(c.id, 0)),
            "Lançamentos": int(tx_by_cong.get(c.id, 0)),
            "Dízimos": int(tithes_by_cong.get(c.id, 0)),
        } for c in congs_all])
        if not dfc.empty:
            st.dataframe(dfc, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir congregações"):
            st.caption("Só é possível excluir congregações **sem usuários, lançamentos ou dízimos**. A congregação **Sede** não pode ser excluída.")
            eligible_ids = []
            for c in congs_all:
                if _norm(c.name) == "sede": continue
                if users_by_cong.get(c.id, 0) == 0 and tx_by_cong.get(c.id, 0) == 0 and tithes_by_cong.get(c.id, 0) == 0:
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

        # Categorias
        st.subheader("Categorias")
        col1, col2 = st.columns(2)
        with col1:
            cat_name = st.text_input("Nome da categoria", key="cad_cat_name")
        with col2:
            cat_type = st.selectbox("Tipo", [TYPE_IN, TYPE_OUT], key="cad_cat_type")
        if st.button("Adicionar categoria", disabled=not cat_name.strip(), key="cad_add_cat"):
            if db.scalar(select(Category).where(Category.name == cat_name.strip())):
                st.error("Já existe categoria com esse nome.")
            else:
                db.add(Category(name=cat_name.strip(), type=cat_type)); db.commit()
                st.success("Categoria adicionada."); st.rerun()

        cats = db.scalars(select(Category).order_by(Category.type, Category.name)).all()
        usage = dict(db.execute(select(Category.id, func.count(Transaction.id))
                           .join(Transaction, Transaction.category_id == Category.id, isouter=True)
                           .group_by(Category.id)).all())
        dfcat = pd.DataFrame([{
            "ID": c.id, "Nome": c.name, "Tipo": c.type, "Usos em lançamentos": int(usage.get(c.id, 0))
        } for c in cats])
        if not dfcat.empty:
            st.dataframe(dfcat, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir categorias"):
            st.caption("Só é possível excluir categorias **sem lançamentos** vinculados.")
            ids_del = st.multiselect("IDs de categorias para excluir", dfcat.loc[dfcat["Usos em lançamentos"] == 0, "ID"].tolist(), key="cad_del_cat_ids")
            confc = st.text_input("Digite EXCLUIR para confirmar", key="cad_del_cat_conf")
            btn_disabled = (not ids_del) or (not _confirm_ok(confc))
            if st.button("Excluir categorias selecionadas", disabled=btn_disabled, key="cad_del_cat_btn"):
                with SessionLocal() as _db:
                    _db.query(Category).filter(Category.id.in_(ids_del)).delete(synchronize_session=False)
                    _db.commit()
                st.success(f"{len(ids_del)} categoria(s) excluída(s)."); st.rerun()
        st.divider()

        # Usuários
        st.subheader("Usuários")
        u_user = st.text_input("Usuário (login)", key="cad_user_login")
        u_pwd = st.text_input("Senha", type="password", key="cad_user_pwd")
        u_role = st.selectbox("Perfil", ["SEDE", "TESOUREIRO", "TESOUREIRO MISSIONÁRIO"], key="cad_user_role")
        all_congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        cong_options = ["—"] + [c.name for c in all_congs]
        cong_sel_key = "cad_user_cong"
        if u_role == "TESOUREIRO MISSIONÁRIO":
            try:
                index = cong_options.index("Sede")
            except ValueError:
                index = 0
            u_cong_name = st.selectbox("Congregação (vinculada a Saídas de Missões)", cong_options, index=index, key=cong_sel_key)
        else:
            u_cong_name = st.selectbox("Congregação", cong_options, key=cong_sel_key)

        if st.button("Criar usuário", key="cad_user_add"):
            if not u_user.strip() or not u_pwd.strip():
                st.error("Usuário e senha são obrigatórios.")
            elif db.scalar(select(User).where(User.username == u_user.strip())):
                st.error("Usuário já existe.")
            else:
                cong_id = None
                if u_role == "TESOUREIRO":
                    if u_cong_name == "—":
                        st.error("Selecione a congregação."); return
                    cong_id = next(c.id for c in all_congs if c.name == u_cong_name)
                elif u_role == "TESOUREIRO MISSIONÁRIO":
                     cong_id = db.scalar(select(Congregation.id).where(Congregation.name == "Sede"))
                db.add(User(username=u_user.strip(), password_hash=hash_password(u_pwd.strip()), role=u_role, congregation_id=cong_id))
                db.commit()
                st.success("Usuário criado."); st.rerun()

        users = db.scalars(select(User).order_by(User.username)).all()
        dfu = pd.DataFrame([{
            "ID": u.id, "Usuário": u.username, "Perfil": u.role,
            "Congregação": (db.get(Congregation, u.congregation_id).name if u.congregation_id else "—")
        } for u in users])
        if not dfu.empty:
            st.dataframe(dfu, use_container_width=True, hide_index=True, height=200)

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

# ===================== MAIN =====================
def main():
    try:
        ensure_seed()

        # Restaura login do cookie antes de consultar current_user()
        try:
            if not st.session_state.get("skip_cookie_login"):
                cm = get_cookie_manager()
                tok = cm.get(COOKIE_NAME)
                data = _read_token(tok)
                if data and not st.session_state.get("uid"):
                    with SessionLocal() as db:
                        u = db.get(User, int(data["uid"]))
                        if u:
                            st.session_state.uid = u.id
                if st.session_state.get("uid"):
                    _check_inactivity_and_maybe_logout(cm)
        except Exception:
            pass

        user = current_user()
        if not user:
            login_ui(); return

        with st.sidebar:
            if user.role == "SEDE":
                menu_options = ["Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Dizimistas", "Relatório de Missões", "Visão Geral", "Cadastro"]
            elif user.role == "TESOUREIRO":
                menu_options = ["Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Missões (Congregação)", "Relatório de Dizimistas", "Visão Geral"]
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
        elif page == "Relatório de Missões (Congregação)":
            page_relatorio_missoes_congregacao(user)
        elif page == "Visão Geral":
            page_visao_geral(user)
        elif page == "Cadastro":
            page_cadastro(user)
    except Exception as e:
        st.error("Ocorreu um erro ao renderizar a aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
