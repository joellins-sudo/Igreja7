# main.py — AD Relatório Financeiro — v14.2 (Solução Completa e Estável)
# CORREÇÃO: Versão completa com migração automática de banco de dados para preservar dados existentes.
# - Garante que as colunas 'service_type' sejam adicionadas às tabelas 'transactions' e 'tithes' se não existirem.

from __future__ import annotations

import os
from datetime import date, timedelta, datetime
from typing import Optional, List, Tuple, Dict, Any
from collections import defaultdict
import locale as _locale
import pandas as pd
import streamlit as st

from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine, and_, inspect
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
from sqlalchemy.ext.declarative import declarative_base
import unicodedata as ud
import hashlib
import json, base64, hmac, time

# PDF
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER

# TZ Bahia/BR
try:
    from zoneinfo import ZoneInfo
    TZ_BA = ZoneInfo("America/Bahia")
except Exception:
    TZ_BA = None

APP_NAME = "AD Relatório Financeiro"
ADJ_ENTRY_DESC = "[Ajuste via relatório de entrada]"
ADJ_MISS_IN_DESC = "[Ajuste Missões por Congregação]"
ADJ_SEDE_IN_DESC = "[Ajuste Sede via Visão Geral (Entrada)]"
ADJ_SEDE_OUT_DESC = "[Ajuste Sede via Visão Geral (Saída)]"

# ===================== ST CONFIG / THEME =====================
st.set_page_config(page_title=APP_NAME, page_icon="⛪", layout="wide")

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
.page-title{ font-family:'Nunito','Inter',system-ui; font-size:36px; font-weight:900; letter-spacing:.4px; color: var(--brand); margin:.25rem 0 1rem; }
h2, .stMarkdown h2, .st-subheader{ color:#0f172a!important; font-weight:900 !important; }
.st-container-card{ border: 1px solid var(--ring); border-radius: 1rem; padding: 1rem; margin-bottom: 1.15rem; background: var(--card); box-shadow:0 10px 30px rgb(31 58 138 / .06), 0 2px 8px rgb(0 0 0 /.04); position: relative; }
.st-container-card::before{ content:""; position: absolute; left:0; top:0; bottom:0; width:6px; background: linear-gradient(180deg,var(--brand-2), var(--brand)); border-top-left-radius:1rem; border-bottom-left-radius:1rem; }
div[data-testid="stMetric"]{ padding: .75rem .9rem; border:1px solid var(--ring); border-radius:.9rem; background:var(--card); box-shadow:0 1px 2px rgb(0 0 0 /.06); }
div[data-testid="stMetricLabel"]{ color:#334155; font-weight:800; }
div[data-testid="stMetricValue"]{ font-size:26px!important; line-height:1.15!important; white-space:nowrap!important; font-weight:900!important; }
.stat-card{ border:1px solid var(--ring); border-radius:.9rem; background:var(--card); padding:.9rem 1rem; box-shadow:0 1px 2px rgba(0,0,0,.06); transition: transform .15s ease, box-shadow .15s ease; position:relative; height:86px; display:flex; flex-direction:column; justify-content:center; }
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
MONTHS = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
MONTHS_SHORT = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

def now_bahia() -> datetime:
    try:
        return datetime.now(TZ_BA) if TZ_BA else datetime.now()
    except Exception:
        return datetime.now()

def today_bahia() -> date:
    return now_bahia().date()

def format_currency(value: float) -> str:
    try:
        v = float(value or 0.0)
    except Exception:
        v = 0.0
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
    today = today_bahia()
    colm, coly = st.columns([2, 1])
    with colm:
        m = st.selectbox(f"{label} — Mês", list(range(1, 13)), index=today.month-1, format_func=lambda i: MONTHS[i-1])
    with coly:
        y = st.number_input("Ano", value=today.year, step=1, format="%d")
    return date(int(y), int(m), 1)

def _confirm_ok(val: str) -> bool:
    return str(val or "").strip().upper() == "EXCLUIR"

def _to_date(obj: Any) -> date:
    if isinstance(obj, date):
        return obj
    s = str(obj or "").strip()
    if not s:
        return today_bahia()
    try:
        if "/" in s:
            return datetime.strptime(s, "%d/%m/%Y").date()
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return today_bahia()

def _to_float_brl(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x)
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

# ===================== DB BASE & MODELS =====================
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)
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
    type: Mapped[str] = mapped_column(String)
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="category")

class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    type: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    amount: Mapped[float] = mapped_column(Float)
    description: Mapped[Optional[str]] = mapped_column(String)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)
    service_type: Mapped[Optional[str]] = mapped_column(String, index=True, default=None)
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
    service_type: Mapped[Optional[str]] = mapped_column(String, index=True, default=None)
    congregation: Mapped["Congregation"] = relationship(back_populates="tithes")

# ===================== ENGINE / SESSION =====================
@st.cache_resource
def get_engine():
    db_url = st.secrets.get("DATABASE_URL", os.environ.get("DATABASE_URL"))
    if not db_url:
        db_url = "sqlite:///database.db"
    return create_engine(db_url, pool_pre_ping=True)

@st.cache_resource
def get_sessionmaker():
    engine = get_engine()
    Base.metadata.create_all(engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ===================== CORREÇÃO: MIGRAÇÃO DO BANCO DE DADOS =====================
@st.cache_resource(show_spinner=False)
def migrate_database():
    """
    Verifica e adiciona colunas ausentes para evitar erros em bancos de dados existentes.
    Executa apenas uma vez.
    """
    try:
        engine = get_engine()
        inspector = inspect(engine)
        
        # Verifica a tabela 'transactions'
        columns_transactions = [c['name'] for c in inspector.get_columns('transactions')]
        if 'service_type' not in columns_transactions:
            with engine.connect() as con:
                transaction = con.begin()
                con.execute('ALTER TABLE transactions ADD COLUMN service_type VARCHAR')
                transaction.commit()
                print("MIGRATION: Coluna 'service_type' adicionada à tabela 'transactions'.")

        # Verifica a tabela 'tithes'
        columns_tithes = [c['name'] for c in inspector.get_columns('tithes')]
        if 'service_type' not in columns_tithes:
            with engine.connect() as con:
                transaction = con.begin()
                con.execute('ALTER TABLE tithes ADD COLUMN service_type VARCHAR')
                transaction.commit()
                print("MIGRATION: Coluna 'service_type' adicionada à tabela 'tithes'.")
        print("MIGRATION: Verificação do banco de dados concluída.")
    except Exception as e:
        print(f"ERRO DURANTE MIGRAÇÃO: {e}")
        # Não lança exceção para não quebrar o app se a migração falhar,
        # mas o erro ainda ocorrerá nas consultas.
        pass

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

# ===================== AUTH COOKIE =====================
COOKIE_NAME = "chms_auth"
LAST_COOKIE = "chms_last"
APP_SECRET = st.secrets.get("APP_SECRET") or os.environ.get("APP_SECRET") or "troque-esta-chave"
INACTIVITY_MINUTES = int(os.environ.get("INACTIVITY_MINUTES", st.secrets.get("INACTIVITY_MINUTES", 20)))

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

def get_cookie_manager():
    import extra_streamlit_components as stx
    if "cookie_mgr" not in st.session_state:
        st.session_state["cookie_mgr"] = stx.CookieManager()
    return st.session_state["cookie_mgr"]

def _update_last_active(cm):
    try:
        cm.set(LAST_COOKIE, str(int(time.time())), expires_at=datetime.utcnow()+timedelta(days=30), key="last_set")
    except Exception:
        pass

def _check_inactivity_and_logout(cm):
    try:
        last = cm.get(LAST_COOKIE)
        if last:
            last_ts = int(str(last))
            if int(time.time()) - last_ts > INACTIVITY_MINUTES * 60:
                logout()
                st.stop()
    except Exception:
        pass

def logout():
    st.session_state.uid = None
    try:
        cm = get_cookie_manager()
        if hasattr(cm, "delete"):
            cm.delete(COOKIE_NAME, key="auth_del")
            cm.delete(LAST_COOKIE, key="last_del")
        else:
            cm.set(COOKIE_NAME, "", expires_at=datetime.utcnow()-timedelta(days=1), key="auth_del_fallback")
            cm.set(LAST_COOKIE, "", expires_at=datetime.utcnow()-timedelta(days=1), key="last_del_fallback")
    except Exception:
        pass
    st.rerun()

# ===================== SEED =====================
TYPE_IN = "DOAÇÃO"
TYPE_OUT = "SAÍDA"
LEGACY_TYPES = {"DOAÇÃO": ["RECEITA"], "SAÍDA": ["DESPESA"]}

CONGREGACOES_PADRAO = [
    "Sede","Rodeadouro","Dr. Humberto","Jatobá","Massaroca","Riacho Seco","Pedro Raimundo",
    "Lagoa do Salitre","Lagoa da Areia","Sítio Roçado","Fazenda Bebedouro","Junco","Rua Vermelha",
    "Manga II","Campos Casa","Campos Terreno","Alto Alencar","Alto da Aliança","Alto do Cruzeiro",
    "Amf Empreendimento","Antônio Guilhermino I","Antônio Guilhermino II","Antônio Guilhermino III",
    "Abreus","Argemiro","Araras","Baixo Salitre","Bairro Vermelho","Cacimba do Silva",
    "Campo dos Cavalos","Campim de Raiz","Carnaíba Carneiros","Carnaíba Casa Pastoral",
    "Carnaíba Serra dos Espinhos","Cipó Mandacaru","Codevasf","Fazenda Olaria","Itaberaba",
    "Jardim Alvorada","Jardim das Acácias","Jardim Europa","Jardim Primavera","Jardim Vitória",
    "Jazida 7","João Paulo II","João Paulo II 2","João Paulo II A",
    "João Paulo II Jp II Terreno Lado Templo","João Paulo II Templo","Juazeiro"
]

def ensure_seed():
    engine = get_engine()
    Base.metadata.create_all(engine)
    with SessionLocal() as db:
        default_categories = {
            "Dízimo": TYPE_IN, "Oferta": TYPE_IN, "Missões": TYPE_IN,
            "Aluguel": TYPE_OUT, "Energia": TYPE_OUT, "Assistência Social": TYPE_OUT,
            "Produtos de Limpeza": TYPE_OUT, "Transporte": TYPE_OUT, "Material de Expediente": TYPE_OUT,
            "Missões (Saída)": TYPE_OUT,
            "Ajuste Sede (Entrada)": TYPE_IN,
            "Ajuste Sede (Saída)": TYPE_OUT
        }
        existing_cats = set(db.scalars(select(Category.name)).all())
        for name, type_ in default_categories.items():
            if name not in existing_cats:
                db.add(Category(name=name, type=type_))

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
        if db.scalar(select(User).where(User.username == "admin")) is None:
            db.add(User(
                username="admin",
                password_hash=hash_password("123456"),
                role="SEDE",
                congregation_id=sede_cong.id,
            ))
        db.commit()

# ===================== SESSION / LOGIN =====================
if "uid" not in st.session_state:
    st.session_state.uid = None

def current_user():
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
        st.markdown(f"<h1 class='page-title'>{APP_NAME}</h1>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username == u))
            if user and verify_password(p, user.password_hash):
                st.session_state.uid = user.id
                try:
                    cm = get_cookie_manager()
                    token = _make_token({"uid": int(user.id)})
                    cm.set(COOKIE_NAME, token, expires_at=datetime.utcnow()+timedelta(days=30), key="auth_set")
                    _update_last_active(cm)
                except Exception:
                    st.warning("Login salvo só na sessão atual. Instale 'extra-streamlit-components' para lembrar o login.")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

# ===================== HELPERS =====================
def is_admin_general(user: "User") -> bool:
    return (user.username or "").strip().lower() == "admin"

def categories_for_type(db: Session, kind: str) -> List[Category]:
    kinds = [kind] + LEGACY_TYPES.get(kind, [])
    cats = list(db.scalars(select(Category).where(Category.type.in_(kinds))).all())
    if kind == TYPE_IN:
        priority = {"dízimo": 0, "dizimo": 0, "oferta": 1, "missões": 2, "missoes": 2}
        def sort_key(c: Category):
            n = _norm(c.name); base = priority.get(n, 100)
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
        if st.button("Sair"):
            logout()

# ===================== AUTO-SAVE CORE =====================
def _auto_save_if_changed(orig_df_view: pd.DataFrame, edited_df_view: pd.DataFrame, save_fn, *save_args):
    try:
        changed = not edited_df_view.equals(orig_df_view)
    except Exception:
        changed = True
    if changed:
        save_fn(*save_args)
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

# ===================== APPLY CHANGES — LANÇAMENTOS / DÍZIMOS =====================
def _apply_tx_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, tx_type: str, default_cong_id: Optional[int]):
    def norm_df(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        if "Valor" in d.columns:
            d["Valor"] = d["Valor"].map(_to_float_brl)
        if "Data" in d.columns:
            d["Data"] = d["Data"].map(_to_date)
        for c in ("Categoria","Descrição"):
            if c in d.columns:
                d[c] = d[c].astype(str).fillna("")
        return d

    o = norm_df(orig_df)
    n = norm_df(edited_df)

    old_ids = set(int(x) for x in o["ID"].tolist() if pd.notna(x))
    new_ids = set(int(x) for x in n["ID"].tolist() if pd.notna(x) and int(x) > 0)
    to_delete = list(old_ids - new_ids)
    old_map = {int(r["ID"]): r for _, r in o.iterrows()}

    with SessionLocal() as db:
        cats = categories_for_type(db, tx_type)
        cat_by_name = {c.name: c for c in cats}

        if to_delete:
            db.query(Transaction).filter(Transaction.id.in_(to_delete)).delete(synchronize_session=False)

        for rid in sorted(new_ids & old_ids):
            old = old_map[rid]
            new = n.loc[n["ID"] == rid].iloc[0]
            changed = False
            t = db.get(Transaction, rid)
            if not t:
                continue
            if old["Data"] != new["Data"]:
                t.date = _to_date(new["Data"]); changed = True
            if str(old.get("Categoria","")) != str(new.get("Categoria","")) and "Categoria" in n.columns:
                cat = cat_by_name.get(str(new["Categoria"]))
                if cat:
                    t.category_id = cat.id; changed = True
            if float(old["Valor"]) != float(new["Valor"]):
                t.amount = float(new["Valor"]); changed = True
            if (old.get("Descrição","") or "") != (new.get("Descrição","") or ""):
                t.description = (new.get("Descrição","") or None); changed = True
            if changed:
                db.add(t)

        for _, row in n.iterrows():
            rid = row.get("ID", None)
            is_new = pd.isna(rid) or int(rid) <= 0 or int(rid) not in old_ids
            if not is_new:
                continue
            data = _to_date(row.get("Data"))
            amount = _to_float_brl(row.get("Valor"))
            desc = str(row.get("Descrição","")).strip() or None
            if "Categoria" in n.columns:
                cat_name = str(row.get("Categoria","")).strip()
                if not cat_name:
                    continue
                cat = cat_by_name.get(cat_name)
                if not cat:
                    continue
            else:
                cat = db.scalar(select(Category).where(Category.name == "Missões (Saída)"))
                if not cat:
                    continue
            cong_id = default_cong_id
            if cong_id is None:
                if not o.empty:
                    cong_id = int(o.iloc[0].get("_cong_id", 0)) or None
            if cong_id is None:
                continue
            db.add(Transaction(
                date=data, type=tx_type, category_id=cat.id, amount=float(amount),
                description=desc, congregation_id=int(cong_id)
            ))
        db.commit()

def _apply_tithe_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, default_cong_id: Optional[int]):
    def norm_df(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        if "Valor" in d.columns:
            d["Valor"] = d["Valor"].map(_to_float_brl)
        if "Data" in d.columns:
            d["Data"] = d["Data"].map(_to_date)
        for c in ("Dizimista","Forma de Pagamento"):
            if c in d.columns:
                d[c] = d[c].astype(str).fillna("")
        return d

    o = norm_df(orig_df)
    n = norm_df(edited_df)

    old_ids = set(int(x) for x in o["ID"].tolist() if pd.notna(x))
    new_ids = set(int(x) for x in n["ID"].tolist() if pd.notna(x) and int(x) > 0)
    to_delete = list(old_ids - new_ids)
    old_map = {int(r["ID"]): r for _, r in o.iterrows()}

    with SessionLocal() as db:
        if to_delete:
            db.query(Tithe).filter(Tithe.id.in_(to_delete)).delete(synchronize_session=False)

        for rid in sorted(new_ids & old_ids):
            old = old_map[rid]
            new = n.loc[n["ID"] == rid].iloc[0]
            changed = False
            t = db.get(Tithe, rid)
            if not t:
                continue
            if old["Data"] != new["Data"]:
                t.date = _to_date(new["Data"]); changed = True
            if (old["Dizimista"] or "") != (new["Dizimista"] or ""):
                t.tither_name = (new["Dizimista"] or ""); changed = True
            if float(old["Valor"]) != float(new["Valor"]):
                t.amount = float(new["Valor"]); changed = True
            if (old["Forma de Pagamento"] or "") != (new["Forma de Pagamento"] or ""):
                t.payment_method = (new["Forma de Pagamento"] or None); changed = True
            if changed:
                db.add(t)

        for _, row in n.iterrows():
            rid = row.get("ID", None)
            is_new = pd.isna(rid) or int(rid) <= 0 or int(rid) not in old_ids
            if not is_new:
                continue
            data = _to_date(row.get("Data"))
            nome = str(row.get("Dizimista","")).strip()
            amount = _to_float_brl(row.get("Valor"))
            forma = str(row.get("Forma de Pagamento","")).strip() or None
            if not nome:
                continue
            cong_id = default_cong_id
            if cong_id is None and not o.empty:
                cong_id = int(o.iloc[0].get("_cong_id", 0)) or None
            if cong_id is None:
                continue
            db.add(Tithe(
                date=data, tither_name=nome, amount=float(amount),
                congregation_id=int(cong_id), payment_method=forma
            ))
        db.commit()

# ===================== RELATÓRIO DE ENTRADA — TABELA ÚNICA (EDIT SUMÁRIO) =====================
def _entrada_summary_df(db: Session, cong_id: int, start: date, end: date) -> pd.DataFrame:
    tithes = db.execute(
        select(Tithe.date, Tithe.service_type, func.sum(Tithe.amount))
        .where(and_(Tithe.congregation_id == cong_id, Tithe.date >= start, Tithe.date < end))
        .group_by(Tithe.date, Tithe.service_type)
    ).all()
    diz_trans = db.execute(
        select(Transaction.date, Transaction.service_type, func.sum(Transaction.amount))
        .join(Category, Transaction.category_id == Category.id)
        .where(and_(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type.in_((TYPE_IN, "RECEITA")),
            func.lower(Category.name).in_(("dízimo","dizimo"))
        ))
        .group_by(Transaction.date, Transaction.service_type)
    ).all()
    oferta_trans = db.execute(
        select(Transaction.date, Transaction.service_type, func.sum(Transaction.amount))
        .join(Category, Transaction.category_id == Category.id)
        .where(and_(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type.in_((TYPE_IN, "RECEITA")),
            func.lower(Category.name) == "oferta"
        ))
        .group_by(Transaction.date, Transaction.service_type)
    ).all()

    by_key = defaultdict(lambda: {"dizimo": 0.0, "oferta": 0.0})
    for d, stype, s in tithes:
        key = (d, stype or 'Normal'); by_key[key]["dizimo"] += float(s or 0.0)
    for d, stype, s in diz_trans:
        key = (d, stype or 'Normal'); by_key[key]["dizimo"] += float(s or 0.0)
    for d, stype, s in oferta_trans:
        key = (d, stype or 'Normal'); by_key[key]["oferta"] += float(s or 0.0)

    rows = []
    for (d, stype), values in sorted(by_key.items()):
        dz = values["dizimo"]
        ofe = values["oferta"]
        rows.append({
            "Data do Culto": d,
            "EBD": (stype == 'EBD'),
            "Dízimo": dz,
            "Oferta": ofe,
            "Total": dz + ofe
        })
    return pd.DataFrame(rows)

def _apply_entrada_summary_changes(cong_id: int, start: date, end: date, edited_df: pd.DataFrame):
    with SessionLocal() as db:
        cats_in = categories_for_type(db, TYPE_IN)
        cat_diz = next((c for c in cats_in if _norm(c.name) in ("dizimo","dízimo")), None)
        cat_ofe = next((c for c in cats_in if _norm(c.name) == "oferta"), None)
        if not (cat_diz and cat_ofe):
            st.error("Categorias 'Dízimo' e/ou 'Oferta' não encontradas."); return

        baseline_df = _entrada_summary_df(db, cong_id, start, end)
        baseline_df["service_type"] = baseline_df["EBD"].apply(lambda x: 'EBD' if x else None)
        baseline = {(r["Data do Culto"], r["service_type"]): (float(r["Dízimo"]), float(r["Oferta"])) for _, r in baseline_df.iterrows()}

        edited = edited_df.copy()
        edited["Data do Culto"] = edited["Data do Culto"].map(_to_date)
        edited["service_type"] = edited["EBD"].apply(lambda x: 'EBD' if x else None)
        wanted = {(r["Data do Culto"], r["service_type"]): (float(_to_float_brl(r["Dízimo"])), float(_to_float_brl(r["Oferta"]))) for _, r in edited.iterrows()}

        all_keys = sorted(set(list(baseline.keys()) + list(wanted.keys())))
        for d, service_type in all_keys:
            cur_dz, cur_of = baseline.get((d, service_type), (0.0, 0.0))
            want_dz, want_of = wanted.get((d, service_type), (0.0, 0.0))

            sum_dz_others = float(db.scalar(
                select(func.coalesce(func.sum(Transaction.amount), 0.0))
                .where(and_(
                    Transaction.congregation_id == cong_id, Transaction.date == d,
                    Transaction.service_type == service_type,
                    Transaction.category_id == cat_diz.id,
                    func.coalesce(Transaction.description, "") != ADJ_ENTRY_DESC
                ))
            ) or 0.0) + float(db.scalar(
                select(func.coalesce(func.sum(Tithe.amount), 0.0))
                .where(and_(
                    Tithe.congregation_id == cong_id, Tithe.date == d,
                    Tithe.service_type == service_type
                ))
            ) or 0.0)

            sum_of_others = float(db.scalar(
                select(func.coalesce(func.sum(Transaction.amount), 0.0))
                .where(and_(
                    Transaction.congregation_id == cong_id, Transaction.date == d,
                    Transaction.service_type == service_type,
                    Transaction.category_id == cat_ofe.id,
                    func.coalesce(Transaction.description, "") != ADJ_ENTRY_DESC
                ))
            ) or 0.0)

            adj_dz_new = want_dz - sum_dz_others
            adj_of_new = want_of - sum_of_others

            adj_dz = db.scalar(select(Transaction).where(
                Transaction.congregation_id == cong_id, Transaction.date == d,
                Transaction.service_type == service_type,
                Transaction.category_id == cat_diz.id,
                func.coalesce(Transaction.description, "") == ADJ_ENTRY_DESC
            ))
            adj_of = db.scalar(select(Transaction).where(
                Transaction.congregation_id == cong_id, Transaction.date == d,
                Transaction.service_type == service_type,
                Transaction.category_id == cat_ofe.id,
                func.coalesce(Transaction.description, "") == ADJ_ENTRY_DESC
            ))

            if abs(adj_dz_new) < 0.0001:
                if adj_dz: db.delete(adj_dz)
            else:
                if adj_dz: adj_dz.amount = float(adj_dz_new)
                else: adj_dz = Transaction(
                        date=d, type=TYPE_IN, category_id=cat_diz.id, amount=float(adj_dz_new),
                        description=ADJ_ENTRY_DESC, congregation_id=cong_id, service_type=service_type
                    )
                db.add(adj_dz)

            if abs(adj_of_new) < 0.0001:
                if adj_of: db.delete(adj_of)
            else:
                if adj_of: adj_of.amount = float(adj_of_new)
                else: adj_of = Transaction(
                        date=d, type=TYPE_IN, category_id=cat_ofe.id, amount=float(adj_of_new),
                        description=ADJ_ENTRY_DESC, congregation_id=cong_id, service_type=service_type
                    )
                db.add(adj_of)
        db.commit()


# ===================== EDITORES INLINE REUTILIZÁVEIS =====================
def _editor_lancamentos(transactions: List["Transaction"], titulo: str, tx_type_hint: Optional[str] = None):
    if not transactions:
        st.caption("— Não há lançamentos para esse filtro.")
        return

    cong_ids = {int(t.congregation_id) for t in transactions if t.congregation_id}
    default_cong_id = list(cong_ids)[0] if len(cong_ids) == 1 else None
    tx_type = tx_type_hint or (transactions[0].type if transactions else TYPE_IN)

    rows = []
    for t in transactions:
        rows.append({
            "ID": t.id,
            "Data": t.date,
            "Categoria": (t.category.name if t.category else ""),
            "Valor": float(t.amount),
            "Descrição": t.description or "",
            "_cong_id": int(t.congregation_id or 0),
        })
    df_full = pd.DataFrame(rows)
    df_view = df_full.drop(columns=["_cong_id"])

    with SessionLocal() as db:
        cats = categories_for_type(db, tx_type)
        cat_names = [c.name for c in cats] or ["—"]

    st.markdown(f"**{titulo}**")
    allow_add = default_cong_id is not None
    edited_view = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        num_rows=("dynamic" if allow_add else "fixed"),
        column_config={
            "ID": st.column_config.Column("ID", disabled=True),
            "Data": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
            "Categoria": st.column_config.SelectboxColumn("Categoria", options=cat_names, required=True),
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
            "Descrição": st.column_config.TextColumn("Descrição", max_chars=200),
        },
        key=f"tx_editor_{titulo}",
    )

    _auto_save_if_changed(
        df_view, edited_view,
        lambda: _apply_tx_changes(df_full, edited_view, tx_type, default_cong_id)
    )

def _editor_dizimos(tithes: List["Tithe"], titulo: str):
    if not tithes:
        st.caption("— Não há dízimos neste período.")
        return

    cong_ids = {int(t.congregation_id) for t in tithes if t.congregation_id}
    default_cong_id = list(cong_ids)[0] if len(cong_ids) == 1 else None

    rows = [{
        "ID": t.id,
        "Data": t.date,
        "Dizimista": t.tither_name,
        "Valor": float(t.amount),
        "Forma de Pagamento": t.payment_method or "",
        "_cong_id": int(t.congregation_id or 0),
    } for t in tithes]
    df_full = pd.DataFrame(rows)
    df_view = df_full.drop(columns=["_cong_id"])

    st.markdown(f"**{titulo}**")
    allow_add = default_cong_id is not None
    edited_view = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        num_rows=("dynamic" if allow_add else "fixed"),
        column_config={
            "ID": st.column_config.Column("ID", disabled=True),
            "Data": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
            "Dizimista": st.column_config.TextColumn("Dizimista", max_chars=120, required=True),
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
            "Forma de Pagamento": st.column_config.SelectboxColumn("Forma de Pagamento", options=["Dinheiro","PIX","Cartão","Transferência",""], required=False),
        },
        key=f"tithe_editor_{titulo}",
    )

    _auto_save_if_changed(
        df_view, edited_view,
        lambda: _apply_tithe_changes(df_full, edited_view, default_cong_id)
    )

# ===== MISSÕES: Editores específicos =====
def _editor_missions_outflows(saidas: List["Transaction"], titulo: str):
    rows = [{
        "ID": t.id,
        "Data": t.date,
        "Descrição": t.description or "",
        "Valor": float(t.amount),
        "_cong_id": int(t.congregation_id or 0),
    } for t in saidas]
    df_full = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["ID", "Data", "Descrição", "Valor", "_cong_id"])
    df_view = df_full.drop(columns=["_cong_id"])

    edited_view = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "ID": st.column_config.Column("ID", disabled=True),
            "Data": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
            "Descrição": st.column_config.TextColumn("Descrição", max_chars=200),
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
        },
        key=f"missoes_out_{titulo}",
    )

    def _save():
        with SessionLocal() as db:
            sede_id = db.scalar(select(Congregation.id).where(Congregation.name == "Sede"))
        _apply_tx_changes(
            df_full.assign(**{"Categoria": "Missões (Saída)"}),
            edited_view.assign(**{"Categoria": "Missões (Saída)"}),
            TYPE_OUT,
            default_cong_id=sede_id
        )

    _auto_save_if_changed(df_view, edited_view, _save)

def _editor_missions_entries_agg(congs_all: List["Congregation"], start: date, end: date, titulo: str):
    with SessionLocal() as db:
        q = select(Transaction.congregation_id, func.sum(Transaction.amount))\
            .join(Category, Transaction.category_id == Category.id)\
            .where(
                Transaction.date >= start, Transaction.date < end,
                Transaction.type == TYPE_IN,
                func.lower(Category.name).in_(("missões","missoes"))
            ).group_by(Transaction.congregation_id)
        sums = dict((int(cid), float(val or 0.0)) for cid, val in db.execute(q).all())

    rows = []
    for c in congs_all:
        val = float(sums.get(c.id, 0.0))
        rows.append({"Congregação": c.name, "Valor": val, "_cong_id": c.id})
    df_full = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["Congregação","Valor","_cong_id"])
    df_view = df_full.drop(columns=["_cong_id"]) if not df_full.empty else pd.DataFrame(columns=["Congregação","Valor"])

    edited_view = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "Congregação": st.column_config.SelectboxColumn("Congregação", options=[c.name for c in order_congs_sede_first(congs_all)], required=True),
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
        },
        key=f"missoes_in_agg_{titulo}",
    )

    def _save():
        with SessionLocal() as db:
            by_name = {c.name: c.id for c in congs_all}
            cats_in = categories_for_type(db, TYPE_IN)
            cat_miss = next((c for c in cats_in if _norm(c.name) in ("missoes","missões")), None)
            if not cat_miss:
                st.error("Categoria 'Missões' não encontrada."); return

            q_others = select(Transaction.congregation_id, func.coalesce(func.sum(Transaction.amount),0.0))\
                .join(Category, Transaction.category_id == Category.id)\
                .where(
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.type == TYPE_IN,
                    Transaction.category_id == cat_miss.id,
                    func.coalesce(Transaction.description, "") != ADJ_MISS_IN_DESC
                ).group_by(Transaction.congregation_id)
            base_others = dict((int(cid), float(v)) for cid, v in db.execute(q_others).all())

            q_adj = select(Transaction).where(
                Transaction.date == start,
                Transaction.type == TYPE_IN,
                Transaction.category_id == cat_miss.id,
                func.coalesce(Transaction.description, "") == ADJ_MISS_IN_DESC
            )
            adjs = db.scalars(q_adj).all()
            adj_map = {(a.congregation_id): a for a in adjs}

            desired = defaultdict(float)
            for _, r in edited_view.iterrows():
                name = str(r.get("Congregação","")).strip()
                if not name:
                    continue
                cid = by_name.get(name)
                if not cid:
                    continue
                desired[cid] += float(_to_float_brl(r.get("Valor", 0.0)))

            all_cids = set(list(base_others.keys()) + list(desired.keys()) + list(adj_map.keys()))
            for cid in all_cids:
                want = float(desired.get(cid, 0.0))
                others = float(base_others.get(cid, 0.0))
                new_adj = want - others
                exist = adj_map.get(cid)
                if abs(new_adj) < 0.0001:
                    if exist:
                        db.delete(exist)
                else:
                    if exist:
                        exist.amount = float(new_adj); db.add(exist)
                    else:
                        db.add(Transaction(
                            date=start, type=TYPE_IN, category_id=cat_miss.id, amount=float(new_adj),
                            description=ADJ_MISS_IN_DESC, congregation_id=int(cid)
                        ))
            db.commit()

    _auto_save_if_changed(df_view, edited_view, _save)

# ===================== CORE COLETA =====================
def _collect_month_data(db, cong_id: int, start: date, end: date, is_all: bool = False):
    tx_in_query = select(Transaction).options(joinedload(Transaction.category)).where(
        Transaction.date >= start, Transaction.date < end, Transaction.type.in_(("DOAÇÃO", "RECEITA"))
    ).order_by(Transaction.date)
    if not is_all:
        tx_in_query = tx_in_query.where(Transaction.congregation_id == cong_id)
    tx_in = db.scalars(tx_in_query).all()

    tithes_query = select(Tithe).where(Tithe.date >= start, Tithe.date < end).order_by(Tithe.date)
    if not is_all:
        tithes_query = tithes_query.where(Tithe.congregation_id == cong_id)
    tithes = db.scalars(tithes_query).all()

    tx_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
        Transaction.date >= start, Transaction.date < end, Transaction.type.in_(("SAÍDA", "DESPESA"))
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

    total_dizimos = sum(float(t.amount) for t in tithes) + sum(float(t.amount) for t in tx_in if _is_dizimo_tx(t))
    total_ofertas = sum(float(t.amount) for t in tx_in if _is_oferta_tx(t))
    total_missoes = sum(float(t.amount) for t in tx_in if _is_mission_entry(t))

    entradas_excluidas = ("dizimo", "dízimo", "oferta", "missoes", "missões")
    total_entradas_outros = sum(
        float(t.amount) for t in tx_in if t.category and _norm(t.category.name) not in entradas_excluidas
    )

    total_geral_entradas = total_dizimos + total_ofertas + total_missoes + total_entradas_outros
    total_saidas = sum(float(t.amount) for t in tx_out)
    saldo = total_geral_entradas - total_saidas

    return {
        "tx_in": tx_in, "tithes": tithes, "tx_out": tx_out,
        "totals": {
            "dizimos": total_dizimos, "ofertas": total_ofertas, "missoes": total_missoes,
            "entradas_outros": total_entradas_outros, "entradas_total": total_geral_entradas,
            "saidas_total": total_saidas, "saldo": saldo
        }
    }

# ===================== PAGE: LANÇAMENTOS =====================
def page_lancamentos(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)
        st.markdown(f"<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)

        congs = cong_options_for(user, db)
        if not congs:
            st.info("Nenhuma congregação disponível."); return

        cong_obj = congs[0]
        if user.role == "SEDE":
            congs_ordered = order_congs_sede_first(congs)
            cong_sel_name = st.selectbox("Selecione a congregação", [c.name for c in congs_ordered], key="lan_cong_sel")
            cong_obj = next(c for c in congs_ordered if c.name == cong_sel_name)

        st.markdown(f"<div class='cong-title'>CONGREGAÇÃO: {cong_obj.name.upper()}</div>", unsafe_allow_html=True)

        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar ENTRADA (Doação)")
        with st.form("form_entrada", clear_on_submit=True):
            ent_data = st.date_input("Data do Culto", value=today_bahia(), key="ent_data", format="DD/MM/YYYY")
            c1,c2 = st.columns(2)
            with c1:
                cats_in = [c.name for c in categories_for_type(db, TYPE_IN)]
                ent_cat = st.selectbox("Categoria", cats_in, key="ent_cat")
            with c2:
                ent_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="ent_valor")
            ent_desc = st.text_input("Descrição (opcional)", key="ent_desc")
            ent_ebd = st.checkbox("Culto EBD?", key="ent_ebd_flag")

            if st.form_submit_button("Salvar ENTRADA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name == ent_cat))
                    if not cat_obj: st.error("Informe a categoria."); return
                    _db.add(Transaction(
                        date=ent_data, type=TYPE_IN, category_id=cat_obj.id,
                        amount=ent_valor, description=(ent_desc or None),
                        congregation_id=cong_obj.id, payment_method=None,
                        service_type='EBD' if ent_ebd else None
                    ))
                    _db.commit()
                    st.success("Entrada registrada.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Salvar DIZIMISTA")
        with st.form("form_dizimo", clear_on_submit=True):
            dz_data = st.date_input("Data do Culto", value=today_bahia(), key="dz_data", format="DD/MM/YYYY")
            c1,c2 = st.columns(2)
            with c1:
                dz_nome = st.text_input("Nome do dizimista", key="dz_nome")
            with c2:
                dz_valor = st.number_input("Valor dízimo (R$)", min_value=0.0, step=1.0, format="%.2f", key="dz_valor")
            dz_payment = st.selectbox("Forma de Pagamento", ["Dinheiro", "PIX", "Cartão", "Transferência"], key="dz_payment_method")
            dz_ebd = st.checkbox("Culto EBD?", key="dz_ebd_flag")

            if st.form_submit_button("Salvar DIZIMISTA", type="primary"):
                nome = (dz_nome or "").strip()
                if not nome: st.error("Informe o nome do dizimista."); return
                with SessionLocal() as _db:
                    _db.add(Tithe(
                        date=dz_data, tither_name=nome, amount=float(dz_valor),
                        congregation_id=cong_obj.id, payment_method=dz_payment,
                        service_type='EBD' if dz_ebd else None
                    ))
                    _db.commit()
                    st.success("Dízimo registrado.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar SAÍDA")
        with st.form("form_saida", clear_on_submit=True):
            sai_data = st.date_input("Data", value=today_bahia(), key="sai_data", format="DD/MM/YYYY")
            cats_out = [c.name for c in categories_for_type(db, TYPE_OUT)]
            sai_cat = st.selectbox("Tipo da saída (Categoria)", cats_out, key="sai_cat")
            sai_desc = st.text_input("Descrição (opcional)", key="sai_desc")
            sai_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="sai_valor")

            if st.form_submit_button("Salvar SAÍDA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name == sai_cat))
                    if not cat_obj: st.error("Informe o tipo de saída."); return
                    _db.add(Transaction(
                        date=sai_data, type=TYPE_OUT, category_id=cat_obj.id,
                        amount=sai_valor, description=(sai_desc or None),
                        congregation_id=cong_obj.id,
                    ))
                    _db.commit()
                    st.success("Saída registrada.")
        st.markdown('</div>', unsafe_allow_html=True)

# ===================== PAGE: RELATÓRIO DE ENTRADA (TABELA ÚNICA) =====================
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
            cong_sel = st.selectbox("Congregação", [c.name for c in ordered], key="re_cong_only")
            cong_obj = next(c for c in ordered if c.name == cong_sel)
        else:
            cong_obj = congs[0] if congs else None

        if not cong_obj:
            st.info("Selecione uma congregação."); return
        st.info(f"Escopo: **{cong_obj.name}**")

        base_df = _entrada_summary_df(db, cong_obj.id, start, end)
        if base_df.empty:
            base_df = pd.DataFrame(columns=["Data do Culto", "EBD", "Dízimo", "Oferta", "Total"])
        view_df = base_df.copy()

        edited = st.data_editor(
            view_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Data do Culto": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
                "EBD": st.column_config.CheckboxColumn("EBD?", default=False),
                "Dízimo": st.column_config.NumberColumn("Dízimo (R$)", min_value=0.0, step=1.0, format="R$ %.2f", required=True),
                "Oferta": st.column_config.NumberColumn("Oferta (R$)", min_value=0.0, step=1.0, format="R$ %.2f", required=True),
                "Total": st.column_config.NumberColumn("Total (R$)", disabled=True, format="R$ %.2f"),
            },
            key="re_entrada_sum_editor",
        )

        if not edited.empty:
            try:
                edited["Total"] = edited["Dízimo"].map(_to_float_brl) + edited["Oferta"].map(_to_float_brl)
            except Exception:
                pass

        _auto_save_if_changed(
            view_df, edited,
            lambda: _apply_entrada_summary_changes(cong_obj.id, start, end, edited)
        )

        st.divider()
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Totais do Mês (conforme tabela acima)")
        total_dizimo = float(edited["Dízimo"].map(_to_float_brl).sum())
        total_oferta = float(edited["Oferta"].map(_to_float_brl).sum())
        total_geral = total_dizimo + total_oferta
        c1, c2, c3 = st.columns(3)
        c1.metric("Total de Dízimos", format_currency(total_dizimo))
        c2.metric("Total de Ofertas", format_currency(total_oferta))
        c3.metric("Total Geral de Entradas", format_currency(total_geral))
        st.markdown('</div>', unsafe_allow_html=True)

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

        q = select(Transaction).options(joinedload(Transaction.category), joinedload(Transaction.congregation)).where(
            Transaction.date >= start, Transaction.date < end, Transaction.type.in_(("SAÍDA", "DESPESA"))
        )
        if not is_all:
            q = q.where(Transaction.congregation_id == cong_obj.id)
        txs = db.scalars(q.order_by(Transaction.date)).all()

        st.divider()
        if is_all:
            st.info("Edição inline de saídas habilitada ao escolher uma congregação específica.")
            if txs:
                df_view = pd.DataFrame([{
                    "Data": t.date,
                    "Congregação": t.congregation.name if t.congregation else "N/A",
                    "Categoria": t.category.name if t.category else "N/A",
                    "Valor": t.amount,
                    "Descrição": t.description or ""
                } for t in txs])
                st.dataframe(df_view.assign(Valor=df_view["Valor"].map(format_currency)), use_container_width=True, hide_index=True)
        else:
            _editor_lancamentos(txs, "Saídas do período (editar na tabela)", tx_type_hint=TYPE_OUT)
        
        st.markdown("---")
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Total de Saídas no Período")
        total_saidas = sum(float(t.amount) for t in txs)
        st.metric("Valor Total", format_currency(total_saidas))
        st.markdown('</div>', unsafe_allow_html=True)

        st.divider()
        rows_csv = [{
            "Data": t.date.strftime("%Y-%m-%d"),
            "Congregação": t.congregation.name,
            "Tipo da saída": t.category.name,
            "Valor": float(t.amount),
            "Descrição": t.description or ""
        } for t in txs]
        csv = pd.DataFrame(rows_csv).to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Baixar CSV das SAÍDAS do período",
            data=csv, file_name=f"saidas_{start.strftime('%Y-%m')}.csv", mime="text/csv"
        )


# ===================== PAGE: RELATÓRIO DE DIZIMISTAS =====================
def build_dizimista_search_pdf(df: pd.DataFrame, ano_pesq: int, cong_sel: str, mes_sel: str, nome_q: str) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)

    story = []
    story.append(Paragraph("Relatório de Pesquisa de Dizimistas", title_style))
    story.append(Paragraph(f"Ano: {ano_pesq} | Congregação: {cong_sel} | Mês: {mes_sel}", subtitle_style))
    if (nome_q or "").strip():
        story.append(Paragraph(f"Filtrado por: '{nome_q}'", subtitle_style))
    story.append(Spacer(1, 0.5*cm))

    df_pdf = df.copy()
    total_value = float(df_pdf["Total no ano (R$)"].sum())
    
    df_pdf["Total no ano (R$)"] = df_pdf["Total no ano (R$)"].apply(format_currency)

    data_table = [df_pdf.columns.tolist()] + df_pdf.values.tolist()
    total_row = ["", "", "", "Total Geral:", format_currency(total_value), "", ""]
    data_table.append(total_row)
    
    tbl = Table(data_table, colWidths=[3.5*cm, 3.5*cm, 2.0*cm, 2.5*cm, 2.5*cm, 2.0*cm, 2.0*cm])
    tbl.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (4, 1), (4, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(f"Dizimistas encontrados: **{len(df)}**", styles['Normal']))
    story.append(Paragraph(f"Total geral da pesquisa: **{format_currency(total_value)}**", styles['Normal']))

    doc.build(story)
    return buf.getvalue()

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
            all_tz = db.scalars(select(Tithe).options(joinedload(Tithe.congregation)).where(Tithe.date >= start, Tithe.date < end)).all()
            by_cong = defaultdict(lambda: {"qtd":0, "valor":0.0})
            for t in all_tz:
                k = t.congregation.name
                by_cong[k]["qtd"] += 1
                by_cong[k]["valor"] += float(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Qtde de dízimos": v["qtd"], "Total (R$)": v["valor"]} for k,v in sorted(by_cong.items())])
            if not df.empty:
                st.dataframe(df.assign(**{"Total (R$)": df["Total (R$)"].map(format_currency)}), use_container_width=True, hide_index=True)
            st.info("Selecione uma congregação específica para ver a lista nominal e editar.")
        else:
            tithes = db.scalars(select(Tithe).where(
                Tithe.date >= start, Tithe.date < end, Tithe.congregation_id == cong_obj.id
            ).order_by(Tithe.date)).all()

            tithes_by_payment = defaultdict(lambda: {"count": 0, "total": 0.0})
            for tithe in tithes:
                method = (tithe.payment_method or "Não Informado").upper()
                tithes_by_payment[method]["count"] += 1
                tithes_by_payment[method]["total"] += float(tithe.amount)

            if tithes_by_payment:
                st.subheader("Resumo de Pagamentos de Dízimos")
                cols_metrics = st.columns(len(tithes_by_payment))
                for i, (method, datax) in enumerate(tithes_by_payment.items()):
                    with cols_metrics[i]:
                        st.metric(f"Total ({method})", format_currency(datax["total"]), f"{datax['count']} dízimos")
                st.divider()

            _editor_dizimos(tithes, "Dizimistas do período (editar na tabela)")

        st.divider()
        st.subheader("Pesquisa de Dizimistas (por Ano)")
        c1, c2, c3, c4, c5 = st.columns([1.2, 1.8, 1.4, 2.2, 1.6])
        with c1:
            ano_pesq = st.number_input("Ano", value=today_bahia().year, step=1, format="%d", key="srch_year")
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
        with c5:
            only_pix = st.checkbox("Somente PIX", value=False, key="srch_only_pix")

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

        if (nome_q or "").strip():
            nneedle = _norm(nome_q)
            t_list = [t for t in t_list if nneedle in _norm(t.tither_name)]

        if only_pix:
            t_list = [t for t in t_list if (t.payment_method or "").strip().upper() == "PIX"]

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
            df_show = df_pesq.sort_values(["Qtde de meses no ano","Dizimista"], ascending=[False, True]).reset_index(drop=True)
            st.dataframe(
                df_show.assign(**{"Total no ano (R$)": df_show["Total no ano (R$)"].map(lambda x: format_currency(float(x)))}),
                use_container_width=True, hide_index=True, height=320
            )
            tot_reg = len(df_show)
            tot_val = float(df_show["Total no ano (R$)"].sum())
            cA, cB, cC = st.columns(3)
            cA.metric("Dizimistas encontrados", f"{tot_reg}")
            cB.metric("Total geral da pesquisa", format_currency(tot_val))

            pix_names = sorted({t.tither_name for t in t_list if (t.payment_method or "").strip().upper() == "PIX"})
            cC.metric("Dizimaram por PIX (únicos)", f"{len(pix_names)}")
            if pix_names:
                with st.expander("Ver nomes que dizimaram por PIX (neste filtro)"):
                    st.write(", ".join(pix_names))

            pdf_data = build_dizimista_search_pdf(df_show, ano_pesq, cong_sel, mes_sel, nome_q)
            cDown1, cDown2 = st.columns(2)
            with cDown1:
                csv = df_show.to_csv(index=False).encode("utf-8-sig")
                st.download_button("⬇️ Baixar CSV da pesquisa", data=csv, file_name=f"pesquisa_dizimistas_{ano_pesq}.csv", mime="text/csv", use_container_width=True)
            with cDown2:
                st.download_button("⬇️ Baixar PDF da pesquisa", data=pdf_data, file_name=f"pesquisa_dizimistas_{ano_pesq}.pdf", mime="application/pdf", use_container_width=True)
        else:
            st.caption("Nenhum resultado para os filtros informados.")

# ===================== PDFs (prestação e consolidado) =====================
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

        entries_summary = _entrada_summary_df(db, cong_id, start, end)
        tx_in_data = [["Data", "Culto", "Dízimo", "Oferta", "Total"]]
        if not entries_summary.empty:
            for _, row in entries_summary.iterrows():
                culto_tipo = "EBD" if row["EBD"] else "Normal"
                tx_in_data.append([row["Data do Culto"].strftime("%d/%m/%Y"), culto_tipo, format_currency(row["Dízimo"]), format_currency(row["Oferta"]), format_currency(row["Total"])])

        tx_out_data = [["Data", "Categoria", "Descrição", "Valor"]]
        tx_out_data.extend([[t.date.strftime("%d/%m/%Y"), t.category.name, t.description or "", format_currency(t.amount)] for t in data["tx_out"]])

    story.append(Paragraph("Prestação de Contas Mensal", title_style))
    story.append(Paragraph(f"Congregação: {cong_name}", subtitle_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))

    story.append(Paragraph("1. Entradas", heading_style))
    if len(tx_in_data) > 1:
        tbl_in = Table(tx_in_data, colWidths=[2.5*cm, 2.5*cm, 4.0*cm, 4.0*cm, 4.0*cm])
        tbl_in.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"), ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada registrada.", styles['Normal']))
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("2. Saídas", heading_style))
    if len(tx_out_data) > 1:
        tbl_out = Table(tx_out_data, colWidths=[2.5*cm, 4.5*cm, 7.0*cm, 3.0*cm])
        tbl_out.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (3, 1), (3, -1), "RIGHT"), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ]))
        story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída registrada.", styles['Normal']))
    story.append(Spacer(1, 1*cm))

    story.append(Paragraph("3. Resumo Financeiro do Mês", heading_style))
    with SessionLocal() as db:
        start, end = month_bounds(ref)
        totals = _collect_month_data(db, cong_id, start, end)["totals"]
    
    total_entradas_sem_missoes = totals['entradas_total'] - totals['missoes']
    saldo_sem_missoes = total_entradas_sem_missoes - totals['saidas_total']
    
    summary_data = [
        ["Total de Dízimos", format_currency(totals["dizimos"])],
        ["Total de Ofertas", format_currency(totals.get("ofertas", 0.0))],
        ["Outras Entradas", format_currency(totals.get("entradas_outros", 0.0))],
        ["Total de Entradas (Caixa Principal)", format_currency(total_entradas_sem_missoes)],
        ["Total de Saídas", format_currency(totals["saidas_total"])],
        ["Saldo do Mês (Caixa Principal)", format_currency(saldo_sem_missoes)],
    ]
    summary_table = Table(summary_data, colWidths=[8*cm, 8*cm])
    summary_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e2fbe2")),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
    ]))
    story.append(summary_table)

    doc.build(story)
    return buf.getvalue()

def build_consolidated_pdf(agg_total: list, ref: date) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)
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

    story: List = []
    story.append(Paragraph("Relatório Mensal Consolidado", title_style))
    story.append(Paragraph(f"Mês de Referência: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 1*cm))

    table_data = [["Congregação", "Entradas", "Saídas", "Saldo"]]
    total_entradas = total_saidas = total_saldo = 0.0

    for c_name, entradas, saidas, saldo in agg_total:
        table_data.append([c_name, format_currency(entradas), format_currency(saidas), format_currency(saldo)])
        total_entradas += entradas; total_saidas += saidas; total_saldo += saldo

    table_data.append(["TOTAL GERAL", format_currency(total_entradas), format_currency(total_saidas), format_currency(total_saldo)])
    tbl = Table(table_data, colWidths=[5*cm, 4*cm, 4*cm, 4*cm]); tbl.setStyle(table_style_main)
    story.append(tbl)

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

# ===================== NOVA FUNÇÃO: Aplicar ajustes da Visão Geral (Sede) =====================
def _apply_sede_summary_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, ref_date: date, change_type: str):
    if change_type not in ('entrada', 'saida'): return
    
    col_name = "Total de Entrada" if change_type == 'entrada' else "Total de Saída"
    adj_desc = ADJ_SEDE_IN_DESC if change_type == 'entrada' else ADJ_SEDE_OUT_DESC
    cat_name = "Ajuste Sede (Entrada)" if change_type == 'entrada' else "Ajuste Sede (Saída)"
    tx_type = TYPE_IN if change_type == 'entrada' else TYPE_OUT
    
    with SessionLocal() as db:
        adj_cat = db.scalar(select(Category).where(Category.name == cat_name))
        if not adj_cat:
            st.error(f"Categoria de ajuste '{cat_name}' não encontrada. Verifique o cadastro."); return
        
        congs_map = {c.name: c.id for c in db.scalars(select(Congregation))}
        start, end = month_bounds(ref_date)

        current_adjs = {
            t.congregation_id: t for t in db.scalars(select(Transaction).where(
                Transaction.date >= start, Transaction.date < end,
                Transaction.description == adj_desc,
                Transaction.category_id == adj_cat.id
            ))
        }
        
        orig_map = {r["Congregação"]: _to_float_brl(r[col_name]) for _, r in orig_df.iterrows()}
        
        for _, row in edited_df.iterrows():
            cong_name = row["Congregação"]
            cong_id = congs_map.get(cong_name)
            if not cong_id: continue

            old_total = orig_map.get(cong_name, 0.0)
            new_total = _to_float_brl(row[col_name])

            if abs(old_total - new_total) < 0.01: continue
            
            old_adj_val = current_adjs.get(cong_id).amount if cong_id in current_adjs else 0.0
            base_total = old_total - old_adj_val
            new_adj_val = new_total - base_total
            
            adj = current_adjs.get(cong_id)
            if abs(new_adj_val) < 0.01:
                if adj: db.delete(adj)
            else:
                if adj:
                    adj.amount = new_adj_val
                else:
                    adj = Transaction(
                        date=start, type=tx_type, category_id=adj_cat.id,
                        amount=new_adj_val, description=adj_desc, congregation_id=cong_id
                    )
                db.add(adj)
        db.commit()

# ===================== PAGE: VISÃO GERAL =====================
def page_visao_geral(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)
        
        congs = cong_options_for(user, db)
        ordered_congs = order_congs_sede_first(congs)
        
        agg_data = []
        target_congs = ordered_congs if user.role == "SEDE" else (congs if congs else [])

        for c in target_congs:
            totals = _collect_month_data(db, c.id, start, end)["totals"]
            total_entradas_caixa = totals['entradas_total'] - totals['missoes']
            saidas_missoes = float(db.scalar(select(func.sum(Transaction.amount)).where(
                Transaction.congregation_id == c.id, Transaction.date >= start, Transaction.date < end,
                Transaction.category.has(Category.name.like('%Missões%')), Transaction.type == TYPE_OUT
            )) or 0.0)
            total_saidas_caixa = totals['saidas_total'] - saidas_missoes
            saldo_caixa = total_entradas_caixa - total_saidas_caixa
            agg_data.append({
                "cong_id": c.id, "Congregação": c.name,
                "entradas": total_entradas_caixa, "saidas": total_saidas_caixa, "saldo": saldo_caixa
            })

        if user.role == "SEDE":
            st.info("Escopo: **Todas as congregações**. Os valores abaixo referem-se ao Caixa Principal (exclui Missões).")
            if not agg_data:
                st.caption("Sem dados neste mês.")
                return

            df_agg = pd.DataFrame(agg_data)

            st.subheader("📈 Ranking de Entradas")
            df_in = df_agg[['Congregação', 'entradas']].copy().rename(columns={'entradas': 'Total de Entrada'})
            df_in_sorted = df_in.sort_values("Total de Entrada", ascending=False).reset_index(drop=True)
            
            edited_in = st.data_editor(
                df_in_sorted, use_container_width=True, hide_index=True,
                column_config={
                    "Congregação": st.column_config.TextColumn("Congregação", disabled=True),
                    "Total de Entrada": st.column_config.NumberColumn("Total de Entrada (R$)", format="R$ %.2f")
                }, key="editor_entradas_sede"
            )
            _auto_save_if_changed(df_in_sorted, edited_in, _apply_sede_summary_changes, edited_in, ref, 'entrada')

            st.subheader("📉 Ranking de Saídas")
            df_out = df_agg[['Congregação', 'saidas']].copy().rename(columns={'saidas': 'Total de Saída'})
            df_out_sorted = df_out.sort_values("Total de Saída", ascending=False).reset_index(drop=True)

            edited_out = st.data_editor(
                df_out_sorted, use_container_width=True, hide_index=True,
                column_config={
                    "Congregação": st.column_config.TextColumn("Congregação", disabled=True),
                    "Total de Saída": st.column_config.NumberColumn("Total de Saída (R$)", format="R$ %.2f")
                }, key="editor_saidas_sede"
            )
            _auto_save_if_changed(df_out_sorted, edited_out, _apply_sede_summary_changes, edited_out, ref, 'saida')

            st.divider()
            st.subheader("Relatório Consolidado Mensal")
            pdf_data_consolidado = build_consolidated_pdf([(r['Congregação'], r['entradas'], r['saidas'], r['saldo']) for r in agg_data], ref)
            st.download_button( "⬇️ Baixar PDF do Relatório Geral", data=pdf_data_consolidado, file_name=f"relatorio_mensal_{start.strftime('%Y-%m')}.pdf", mime="application/pdf" )
        
        else:
            if not agg_data:
                st.info("Sem dados para sua congregação neste período."); return
            
            cong_data = agg_data[0]
            st.info(f"Escopo: **{cong_data['Congregação']}** (Caixa Principal, exclui Missões)")
            st.divider()
            st.subheader("Resumo Financeiro Mensal")
            c1, c2, c3 = st.columns(3)
            c1.metric("Total de Entradas", format_currency(cong_data['entradas']))
            c2.metric("Total de Saídas", format_currency(cong_data['saidas']))
            c3.metric("Saldo do Mês", format_currency(cong_data['saldo']))

        st.divider()
        st.subheader("Prestação de contas (PDF completo)")
        if user.role == "SEDE":
            sel = st.selectbox("Selecione a congregação para gerar o PDF", [c['Congregação'] for c in agg_data], key="pc_cong_sel")
            sel_data = next((c for c in agg_data if c['Congregação'] == sel), None)
            if sel_data:
                st.download_button(
                    f"⬇️ Baixar PDF de {sel_data['Congregação']}",
                    data=build_full_statement_pdf(sel_data['cong_id'], sel_data['Congregação'], ref),
                    file_name=f"prestacao_{_norm(sel_data['Congregação'])}_{start.strftime('%Y-%m')}.pdf",
                    mime="application/pdf"
                )
        elif agg_data:
            cong_data = agg_data[0]
            st.download_button(
                "⬇️ Baixar PDF do mês (completo)",
                data=build_full_statement_pdf(cong_data['cong_id'], cong_data['Congregação'], ref),
                file_name=f"prestacao_{_norm(cong_data['Congregação'])}_{start.strftime('%Y-%m')}.pdf",
                mime="application/pdf"
            )

# ===================== RELATÓRIO DE MISSÕES (Entradas/ Saídas editáveis) =====================
def _collect_missions_data(db: Session, start: date, end: date):
    q_in = select(Transaction).options(joinedload(Transaction.congregation), joinedload(Transaction.category)).where(
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type == TYPE_IN,
        Transaction.category.has(Category.name.in_(("Missões", "missões")))
    ).order_by(Transaction.date)
    entradas_missoes = db.scalars(q_in).all()
    
    q_out = select(Transaction).options(joinedload(Transaction.congregation), joinedload(Transaction.category)).where(
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type == TYPE_OUT,
        Transaction.category.has(Category.name.in_(("Missões (Saída)", "missões (saída)")))
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
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ])

    story: List = []
    story.append(Paragraph("Relatório Mensal de Missões", title_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("Entradas de Missões (por data)", heading_style))
    if entradas:
        entradas_data = [["Data", "Congregação", "Valor (R$)"]]
        for t in entradas:
            entradas_data.append([t.date.strftime("%d/%m/%Y"), t.congregation.name if t.congregation else 'N/A', format_currency(float(t.amount))])
        tbl_in = Table(entradas_data, colWidths=[3*cm, 9*cm, 5*cm])
        tbl_in.setStyle(table_style)
        story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada de missões registrada.", styles['Normal']))

    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("Saídas de Missões", heading_style))
    if saidas:
        saidas_data = [["Data", "Descrição", "Valor (R$)"]]
        for t in saidas:
            saidas_data.append([t.date.strftime("%d/%m/%Y"), t.description or "—", format_currency(float(t.amount))])
        tbl_out = Table(saidas_data, colWidths=[3*cm, 10*cm, 5*cm])
        tbl_out.setStyle(table_style)
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

        st.markdown("<h1 class='page-title'>Relatório de Missões (Geral)</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        st.info("Esta página permite editar as entradas de missões de **todas as congregações** e as saídas centralizadas.")
        
        congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()

        st.subheader("Entradas de Missões — por Congregação (editar na tabela)")
        _editor_missions_entries_agg(congs_all, start, end, "missoes_entradas_agg")

        st.subheader("Saídas de Missões (editar na tabela)")
        _, saidas_missoes = _collect_missions_data(db, start, end)
        _editor_missions_outflows(saidas_missoes, "missoes_saidas")

        st.divider()
        st.subheader("Gerar Relatório de Missões (PDF)")
        entradas_missoes_pdf, saidas_missoes_pdf = _collect_missions_data(db, start, end)
        total_entradas = sum(t.amount for t in entradas_missoes_pdf)
        total_saidas = sum(t.amount for t in saidas_missoes_pdf)
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Entradas Missões", format_currency(total_entradas))
        c2.metric("Total Saídas Missões", format_currency(total_saidas))
        c3.metric("Saldo Missões", format_currency(total_entradas - total_saidas))

        st.download_button(
            "⬇️ Baixar Relatório de Missões (PDF)",
            data=build_missions_report_pdf(ref, entradas_missoes_pdf, saidas_missoes_pdf),
            file_name=f"relatorio_missoes_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )

# ===================== NOVA PÁGINA: Relatório de Missões para Congregação =====================
def page_relatorio_missoes_congregacao(user: "User"):
    if not user.congregation_id:
        st.warning("Usuário não vinculado a uma congregação específica."); return

    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)
        cong = db.get(Congregation, user.congregation_id)
        if not cong: st.error("Congregação não encontrada."); return

        st.markdown(f"<h1 class='page-title'>Relatório de Missões</h1>", unsafe_allow_html=True)
        st.info(f"Visualizando entradas de missões para a congregação: **{cong.name}**")
        ref = get_month_selector()
        start, end = month_bounds(ref)

        q_in = select(Transaction).options(joinedload(Transaction.category)).where(
            Transaction.congregation_id == cong.id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type == TYPE_IN,
            Transaction.category.has(Category.name.in_(("Missões", "missões")))
        ).order_by(Transaction.date)
        entradas = db.scalars(q_in).all()
        
        total_entradas = sum(t.amount for t in entradas)
        st.metric(f"Total de Entradas de Missões em {MONTHS[ref.month-1]}", format_currency(total_entradas))
        
        st.subheader("Lançamentos de Entrada")
        if not entradas:
            st.caption("Nenhuma entrada de missões registrada para esta congregação no período.")
        else:
            df = pd.DataFrame([{
                "Data": e.date,
                "Descrição": e.description or "Oferta de Missões",
                "Valor": e.amount
            } for e in entradas])
            st.dataframe(df.assign(Valor=df['Valor'].map(format_currency)), use_container_width=True, hide_index=True)


# ===================== PAGE: CADASTRO =====================
def page_cadastro(user: "User"):
    if not is_admin_general(user):
        st.warning("🔒 Apenas o **administrador geral** (admin) pode acessar o Cadastro.")
        return
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)

        st.subheader("Congregações")
        col_single, col_mass = st.columns(2)
        with col_single:
            new_cong = st.text_input("Nova congregação (individual)", key="cad_new_cong")
            if st.button("Adicionar congregação", disabled=not new_cong.strip(), key="cad_add_cong"):
                if db.scalar(select(Congregation).where(Congregation.name == new_cong.strip())):
                    st.error("Já existe congregação com esse nome.")
                else:
                    db.add(Congregation(name=new_cong.strip())); db.commit()
                    st.success("Congregação adicionada."); st.rerun()
        with col_mass:
            mass_text = st.text_area("Adicionar em massa (uma congregação por linha)", height=140, key="cad_mass_cong")
            if st.button("Adicionar lista", key="cad_add_cong_mass"):
                linhas = [l.strip() for l in (mass_text or "").splitlines() if l.strip()]
                if not linhas:
                    st.warning("Informe ao menos um nome.")
                else:
                    inseridas = 0; repetidas = 0
                    existentes = set(db.scalars(select(Congregation.name)).all())
                    for nome in linhas:
                        if nome in existentes:
                            repetidas += 1
                        else:
                            db.add(Congregation(name=nome)); inseridas += 1; existentes.add(nome)
                    db.commit()
                    st.success(f"Inseridas: {inseridas} | Já existiam: {repetidas}")
                    st.rerun()

        congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        users_by_cong = dict(db.execute(select(Congregation.id, func.count(User.id)).join(User, User.congregation_id == Congregation.id, isouter=True).group_by(Congregation.id)).all())
        tx_by_cong = dict(db.execute(select(Congregation.id, func.count(Transaction.id)).join(Transaction, Transaction.congregation_id == Congregation.id, isouter=True).group_by(Congregation.id)).all())
        tithes_by_cong = dict(db.execute(select(Congregation.id, func.count(Tithe.id)).join(Tithe, Tithe.congregation_id == Congregation.id, isouter=True).group_by(Congregation.id)).all())
        dfc = pd.DataFrame([{"ID": c.id, "Nome": c.name, "Usuários": int(users_by_cong.get(c.id, 0)), "Lançamentos": int(tx_by_cong.get(c.id, 0)), "Dízimos": int(tithes_by_cong.get(c.id, 0))} for c in congs_all])
        if not dfc.empty:
            st.dataframe(dfc, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir congregações"):
            st.caption("Só é possível excluir congregações **sem usuários, lançamentos ou dízimos**. A congregação **Sede** não pode ser excluída.")
            eligible_ids = [c.id for c in congs_all if _norm(c.name) != "sede" and users_by_cong.get(c.id, 0) == 0 and tx_by_cong.get(c.id, 0) == 0 and tithes_by_cong.get(c.id, 0) == 0]
            if not eligible_ids:
                st.info("Nenhuma congregação elegível para exclusão.")
            else:
                ids_del_cong = st.multiselect("IDs de congregações para excluir", eligible_ids, key="cad_del_cong_ids")
                confc2 = st.text_input("Digite EXCLUIR para confirmar", key="cad_del_cong_conf")
                if st.button("Excluir congregações selecionadas", disabled=(not ids_del_cong or not _confirm_ok(confc2)), key="cad_del_cong_btn"):
                    with SessionLocal() as _db:
                        _db.query(Congregation).filter(Congregation.id.in_(ids_del_cong)).delete(synchronize_session=False)
                        _db.commit()
                    st.success(f"{len(ids_del_cong)} congregação(ões) excluída(s)."); st.rerun()
        st.divider()

        st.subheader("Categorias")
        col1, col2 = st.columns(2)
        with col1:
            cat_name = st.text_input("Nome da categoria", key="cad_cat_name")
        with col2:
            cat_type = st.selectbox("Tipo", ["DOAÇÃO", "SAÍDA"], key="cad_cat_type")
        if st.button("Adicionar categoria", disabled=not cat_name.strip(), key="cad_add_cat"):
            if db.scalar(select(Category).where(Category.name == cat_name.strip())):
                st.error("Já existe categoria com esse nome.")
            else:
                db.add(Category(name=cat_name.strip(), type=cat_type)); db.commit()
                st.success("Categoria adicionada."); st.rerun()

        cats = db.scalars(select(Category).order_by(Category.type, Category.name)).all()
        usage = dict(db.execute(select(Category.id, func.count(Transaction.id)).join(Transaction, Transaction.category_id == Category.id, isouter=True).group_by(Category.id)).all())
        dfcat = pd.DataFrame([{"ID": c.id, "Nome": c.name, "Tipo": c.type, "Usos em lançamentos": int(usage.get(c.id, 0))} for c in cats])
        if not dfcat.empty:
            st.dataframe(dfcat, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir categorias"):
            st.caption("Só é possível excluir categorias **sem lançamentos** vinculados.")
            ids_del = st.multiselect("IDs de categorias para excluir", dfcat.loc[dfcat["Usos em lançamentos"] == 0, "ID"].tolist(), key="cad_del_cat_ids")
            confc = st.text_input("Digite EXCLUIR para confirmar", key="cad_del_cat_conf")
            if st.button("Excluir categorias selecionadas", disabled=(not ids_del or not _confirm_ok(confc)), key="cad_del_cat_btn"):
                with SessionLocal() as _db:
                    _db.query(Category).filter(Category.id.in_(ids_del)).delete(synchronize_session=False)
                    _db.commit()
                st.success(f"{len(ids_del)} categoria(s) excluída(s)."); st.rerun()
        st.divider()

        st.subheader("Usuários")
        u_user = st.text_input("Usuário (login)", key="cad_user_login")
        u_pwd = st.text_input("Senha", type="password", key="cad_user_pwd")
        u_role = st.selectbox("Perfil", ["SEDE", "TESOUREIRO", "TESOUREIRO MISSIONÁRIO"], key="cad_user_role")
        all_congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        cong_options = ["—"] + [c.name for c in all_congs]
        
        u_cong_name = "Sede" if u_role == "TESOUREIRO MISSIONÁRIO" else "—"
        cong_disabled = (u_role != "TESOUREIRO")

        u_cong_sel = st.selectbox("Congregação", cong_options, 
            index=(cong_options.index(u_cong_name) if u_cong_name in cong_options else 0),
            key="cad_user_cong", disabled=cong_disabled)

        if st.button("Criar usuário", key="cad_user_add"):
            final_cong_name = "Sede" if u_role == "TESOUREIRO MISSIONÁRIO" else u_cong_sel
            
            if not u_user.strip() or not u_pwd.strip():
                st.error("Usuário e senha são obrigatórios.")
            elif db.scalar(select(User).where(User.username == u_user.strip())):
                st.error("Usuário já existe.")
            else:
                cong_id = None
                if final_cong_name != "—":
                    cong_id = next((c.id for c in all_congs if c.name == final_cong_name), None)
                
                if u_role == "TESOUREIRO" and not cong_id:
                     st.error("Selecione a congregação para o perfil Tesoureiro."); return

                db.add(User(username=u_user.strip(), password_hash=hash_password(u_pwd.strip()), role=u_role, congregation_id=cong_id))
                db.commit()
                st.success("Usuário criado."); st.rerun()

        users = db.scalars(select(User).options(joinedload(User.congregation)).order_by(User.username)).all()
        dfu = pd.DataFrame([{"ID": u.id, "Usuário": u.username, "Perfil": u.role, "Congregação": u.congregation.name if u.congregation else "—"} for u in users])
        if not dfu.empty:
            st.dataframe(dfu, use_container_width=True, hide_index=True, height=200)

        with st.expander("Excluir usuários"):
            st.caption("Não é permitido excluir o usuário atualmente logado.")
            ids_u = st.multiselect("IDs de usuários para excluir", [i for i in dfu["ID"].tolist() if i != user.id], key="cad_del_users_ids")
            confu = st.text_input("Digite EXCLUIR para confirmar", key="cad_del_users_conf")
            if st.button("Excluir usuários selecionadas", disabled=(not ids_u or not _confirm_ok(confu)), key="cad_del_users_btn"):
                with SessionLocal() as _db:
                    _db.query(User).filter(User.id.in_(ids_u)).delete(synchronize_session=False)
                    _db.commit()
                st.success(f"{len(ids_u)} usuário(s) excluído(s)."); st.rerun()

# ===================== MAIN =====================
def main():
    try:
        migrate_database()
        ensure_seed()

        try:
            cm = get_cookie_manager()
            tok = cm.get(COOKIE_NAME)
            data = _read_token(tok)
            if data and not st.session_state.get("uid"):
                with SessionLocal() as db:
                    u = db.get(User, int(data["uid"]))
                    if u:
                        st.session_state.uid = u.id
            if st.session_state.get("uid"):
                _check_inactivity_and_logout(cm)
                _update_last_active(cm)
        except Exception:
            pass

        user = current_user()
        if not user:
            login_ui(); return

        with st.sidebar:
            if user.role == "SEDE":
                menu_options = ["Visão Geral", "Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Dizimistas", "Relatório de Missões", "Cadastro"]
            elif user.role == "TESOUREIRO":
                menu_options = ["Visão Geral", "Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Dizimistas", "Relatório de Missões"]
            elif user.role == "TESOUREIRO MISSIONÁRIO":
                menu_options = ["Relatório de Missões"]
            else:
                menu_options = ["Visão Geral"]
            
            default_index = menu_options.index("Visão Geral") if "Visão Geral" in menu_options else 0
            page = st.radio("Menu", options=menu_options, index=default_index, key="main_menu")

        if page == "Lançamentos":
            page_lancamentos(user)
        elif page == "Relatório de Entrada":
            page_relatorio_entrada(user)
        elif page == "Relatório de Saída":
            page_relatorio_saida(user)
        elif page == "Relatório de Dizimistas":
            page_relatorio_dizimistas(user)
        elif page == "Relatório de Missões":
            if user.role in ["SEDE", "TESOUREIRO MISSIONÁRIO"]:
                page_relatorio_missoes(user)
            elif user.role == "TESOUREIRO":
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
