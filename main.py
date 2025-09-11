# main.py — Igreja Finance CHMS — v8.53 (login persiste após refresh)

from __future__ import annotations

import os
from datetime import date, timedelta, datetime
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

# ==== NOVO: cookies persistentes ====
# pip install extra-streamlit-components
import extra_streamlit_components as stx
import hmac, json, base64, time

# Biblioteca para layout da pesquisa
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# PDF
from io import BytesIO
from reportlab.lib.pagesizes import A4, portrait
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER

# ===================== CONFIG: ADMIN =====================
ADMIN_USERNAME = "admin"  # somente este login verá/entrará no "Cadastro"

# ==== NOVO: Config de sessão/cookie ====
COOKIE_NAME = "chms_auth"
APP_SECRET = st.secrets.get("APP_SECRET", os.environ.get("APP_SECRET", "CHANGE-ME-IN-PROD"))

@st.cache_resource
def get_cookie_manager():
    # instância única do CookieManager durante a sessão do servidor
    return stx.CookieManager()

def _sign_blob(blob: str) -> str:
    return hmac.new(APP_SECRET.encode("utf-8"), blob.encode("utf-8"), hashlib.sha256).hexdigest()

def _encode_token(payload: dict) -> str:
    # token = base64url(json) + '.' + assinatura
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    sig = _sign_blob(data)
    return base64.urlsafe_b64encode(f"{data}.{sig}".encode()).decode()

def _decode_token(token: str) -> Optional[dict]:
    try:
        raw = base64.urlsafe_b64decode(token.encode()).decode()
        data, sig = raw.rsplit(".", 1)
        if not hmac.compare_digest(sig, _sign_blob(data)):
            return None
        payload = json.loads(data)
        # expiração (segundos unix)
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        return payload
    except Exception:
        return None

def set_login_cookie(user, days: int = 90):
    """Cria cookie assinado com expiração."""
    cm = get_cookie_manager()
    payload = {
        "uid": int(user.id),
        "ph": (user.password_hash or "")[:8],  # amarra no hash de senha para invalidação simples
        "exp": int(time.time()) + 86400 * days,
    }
    token = _encode_token(payload)
    # path="/" garante cookie válido em toda a app
    cm.set(COOKIE_NAME, token, expires_at=datetime.utcnow() + timedelta(days=days), key="auth_set", path="/")

def clear_login_cookie():
    cm = get_cookie_manager()
    cm.delete(COOKIE_NAME, key="auth_del", path="/")

def bootstrap_auth_from_cookie():
    """
    Lê cookie logo no início. O CookieManager pode devolver None na 1ª execução após refresh;
    nesse caso paramos o render (st.stop) e deixamos o componente inicializar.
    """
    cm = get_cookie_manager()
    cookies = cm.get_all()
    if cookies is None:
        # Primeira passada após refresh: aguarda cookies ficarem disponíveis
        st.info("Carregando sua sessão…"); st.stop()
    tok = cookies.get(COOKIE_NAME)
    if tok and not st.session_state.get("uid"):
        data = _decode_token(tok)
        if data and "uid" in data:
            with SessionLocal() as db:
                user = db.get(User, int(data["uid"]))
                if user and user.password_hash.startswith(data.get("ph", "")):
                    # restaura sessão
                    st.session_state.uid = user.id

def logout():
    st.session_state.uid = None
    clear_login_cookie()
    st.rerun()

# ===================== ST CONFIG / THEME =====================
st.set_page_config(page_title="Igreja Finance CHMS", page_icon="⛪", layout="wide")

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
    today = date.today()
    colm, coly = st.columns([2, 1])
    with colm:
        m = st.selectbox(f"{label} — Mês", list(range(1, 13)), index=today.month-1, format_func=lambda i: MONTHS[i-1])
    with coly:
        y = st.number_input("Ano", value=today.year, step=1, format="%d")
    return date(int(y), int(m), 1)

def _confirm_ok(val: str) -> bool:
    """Confirmação mais tolerante para 'EXCLUIR'."""
    return str(val or "").strip().upper() == "EXCLUIR"

# ===================== DB BASE & MODELS (primeiro!) =====================
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

# ===================== ENGINE / SESSION (depois dos models) =====================
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

def login_success(user: "User"):
    st.session_state.uid = user.id
    set_login_cookie(user)
    st.rerun()

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
        existentes = set(db.scalars(select(Congregation.name)).all())
        faltantes = [n for n in CONGREGACOES_PADRAO if n not in existentes]
        if faltantes:
            db.add_all(Congregation(name=n) for n in faltantes)
            db.flush()
        sede_cong = db.scalar(select(Congregation).where(Congregation.name == "Sede"))
        if sede_cong is None:
            sede_cong = Congregation(name="Sede")
            db.add(sede_cong); db.flush()
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
        st.markdown("<h1 class='page-title'>Igreja Finance CHMS</h1>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username == u))
            if user and verify_password(p, user.password_hash):
                login_success(user)
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

# ===================== PAGE: LANÇAMENTOS =====================
def page_lancamentos(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
            st.write(f"👤 **{user.username}** — *{user.role}*")
            if st.button("Sair"):
                logout()

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
            for key, default in [("ent_data", date.today()), ("ent_valor", 0.0), ("ent_desc", ""), ("ent_flag_missoes", False)]:
                st.session_state.setdefault(key, default)

            c1,c2,c3 = st.columns([1.1,1.4,2])
            with c1:
                st.date_input("Data do Culto", value=st.session_state["ent_data"], key="ent_data")
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
                        amount=st.session_state["ent_valor"],
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
            for k,dft in [("dz_data", date.today()), ("dz_nome",""), ("dz_valor",0.0)]:
                st.session_state.setdefault(k,dft)

            c1,c2,c3 = st.columns([1.1,2.2,1.1])
            with c1:
                st.date_input("Data do Culto", value=st.session_state["dz_data"], key="dz_data")
            with c2:
                st.text_input("Nome do dizimista", key="dz_nome")
            with c3:
                st.number_input("Valor dízimo (R$)", min_value=0.0, step=1.0, format="%.2f", key="dz_valor")

            st.selectbox("Forma de Pagamento", ["Dinheiro", "PIX"], key="dz_payment_method")

            if st.form_submit_button("Salvar DIZIMISTA", type="primary"):
                nome = (st.session_state.get("dz_nome") or "").strip()
                valor = float(st.session_state.get("dz_valor") or 0.0)
                dta = st.session_state.get("dz_data") or date.today()
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
            for key, default in [("sai_data", date.today()), ("sai_valor", 0.0), ("sai_desc", "")]:
                st.session_state.setdefault(key, default)

            c1,c2,c3 = st.columns([1.1,1.4,2])
            with c1:
                st.date_input("Data", value=st.session_state["sai_data"], key="sai_data")
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
                        amount=st.session_state["sai_valor"], description=(st.session_state["sai_desc"] or None),
                        congregation_id=cong_obj.id,
                    ))
                    _db.commit()
                    st.success("Saída registrada.")
        st.markdown('</div>', unsafe_allow_html=True)

# ===================== CORE COLETA =====================
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

# ===================== PAGE: RELATÓRIO DE ENTRADA =====================
def page_relatorio_entrada(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
            st.write(f"👤 **{user.username}** — *{user.role}*")
            if st.button("Sair"):
                logout()

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
                if t.category and _norm(t.category.name) in ("dizimo", "dízimo"):
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

        if user.role == "SEDE" and not is_all:
            st.divider()
            st.subheader("Exclusões (SEDE)")

            with st.expander("Excluir ENTRADAS (Transaction)"):
                base_rows = [{
                    "ID": t.id,
                    "Data": format_date(t.date),
                    "Congregação": t.congregation.name,
                    "Categoria": t.category.name if t.category else "(Sem categoria)",
                    "Valor (R$)": float(t.amount),
                    "Descrição": t.description or ""
                } for t in data["tx_in"]]

                def _tipo_cat(row):
                    n = _norm(row["Categoria"])
                    if n in ("dizimo","dízimo"): return "Dízimo"
                    if n == "oferta": return "Oferta"
                    if n in ("missoes","missões"): return "Missões"
                    return "Outras"

                filtro = st.selectbox("Filtrar por tipo", ["Todas","Dízimo", "Oferta", "Missões","Outras"], key="del_in_filter")
                if base_rows and filtro != "Todas":
                    base_rows = [r for r in base_rows if _tipo_cat(r) == filtro]

                if base_rows:
                    df_tx = pd.DataFrame(base_rows)
                    df_tx["Valor (R$)"] = df_tx["Valor (R$)"].map(format_currency)
                    st.dataframe(df_tx, use_container_width=True, hide_index=True, height=220)
                    ids_in_list = df_tx["ID"].tolist()
                    ids = st.multiselect("IDs para excluir", ids_in_list, key="del_in_ids")
                    conf = st.text_input("Digite EXCLUIR para confirmar", key="del_in_conf")
                    btn_disabled = (not ids) or (not _confirm_ok(conf))
                    if st.button("Excluir ENTRADAS selecionadas", disabled=btn_disabled, key="del_in_btn"):
                        with SessionLocal() as _db:
                            _db.query(Transaction).filter(Transaction.id.in_(ids)).delete(synchronize_session=False)
                            _db.commit()
                        st.success(f"{len(ids)} entrada(s) excluída(s).")
                        st.rerun()
                else:
                    st.caption("Sem entradas no período/escopo.")

            with st.expander("Excluir Dízimos"):
                df_tz = pd.DataFrame([{
                    "ID": t.id,
                    "Data": format_date(t.date),
                    "Congregação": t.congregation.name,
                    "Dizimista": t.tither_name,
                    "Valor (R$)": float(t.amount)
                } for t in data["tithes"]])
                if not df_tz.empty:
                    df_tz["Valor (R$)"] = df_tz["Valor (R$)"].map(format_currency)
                    st.dataframe(df_tz, use_container_width=True, hide_index=True, height=220)
                    ids2 = st.multiselect("IDs de dízimos para excluir", df_tz["ID"].tolist(), key="del_tithe_ids_in")
                    conf2 = st.text_input("Digite EXCLUIR para confirmar", key="del_tithe_conf_in")
                    btn2_disabled = (not ids2) or (not _confirm_ok(conf2))
                    if st.button("Excluir dízimos selecionados", disabled=btn2_disabled, key="del_tithe_btn_in"):
                        with SessionLocal() as _db:
                            _db.query(Tithe).filter(Tithe.id.in_(ids2)).delete(synchronize_session=False)
                            _db.commit()
                        st.success(f"{len(ids2)} dízimo(s) excluído(s).")
                        st.rerun()
                else:
                    st.caption("Sem dízimos no período/escopo.")

# ===================== PAGE: RELATÓRIO DE SAÍDA =====================
def page_relatorio_saida(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
            st.write(f"👤 **{user.username}** — *{user.role}*")
            if st.button("Sair"):
                logout()

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
        st.subheader("Missões no período (Saídas)")
        if is_all:
            agg = defaultdict(float)
            for t in txs:
                if t.category and _norm(t.category.name) in ("missoes","missões"):
                    agg[t.congregation.name] += float(t.amount)
            dfm = pd.DataFrame([{"Congregação": k, "Saídas Missões": format_currency(v)} for k,v in sorted(agg.items())])
            if not dfm.empty:
                st.dataframe(dfm, use_container_width=True, hide_index=True, height=200)
            else:
                st.caption("Sem saídas de Missões.")
        else:
            val = sum(float(t.amount) for t in txs if t.category and _norm(t.category.name) in ("missoes","missões"))
            st.metric("Saídas Missões", format_currency(val))

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

        if user.role == "SEDE" and not is_all:
            st.divider(); st.subheader("Excluir SAÍDAS (SEDE)")
            if 'df_list' in locals() and not df_list.empty:
                ids = st.multiselect("IDs para excluir", df_list["ID"].tolist(), key="del_out_ids")
                conf = st.text_input("Digite EXCLUIR para confirmar", key="del_out_conf")
                btn_disabled = (not ids) or (not _confirm_ok(conf))
                if st.button("Excluir selecionados", disabled=btn_disabled, key="del_out_btn"):
                    with SessionLocal() as _db:
                        _db.query(Transaction).filter(Transaction.id.in_(ids)).delete(synchronize_session=False)
                        _db.commit()
                    st.success(f"{len(ids)} saída(s) excluída(s).")
                    st.rerun()
            else:
                st.caption("Sem saídas para exclusão neste escopo.")

# ======= (Demais páginas — Relatório de Dizimistas, Missões, Cadastro, Visão Geral)
# >>> As funções a seguir foram mantidas como no seu código, apenas trocando o botão "Sair" para usar logout() <<<

# ... [MANTENHA O RESTANTE DO SEU ARQUIVO IGUAL, COM A ÚNICA MUDANÇA DE SUBSTITUIR
#      `st.session_state.uid=None; st.rerun()` POR `logout()` EM CADA SIDEBAR] ...
# Para economizar espaço aqui, não repliquei cada página novamente; a lógica de persistência já está aplicada.

# ===================== MAIN =====================
def main():
    try:
        ensure_seed()

        # >>> NOVO: restaura a sessão a partir do cookie antes de decidir mostrar login
        bootstrap_auth_from_cookie()

        user = current_user()
        if not user:
            login_ui(); return

        with st.sidebar:
            if user.role == "SEDE":
                menu_options = ["Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Dizimistas", "Relatório de Missões", "Visão Geral", "Cadastro"]
            elif user.role == "TESOUREIRO":
                menu_options = ["Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Dizimistas", "Visão Geral"]
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
