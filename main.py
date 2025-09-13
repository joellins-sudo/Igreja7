# main.py — AD Relatório Financeiro — v9.2 (Saídas com clique/edição, logout robusto, edição intuitiva em Dizimistas/Missões)
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
import json, base64, hmac, time

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

from io import BytesIO
from reportlab.lib.pagesizes import A4, portrait
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER

from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

# ===================== CONFIG =====================
ADMIN_USERNAME = "admin"
INACTIVITY_MINUTES = int(
    st.secrets.get("INACTIVITY_MINUTES", os.environ.get("INACTIVITY_MINUTES", 20))
)

st.set_page_config(page_title="AD Relatório Financeiro", page_icon="⛪", layout="wide")

CSS = """
<style>
:root{
  --brand:#0f172a; --brand-2:#1d4ed8; --accent:#16a34a; --danger:#dc2626; --muted:#6b7280;
  --bg:#f8fafc; --card:#ffffff; --ring:#e5e7eb;
}
html, body { background: var(--bg); }
header[data-testid="stHeader"]{ background: linear-gradient(180deg,#ffffff, #f6f9ff)!important; border-bottom:1px solid var(--ring); }
.block-container{ padding-top: .9rem!important; }
[data-testid="stSidebar"]{ background: linear-gradient(180deg,#ffffff,#fbfdff); border-right:1px solid var(--ring); }
[data-testid="stSidebar"] img{ border-radius: .6rem; border:1px solid var(--ring); box-shadow: 0 4px 16px rgba(0,0,0,.06); }
.page-title{ font-family:'Nunito','Inter',system-ui; font-size:36px; font-weight:900; letter-spacing:.4px; color: var(--brand); margin:.25rem 0 1rem; }
h2, .stMarkdown h2, .st-subheader{ color:#0f172a!important; font-weight:900!important; }
.st-container-card{ border: 1px solid var(--ring); border-radius: 1rem; padding: 1rem; margin-bottom: 1.15rem; background: var(--card); box-shadow: 0 10px 30px rgb(31 58 138 / .06), 0 2px 8px rgb(0 0 0 / .04); position: relative; }
.st-container-card::before{ content:""; position: absolute; left:0; top:0; bottom:0; width:6px; background: linear-gradient(180deg,var(--brand-2), var(--brand)); border-top-left-radius:1rem; border-bottom-left-radius:1rem; }
div[data-testid="stMetric"]{ padding: .75rem .9rem; border:1px solid var(--ring); border-radius:.9rem; background:var(--card); box-shadow:0 1px 2px rgba(0,0,0,.06); }
div[data-testid="stMetricLabel"]{ color:#334155; font-weight:800; }
div[data-testid="stMetricValue"]{ font-size:26px!important; line-height:1.15!important; white-space:nowrap!important; font-weight:900!important; }
.stat-card{ border:1px solid var(--ring); border-radius:.9rem; background:var(--card); padding:.9rem 1rem; box-shadow:0 1px 2px rgba(0,0,0,.06); transition: transform .15s ease, box-shadow .15s ease; position:relative; height:86px; display:flex; flex-direction:column; justify-content:center; }
.stat-card:hover{ transform: translateY(-2px); box-shadow: 0 12px 22px rgba(31,58,138,.12), 0 4px 8px rgba(0,0,0,.08); }
.stat-label{ font-size:.92rem; color:#334155; font-weight:800; margin-bottom:.18rem; }
.stat-value{ font-size:1.25rem; font-weight:900; color:#111827; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
label{ color:#0f172a; font-weight:800; }
.stTextInput input, .stNumberInput input, .stDateInput input{ border:1px solid var(--ring)!important; border-radius:.65rem!important; }
.stSelectbox [data-baseweb="select"]>div{ border:1px solid var(--ring)!important; border-radius:.65rem!important; }
.stButton>button{ border:1px solid #2563eb!important; color:#fff!important; background: linear-gradient(180deg,#3b82f6,#2563eb)!important; font-weight:900!important; border-radius:.7rem!important; padding:.52rem .95rem!important; box-shadow:0 2px 6px rgba(37,99,235,.25)!important; }
.btn-green button{ background: linear-gradient(180deg,#22c55e,#16a34a)!important; border-color:#16a34a!important; color:#fff!important; }
.btn-red button{ background: linear-gradient(180deg,#ef4444,#dc2626)!important; border-color:#dc2626!important; color:#fff!important; }
[data-testid="stDataFrame"]{ border:1px solid var(--ring); border-radius:.9rem; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,.04); }
[data-testid="stDataFrame"] thead tr th{ position:sticky; top:0; z-index:2; background:#f1f5f9!important; font-weight:900!important; }
.st-expander{ border:1px solid var(--ring)!important; border-radius:.9rem!important; background:#fff!important; }
.small-note{ color: var(--muted); font-size:.92rem; }
.cong-title{ font-weight:900; font-size:1.05rem; color:#0f172a; margin-bottom:.35rem; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
LOGO_PATH = os.path.join(ASSETS_DIR, "logo.png")

# ===================== LOCALE =====================
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
    return str(val or "").strip().upper() == "EXCLUIR"

# ===================== DB =====================
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

SessionLocal = get_sessionmaker()

# ===================== AUTH / COOKIES =====================
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
            sede_cong = Congregation(name="Sede"); db.add(sede_cong); db.flush()
        if db.scalar(select(User).where(User.username == ADMIN_USERNAME)) is None:
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password("123456"), role="SEDE", congregation_id=sede_cong.id))
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
        with col_logo: st.image(LOGO_PATH, use_container_width=True)
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
                try:
                    cm = get_cookie_manager()
                    token = _make_token({"uid": int(user.id)})
                    cm.set(COOKIE_NAME, token, expires_at=datetime.utcnow()+timedelta(days=30))
                    cm.set(COOKIE_LAST, str(time.time()), expires_at=datetime.utcnow()+timedelta(days=30))
                except Exception:
                    st.warning("Login salvo só nesta sessão.")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

# ===================== HELPERS =====================
def is_admin_general(user: "User") -> bool:
    return (user.username or "").strip().lower() == ADMIN_USERNAME.lower()

def categories_for_type(db: Session, kind: str) -> List["Category"]:
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

def cong_options_for(user: "User", db: Session) -> List["Congregation"]:
    if user.role == "SEDE":
        return db.scalars(select(Congregation).order_by(Congregation.name)).all()
    else:
        if user.congregation_id:
            c = db.get(Congregation, user.congregation_id)
            return [c] if c else []
        return []

def order_congs_sede_first(congs: List["Congregation"]) -> List["Congregation"]:
    sede = [c for c in congs if _norm(c.name) == "sede"]
    others = sorted([c for c in congs if _norm(c.name) != "sede"], key=lambda x: _norm(x.name))
    return (sede + others) if sede else others

def sidebar_common(user: "User"):
    with st.sidebar:
        if os.path.exists(LOGO_PATH): st.image(LOGO_PATH, use_column_width=True)
        st.write(f"👤 **{user.username}** — *{user.role}*")
        st.button("Sair", on_click=logout)

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

# ===================== PÁGINAS (Lançamentos igual à versão anterior) =====================
# ... (idêntico ao seu código anterior da página de lançamentos para preservar a base)
# Para economizar espaço, a página de lançamentos permanece igual à versão enviada na v9.0.
# >>> COPIE aqui a função page_lancamentos(user) da sua versão anterior, pois não sofreu alterações. <<<

# ===================== RELATÓRIO DE ENTRADA (sem mudanças em relação à v9.0) =====================
# ... (mesmo código da v9.0 — inclui Resumo por data clicável com edição/exclusão para dízimo/oferta)
# >>> COPIE aqui a função page_relatorio_entrada(user) da v9.0. <<<

# ===================== RELATÓRIO DE SAÍDA (NOVO: clique por data + edição direta) =====================
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

        # Busca saídas do período
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
            # Visão geral por congregação (sem edição)
            agg = defaultdict(Decimal)
            for t in txs:
                agg[t.congregation.name] += money(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Total de saídas": format_currency(v)} for k,v in sorted(agg.items())])
            st.subheader("Resumo por congregação")
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True, height=220)
            else:
                st.caption("Sem saídas.")
        else:
            # ==== NOVO: Resumo por data (clicável) ====
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
                    df_sum,
                    gridOptions=gb.build(),
                    data_return_mode=DataReturnMode.FILTERED,
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
                    height=220,
                    allow_unsafe_jscode=True,
                )
                sel_rows = grid.selected_rows
                if sel_rows:
                    sel_date_str = sel_rows[0]["Data"]
                    sel_date = datetime.strptime(sel_date_str, "%d/%m/%Y").date()
                    # Lista lançamentos da data selecionada
                    lista = [t for t in txs if t.date == sel_date]
                    st.markdown(f"**Lançamentos em {sel_date_str}**")
                    df_list = pd.DataFrame([{
                        "ID": t.id,
                        "Categoria": t.category.name if t.category else "—",
                        "Descrição": t.description or "",
                        "Valor (R$)": format_currency(t.amount)
                    } for t in lista])
                    gb2 = GridOptionsBuilder.from_dataframe(df_list)
                    gb2.configure_grid_options(rowSelection='single', suppressRowClickSelection=False)
                    grid2 = AgGrid(
                        df_list,
                        gridOptions=gb2.build(),
                        data_return_mode=DataReturnMode.FILTERED,
                        update_mode=GridUpdateMode.SELECTION_CHANGED,
                        height=230,
                        allow_unsafe_jscode=True,
                    )
                    sel2 = grid2.selected_rows
                    if sel2:
                        rid = int(sel2[0]["ID"])
                        obj = next((t for t in lista if t.id == rid), None)
                        st.write(f"Registro selecionado: **ID {rid}** — {obj.category.name if obj and obj.category else '—'} — {format_currency(obj.amount) if obj else ''}")
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

        # Exportar CSV
        st.divider()
        rows = [{
            "Data": t.date.strftime("%d/%m/%Y"),
            "Congregação": t.congregation.name,
            "Tipo da saída": t.category.name if t.category else "",
            "Valor": f"{money(t.amount):.2f}".replace(".", ","),
            "Descrição": t.description or ""
        } for t in txs]
        csv = pd.DataFrame(rows).to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Baixar CSV das SAÍDAS do período", data=csv, file_name=f"saidas_{start.strftime('%Y-%m')}.csv", mime="text/csv")

# ===================== MISSÕES (GERAL) — edição intuitiva via clique =====================
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
        st.warning("🔒 Acesso negado. Apenas perfis `SEDE` ou `TESOUREIRO MISSIONÁRIO`.")
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

        # ==== Entradas (form + tabela clicável para editar/excluir) ====
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
                        st.error("Categoria 'Missões' não encontrada."); return
                    _db.add(Transaction(
                        date=ent_data, type=TYPE_IN, category_id=cat_obj.id,
                        amount=float(money(ent_valor)), description=(ent_desc or None),
                        congregation_id=cong_obj.id
                    ))
                    _db.commit()
                    st.success(f"Entrada de missões para '{cong_sel}' registrada."); st.rerun()

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
            st.caption("Nenhuma entrada de missões registrada.")

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
                        st.error("Categoria 'Missões (Saída)' não encontrada."); return
                    sede_cong = _db.scalar(select(Congregation).where(Congregation.name == "Sede"))
                    if not sede_cong: st.error("Congregação 'Sede' não encontrada."); return
                    _db.add(Transaction(
                        date=sai_data, type=TYPE_OUT, category_id=cat_obj.id,
                        amount=float(money(sai_valor)), description=(sai_desc or None),
                        congregation_id=sede_cong.id
                    ))
                    _db.commit()
                    st.success("Saída de missões registrada."); st.rerun()

        st.subheader("Saídas de Missões no Período")
        if saidas_missoes:
            df_s = pd.DataFrame([{
                "ID": t.id, "Data": format_date(t.date),
                "Descrição": t.description or "", "Valor (R$)": format_currency(t.amount)
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
            st.caption("Nenhuma saída de missões registrada.")

        st.divider()
        st.subheader("Gerar Relatório em PDF (Geral)")
        st.download_button(
            "⬇️ Baixar Relatório de Missões (PDF)",
            data=build_missions_report_pdf(ref, entradas_missoes, saidas_missoes),
            file_name=f"relatorio_missoes_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf"
        )

# ===================== MISSÕES (CONGREGAÇÃO) — edição intuitiva via clique =====================
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

        st.subheader("Entradas de Missões no Período")
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
                st.write(f"Selecionado: **ID {rid}** — {format_currency(obj.amount) if obj else ''}")
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

# ===================== DIZIMISTAS — edição intuitiva via clique =====================
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
            all_tz = db.scalars(select(Tithe).where(Tithe.date >= start, Tithe.date < end).order_by(Tithe.congregation_id, Tithe.date)).all()
            by_cong = defaultdict(lambda: {"qtd":0, "valor":Decimal("0.00")})
            for t in all_tz:
                k = t.congregation.name
                by_cong[k]["qtd"] += 1; by_cong[k]["valor"] += money(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Qtde de dízimos": v["qtd"], "Total (R$)": format_currency(v["valor"])} for k,v in sorted(by_cong.items())])
            st.dataframe(df, use_container_width=True, hide_index=True, height=200)
            st.download_button(
                "⬇️ Baixar PDF — Lista nominal (todas as congregações)",
                data=build_dizimistas_nominal_pdf(all_tz, "Lista de Dizimistas (Mensal)", f"Referente a {ref.strftime('%B de %Y')} — Todas as congregações"),
                file_name=f"dizimistas_geral_{start.strftime('%Y-%m')}.pdf",
                mime="application/pdf"
            )
        else:
            tithes = db.scalars(select(Tithe).where(
                Tithe.date >= start, Tithe.date < end, Tithe.congregation_id == cong_obj.id
            ).order_by(Tithe.date)).all()

            st.subheader("Dízimos do período")
            df = pd.DataFrame([{
                "ID": t.id, "Data": format_date(t.date),
                "Dizimista": t.tither_name, "Valor (R$)": format_currency(float(t.amount)),
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
                    st.write(f"Selecionado: **ID {rid}** — {obj.tither_name if obj else ''} — {format_currency(obj.amount) if obj else ''}")
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

# ===================== VISÃO GERAL / CADASTRO =====================
# (iguais à v9.0; preservados)
# >>> COPIE aqui page_visao_geral(user) e page_cadastro(user) da v9.0, pois não sofreram mudanças. <<<

# ===================== MAIN =====================
def main():
    try:
        ensure_seed()

        # Restaurar por cookie (a menos que tenhamos acabado de sair)
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
            page_lancamentos(user)             # <- use sua função existente (inalterada)
        elif page == "Relatório de Entrada":
            page_relatorio_entrada(user)       # <- use sua função existente (inalterada)
        elif page == "Relatório de Saída":
            page_relatorio_saida(user)         # <- NOVO fluxo com clique/edição
        elif page == "Relatório de Dizimistas":
            page_relatorio_dizimistas(user)    # <- Edição intuitiva via clique
        elif page == "Relatório de Missões":
            page_relatorio_missoes(user)       # <- Edição intuitiva via clique
        elif page == "Relatório de Missões (Congregação)":
            page_relatorio_missoes_congregacao(user)  # <- Edição intuitiva via clique
        elif page == "Visão Geral":
            page_visao_geral(user)             # <- copie da v9.0
        elif page == "Cadastro":
            page_cadastro(user)                # <- copie da v9.0
    except Exception as e:
        st.error("Ocorreu um erro ao renderizar a aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
