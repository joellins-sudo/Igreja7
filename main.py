# main.py — AD Relatório Financeiro — v13.3
# Melhorias deste commit (apenas estas):
# 1) Adicionado botão "Salvar alterações" abaixo de TODAS as tabelas editáveis.
# 2) Tesoureiro Missionário pode lançar SAÍDAS de Missões para QUALQUER congregação
#    (editor agora tem coluna "Congregação"); Entradas continuam no editor agregado.
# 3) Nova aba "Relatório de Missões" para TESOUREIRO (congregações) ver apenas seus lançamentos.
# 4) [EQUIVALÊNCIA DE DÍZIMOS] Dízimos lançados em "Entrada (Doação)" e por "Dizimista"
#    agora são tratados como equivalentes (NÃO são somados). Em resumos por data e totais mensais,
#    usa-se o MAIOR entre (soma de Tithes) e (soma de Transactions categoria "Dízimo").
#
# Obs.: Todo o restante do seu código foi preservado. Itens que você pediu antes
# (ex.: esconder "ajuste" na ENTRADA, relatórios agregados editáveis da SEDE, etc.) continuam iguais.

from __future__ import annotations
# ===== UI extra (menu bonito com fallback) =====
try:
    import streamlit_antd_components as sac  # pip install streamlit-antd-components
except Exception:
    sac = None  # fallback p/ radio padrão
import hashlib
from sqlalchemy import select

import os
from datetime import date, timedelta, datetime
from typing import Optional, List, Tuple, Dict, Any
from collections import defaultdict, Counter
import locale as _locale
import pandas as pd
import streamlit as st

from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine, and_
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
from sqlalchemy.orm import DeclarativeBase
import unicodedata as ud
import hashlib
import json, base64, hmac, time

# PDF
from io import BytesIO
from reportlab.lib.pagesizes import A4, portrait
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
ADJ_ENTRY_AGG_DESC = "[Ajuste total de entradas (mês, sede)]"
ADJ_OUT_AGG_DESC   = "[Ajuste total de saídas (mês, sede)]"

# ===================== ST CONFIG / THEME =====================
# ===================== ST CONFIG / THEME =====================

st.set_page_config(page_title=APP_NAME, page_icon="⛪", layout="wide")

# ================== CSS do cartão de login (estilo SEI) ==================
# ================== CSS do cartão de login (estilo ADRF) ==================
# ================== LOGIN SEI: CSS/HTML (trabalhando com Streamlit) ==================
# ================== LOGIN ADRF: CSS/HTML ==================
ADRF_LOGIN_CSS = """
<style>
  :root{
    --azul-1:#1f6feb; --azul-2:#185fcd; --azul-esc:#0b4b9a;
    --cinza-bg:#f0f0f0; --cinza-borda:#dfe3ea; --cinza-ico:#e9ecef; --texto:#344054;
  }
  body{ background:var(--cinza-bg); }

  .adrf-wrap{ min-height:calc(100vh - 0px); display:grid; place-items:center; padding:24px 12px; }
  .adrf-card{ width:100%; max-width:540px; background:#fff; border:1px solid rgba(0,0,0,.07);
              border-radius:.5rem; box-shadow:0 10px 26px rgba(16,24,40,.08); }
  .adrf-card .body{ padding:26px 24px 18px; }

  .adrf-logo{ display:flex; align-items:center; justify-content:center; margin:14px 0 18px; }
  .adrf-logo img{ height:58px; }

  .adrf-form .group{ display:flex; align-items:stretch; margin-bottom:12px; }
  .adrf-form .ico{
    flex:0 0 46px; display:flex; align-items:center; justify-content:center;
    background:var(--cinza-ico); border:1px solid var(--cinza-borda);
    border-right:none; border-radius:.25rem 0 0 .25rem; color:#6b7280; font-size:18px;
  }
  .adrf-form .field [data-testid="stTextInput"]>div>div>input,
  .adrf-form .field [data-testid="stPassword"]>div>div>input,
  .adrf-form .field [data-testid="stSelectbox"]>div>div>div>div{
    height:44px; border:1px solid var(--cinza-borda); border-left:none; border-radius:0 .25rem .25rem 0 !important;
    font-size:1rem;
  }
  .adrf-form .field [data-testid="stWidgetLabel"]{ display:none; }

  .adrf-btn .stButton>button{
    width:100%; height:44px; border:none; color:#fff; font-weight:700; letter-spacing:.3px; border-radius:.25rem;
    background:linear-gradient(180deg, var(--azul-1) 0%, var(--azul-2) 100%);
    box-shadow:0 6px 16px rgba(24,95,205,.25);
  }
  .adrf-btn .stButton>button:hover{
    background:linear-gradient(180deg, var(--azul-2) 0%, var(--azul-esc) 100%);
  }

  .adrf-2fa{ text-align:right; margin-top:6px; }
  .adrf-2fa a{ color:#0d6efd; font-size:.92rem; text-decoration:none; }
  .adrf-2fa a:hover{ text-decoration:underline; }
</style>
"""

CSS = """
<style>
/* ==================== BASE / TIPOGRAFIA ==================== */
:root{
  --base-font: 17px;              /* aumente para 18/19/20px se quiser */
  --table-font-size: 1.90rem;     /* fonte das células */
  --table-header-size: 1.08rem;   /* fonte dos cabeçalhos */
}

html, body, [data-testid="stAppViewContainer"]{
  font-size: var(--base-font);
  line-height: 1.45;
}

/* Títulos mais fortes e maiores */
.page-title, h1{ font-size: 2.0rem; font-weight: 800 !important; }
h2{ font-size: 1.45rem; font-weight: 750; }
h3{ font-size: 1.25rem; font-weight: 700; }

/* ==================== WIDGETS / TEXTOS ==================== */
[data-testid="stSidebar"] *{ font-size: 1.02rem; }
label, [data-testid="stWidgetLabel"]{ font-size: 1.02rem; }

/* Inputs (texto, número, data, selects) um pouco maiores */
.stTextInput input,
.stNumberInput input,
.stDateInput input,
.stSelectbox div,
.stMultiSelect div{
  font-size: 1.02rem !important;
}

/* ==================== TABELAS / EDITOR ==================== */
/* Regras gerais (ok manter) */
[data-testid="stDataFrame"] *{ font-size: 1.0rem; }
[data-testid="stDataEditor"] *{ font-size: 1.02rem; }

/* Regras específicas – aumentam o tamanho real das células/cabeçalhos */
[data-testid="stDataFrame"] [role="gridcell"],
[data-testid="stDataEditor"] [role="gridcell"]{
  font-size: var(--table-font-size) !important;
}

[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stDataEditor"] [role="columnheader"]{
  font-size: var(--table-header-size) !important;
  font-weight: 700 !important;
}

/* Altura das linhas (opcional) */
[data-testid="stDataFrame"] [role="row"],
[data-testid="stDataEditor"] [role="row"]{
  min-height: 38px;
}

/* Espaço interno das células (opcional) */
[data-testid="stDataFrame"] [role="gridcell"] > div,
[data-testid="stDataEditor"] [role="gridcell"] > div{
  padding: 8px 10px;
}

/* ==================== MÉTRICAS ==================== */
[data-testid="stMetricValue"]{
  font-size: 1.9rem !important;
  font-weight: 780 !important;
}
[data-testid="stMetricLabel"]{ font-size: 1.0rem; opacity: .8; }

/* ==================== BOTÕES ==================== */
.stButton > button, .stDownloadButton > button{
  font-size: 1.02rem;
  border-radius: 14px;
  font-weight: 650;
}

/* ==================== CARTÕES ESTATÍSTICOS ==================== */
.stat-card{
  background: #fff;
  border: 1px solid #e9e9ee;
  border-radius: 16px;
  padding: 14px 16px;
}
.stat-label{ font-size: .92rem; opacity: .75; }
.stat-value{ font-size: 1.12rem; font-weight: 700; margin-top: .2rem; }

/* ==================== SIDEBAR ==================== */
[data-testid="stSidebar"]{
  background: linear-gradient(180deg, #f7f7fb 0%, #f2f3f9 100%);
}
[data-testid="stSidebar"] .block-container{ padding-top: 1rem; }

/* ==================== AJUSTES LEVES ==================== */
hr{ opacity: .6; }
</style>
"""
CSS_TABLE_BOOST = """
<style>
/* Aumenta o tamanho da fonte APENAS do conteúdo das células */
[data-testid="stDataFrame"] [role="gridcell"] *,
[data-testid="stDataEditor"] [role="gridcell"] *{
  font-size: 1.18rem !important;   /* ajuste aqui: 1.10–1.30rem */
  line-height: 1.55 !important;
}

/* Cabeçalhos das colunas um pouco maiores e mais fortes */
[data-testid="stDataFrame"] [role="columnheader"] *,
[data-testid="stDataEditor"] [role="columnheader"] *{
  font-size: 1.08rem !important;
  font-weight: 700 !important;
}
</style>
"""
st.markdown(CSS_TABLE_BOOST, unsafe_allow_html=True)

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
# ===================== DB BASE & MODELS =====================
class Base(DeclarativeBase):
    pass

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
        db_url = "sqlite:///database.db"
    return create_engine(db_url, pool_pre_ping=True)

@st.cache_resource
def get_sessionmaker():
    engine = get_engine()
    Base.metadata.create_all(engine)
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
    # === CSS base ADRF + centralização e largura fixa do cartão ===
    st.markdown("""
    <style>
      /* remove header/rodapé do Streamlit na tela de login */
      header[data-testid="stHeader"]{ display:none; }
      footer{ visibility:hidden; }

      /* centraliza tudo vertical/horizontal */
      [data-testid="stAppViewContainer"] > .main{
        display:flex; align-items:center; justify-content:center;
        min-height:100vh; padding:0 !important;
      }

      /* cartão do login */
      .adrf-card{
        width: 420px;               /* ajuste: 360–520px */
        max-width: 92vw;
        background:#fff;
        border:1px solid #E6E8F0;
        border-radius:14px;
        box-shadow:0 10px 30px rgba(16,24,40,.08);
        padding:22px 18px 18px;
      }

      /* título ADRF! */
      .adrf-title{
        text-align:center;
        font: 800 48px/1.1 Inter,system-ui;
        color:#1f66eb;
        letter-spacing:.4px;
        margin:2px 0 14px;
      }
      .adrf-title .i{ color:#74b816; }

      /* inputs e botão dentro do form */
      form[aria-label="adrf_login_form"] .stTextInput>div>div>input,
      form[aria-label="adrf_login_form"] .stPassword>div>div>input{
        height:44px; font-size:1rem;
      }
      form[aria-label="adrf_login_form"] .stButton>button{
        width:100%; height:44px; font-weight:700; border-radius:10px;
      }
    </style>
    """, unsafe_allow_html=True)

    # === Cartão ===
    st.markdown("<div class='adrf-card'>", unsafe_allow_html=True)

    # Logo (se houver) ou título ADRF!
    try:
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, use_container_width=True)
        else:
            st.markdown("<div class='adrf-title'>ADRF<span class='i'>!</span></div>", unsafe_allow_html=True)
    except Exception:
        st.markdown("<div class='adrf-title'>ADRF<span class='i'>!</span></div>", unsafe_allow_html=True)

    # === Formulário de login (chave usada no CSS) ===
    with st.form("adrf_login_form", clear_on_submit=False):
        u = st.text_input("Usuário", placeholder="Usuário")
        p = st.text_input("Senha", type="password", placeholder="Senha")
        submitted = st.form_submit_button("ACESSAR")

    # === Autenticação ===
    if submitted:
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username == u))
            if user and verify_password(p, user.password_hash):
                st.session_state.uid = user.id
                # Cookie (se disponível)
                try:
                    cm = get_cookie_manager()
                    token = _make_token({"uid": int(user.id)})
                    cm.set(COOKIE_NAME, token,
                           expires_at=datetime.utcnow()+timedelta(days=30),
                           key="auth_set")
                    _update_last_active(cm)
                except Exception:
                    st.warning("Login salvo só na sessão atual. (extra-streamlit-components não instalado)")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

    st.markdown("</div>", unsafe_allow_html=True)

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

def sidebar_common(user: "User") -> str:
    """
    Desenha o menu lateral apenas uma vez e retorna a página selecionada.
    Em reruns, mantém a escolha do usuário.
    """
    # Se já desenhou nesta execução, só devolve a última seleção
    if st.session_state.get("sidebar_rendered", False):
        return st.session_state.get("main_menu_page", "Visão Geral")

    with st.sidebar:
        # Identidade / logo
        try:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
        except Exception:
            pass
        st.write(f"👤 **{getattr(user, 'username', 'Usuário')}** — *{getattr(user, 'role', '')}*")

        # Ícones
        MENU_ICONS = {
            "Lançamentos": "📥",
            "Relatório de Entrada": "📊",
            "Relatório de Saída": "📉",
            "Relatório de Missões": "🌍",
            "Relatório de Dizimistas": "🧾",
            "Visão Geral": "🏁",
            "Cadastro": "🛠️",
        }

        # Opções conforme o papel
        role = getattr(user, "role", "")
        if role == "SEDE":
            menu_options_plain = [
                "Lançamentos", "Relatório de Entrada", "Relatório de Saída",
                "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral", "Cadastro"
            ]
        elif role == "TESOUREIRO":
            menu_options_plain = [
                "Lançamentos", "Relatório de Entrada", "Relatório de Saída",
                "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral"
            ]
        elif role == "TESOUREIRO MISSIONÁRIO":
            menu_options_plain = ["Relatório de Missões"]
        else:
            menu_options_plain = ["Visão Geral"]

        # Labels com ícones
        menu_labels_pretty = [f"{MENU_ICONS.get(opt, '•')} {opt}" for opt in menu_options_plain]

        # Chave estável do widget
        state_key = f"main_menu_nav_{getattr(user, 'id', 'anon')}"

        # Define valor inicial só uma vez (sem passar index no radio)
        if state_key not in st.session_state:
            # usa última página escolhida, se houver
            prev_page = st.session_state.get("main_menu_page")
            if prev_page in menu_options_plain:
                init_label = f"{MENU_ICONS.get(prev_page, '•')} {prev_page}"
            else:
                init_label = menu_labels_pretty[0]
            st.session_state[state_key] = init_label

        sel_label = st.radio(
            "Menu",
            options=menu_labels_pretty,
            key=state_key,
            label_visibility="visible",
        )

        # Converte label -> página “pura”
        sel_index = menu_labels_pretty.index(sel_label)
        page = menu_options_plain[sel_index]
        st.session_state["main_menu_page"] = page

        st.divider()
        if st.button("Sair", key=f"btn_logout_{getattr(user, 'id', 'anon')}"):
            logout()

        # Marca como renderizado
        st.session_state["sidebar_rendered"] = True

        return page


# ======= NOVO: helper padrão para botões 'Salvar alterações' =======
def _save_btn(on_click, key_suffix: str):
    st.button("Salvar alterações", type="primary", key=f"btn_save_{key_suffix}", on_click=on_click)


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
            # NOVO: permitir mudar congregação no editor (quando houver _cong_id)
            if "_cong_id" in n.columns:
                old_cid = int(old.get("_cong_id", 0) or 0)
                new_cid = int(new.get("_cong_id", 0) or 0)
                if new_cid and new_cid != old_cid:
                    t.congregation_id = new_cid; changed = True
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
                # Missões (Saída) editor simples
                cat = db.scalar(select(Category).where(Category.name == "Missões (Saída)"))
                if not cat:
                    continue
            # NOVO: pegar congregação do próprio row (quando existir)
            row_cid = int(row.get("_cong_id", 0) or 0)
            cong_id = row_cid or default_cong_id
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
    # [EQ FIX]: separar somatórios de Dízimo (tithes) e Dízimo (transactions), e usar o MAIOR por data.
    tithes = db.execute(
        select(Tithe.date, func.sum(Tithe.amount))
        .where(and_(Tithe.congregation_id == cong_id, Tithe.date >= start, Tithe.date < end))
        .group_by(Tithe.date)
    ).all()
    diz_trans = db.execute(
        select(Transaction.date, func.sum(Transaction.amount))
        .join(Category, Transaction.category_id == Category.id)
        .where(and_(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type.in_((TYPE_IN, "RECEITA")),
            func.lower(Category.name).in_(("dízimo","dizimo"))
        ))
        .group_by(Transaction.date)
    ).all()
    oferta_trans = db.execute(
        select(Transaction.date, func.sum(Transaction.amount))
        .join(Category, Transaction.category_id == Category.id)
        .where(and_(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type.in_((TYPE_IN, "RECEITA")),
            func.lower(Category.name) == "oferta"
        ))
        .group_by(Transaction.date)
    ).all()

    by_date_diz_tit = defaultdict(float)
    for d, s in tithes: by_date_diz_tit[d] += float(s or 0.0)
    by_date_diz_tx = defaultdict(float)
    for d, s in diz_trans: by_date_diz_tx[d] += float(s or 0.0)
    by_date_ofe = defaultdict(float)
    for d, s in oferta_trans: by_date_ofe[d] += float(s or 0.0)

    all_dates = sorted(set(list(by_date_diz_tit.keys()) + list(by_date_diz_tx.keys()) + list(by_date_ofe.keys())))
    rows = []
    for d in all_dates:
        dz = max(float(by_date_diz_tit.get(d, 0.0)), float(by_date_diz_tx.get(d, 0.0)))  # [EQ FIX]
        ofe = float(by_date_ofe.get(d, 0.0))
        rows.append({"Data do Culto": d, "Dízimo": dz, "Oferta": ofe, "Total": dz + ofe})
    return pd.DataFrame(rows)

def _apply_entrada_summary_changes(cong_id: int, start: date, end: date, edited_df: pd.DataFrame):
    with SessionLocal() as db:
        cats_in = categories_for_type(db, TYPE_IN)
        cat_diz = next((c for c in cats_in if _norm(c.name) in ("dizimo","dízimo")), None)
        cat_ofe = next((c for c in cats_in if _norm(c.name) == "oferta"), None)
        if not (cat_diz and cat_ofe):
            st.error("Categorias 'Dízimo' e/ou 'Oferta' não encontradas."); return

        def current_sums():
            base = _entrada_summary_df(db, cong_id, start, end)
            base["Data do Culto"] = base["Data do Culto"].map(_to_date)
            bydate = {r["Data do Culto"]: (float(r["Dízimo"]), float(r["Oferta"])) for _, r in base.iterrows()}
            return bydate

        baseline = current_sums()
        edited = edited_df.copy()
        edited["Data do Culto"] = edited["Data do Culto"].map(_to_date)
        wanted = {r["Data do Culto"]: (float(_to_float_brl(r["Dízimo"])), float(_to_float_brl(r["Oferta"]))) for _, r in edited.iterrows()}

        all_dates = sorted(set(list(baseline.keys()) + list(wanted.keys())))
        for d in all_dates:
            cur_dz, cur_of = baseline.get(d, (0.0, 0.0))
            want_dz, want_of = wanted.get(d, (0.0, 0.0))

            # [EQ FIX]: equivalência → base de "outros" do dízimo é o MAIOR entre (tithes do dia) e (doações 'Dízimo' do dia, exceto ajustes)
            sum_dz_tithes = float(db.scalar(
                select(func.coalesce(func.sum(Tithe.amount), 0.0))
                .where(and_(Tithe.congregation_id == cong_id, Tithe.date == d))
            ) or 0.0)
            sum_dz_tx_no_adj = float(db.scalar(
                select(func.coalesce(func.sum(Transaction.amount), 0.0))
                .where(and_(
                    Transaction.congregation_id == cong_id,
                    Transaction.date == d,
                    Transaction.type.in_((TYPE_IN, "RECEITA")),
                    Transaction.category_id == cat_diz.id,
                    func.coalesce(Transaction.description, "") != ADJ_ENTRY_DESC
                ))
            ) or 0.0)
            sum_dz_others = max(sum_dz_tithes, sum_dz_tx_no_adj)  # [EQ FIX]

            sum_of_others = float(db.scalar(
                select(func.coalesce(func.sum(Transaction.amount), 0.0))
                .where(and_(
                    Transaction.congregation_id == cong_id,
                    Transaction.date == d,
                    Transaction.type.in_((TYPE_IN, "RECEITA")),
                    Transaction.category_id == cat_ofe.id,
                    func.coalesce(Transaction.description, "") != ADJ_ENTRY_DESC
                ))
            ) or 0.0)

            adj_dz_new = want_dz - sum_dz_others
            adj_of_new = want_of - sum_of_others

            adj_dz = db.scalar(select(Transaction).where(
                Transaction.congregation_id == cong_id, Transaction.date == d,
                Transaction.type.in_((TYPE_IN, "RECEITA")), Transaction.category_id == cat_diz.id,
                func.coalesce(Transaction.description, "") == ADJ_ENTRY_DESC
            ))
            adj_of = db.scalar(select(Transaction).where(
                Transaction.congregation_id == cong_id, Transaction.date == d,
                Transaction.type.in_((TYPE_IN, "RECEITA")), Transaction.category_id == cat_ofe.id,
                func.coalesce(Transaction.description, "") == ADJ_ENTRY_DESC
            ))

            if abs(adj_dz_new) < 0.0001:
                if adj_dz: db.delete(adj_dz)
            else:
                if adj_dz: adj_dz.amount = float(adj_dz_new); db.add(adj_dz)
                else:
                    db.add(Transaction(date=d, type=TYPE_IN, category_id=cat_diz.id,
                                       amount=float(adj_dz_new), description=ADJ_ENTRY_DESC,
                                       congregation_id=cong_id))

            if abs(adj_of_new) < 0.0001:
                if adj_of: db.delete(adj_of)
            else:
                if adj_of: adj_of.amount = float(adj_of_new); db.add(adj_of)
                else:
                    db.add(Transaction(date=d, type=TYPE_IN, category_id=cat_ofe.id,
                                       amount=float(adj_of_new), description=ADJ_ENTRY_DESC,
                                       congregation_id=cong_id))
        db.commit()

# ===================== EDITORES INLINE REUTILIZÁVEIS (com botão Salvar) =====================
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
    df_full = pd.DataFrame(rows)  # com _cong_id
    df_view = df_full.drop(columns=["_cong_id"])

    with SessionLocal() as db:
        cats = categories_for_type(db, tx_type)
        if tx_type == TYPE_IN:
            cats = [c for c in cats if "ajuste" not in _norm(c.name)]  # mantém sua regra
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
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
            "Descrição": st.column_config.TextColumn("Descrição", max_chars=200),
        },
        key=f"tx_editor_{titulo}",
    )

    def _save():
        _apply_tx_changes(df_full, edited_view, tx_type, default_cong_id)
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, f"tx_{titulo}")

def _editor_dizimos(tithes: List["Tithe"], titulo: str):
    if not tithes:
        st.caption("— Não há dízimos neste período.")
        return

    cong_ids = {int(t.congregation_id) for t in tithes if t.congregation_id}
    default_cong_id = list(cong_ids)[0] if len(cong_ids) == 1 else None

    rows = [{
        "ID": t.id, "Data": t.date, "Dizimista": t.tither_name,
        "Valor": float(t.amount), "Forma de Pagamento": t.payment_method or "",
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
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
            "Forma de Pagamento": st.column_config.SelectboxColumn("Forma de Pagamento", options=["Dinheiro","PIX","Cartão","Transferência",""], required=False),
        },
        key=f"tithe_editor_{titulo}",
    )

    def _save():
        _apply_tithe_changes(df_full, edited_view, default_cong_id)
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, f"tithe_{titulo}")

# ===== MISSÕES: Editores específicos =====
def _editor_missions_outflows(saidas: List["Transaction"], titulo: str, congs_all: List["Congregation"]):
    by_name = {c.name: c.id for c in congs_all}
    names_order = [c.name for c in order_congs_sede_first(congs_all)]

    rows = []
    if saidas:
        for t in saidas:
            rows.append({
                "ID": t.id,
                "Data": t.date,
                "Congregação": t.congregation.name if t.congregation else "Sede",
                "Descrição": (t.description or ""),
                "Valor": float(t.amount),
                "_cong_id": int(t.congregation_id or 0),
            })
    else:
        rows = [{
            "ID": None,
            "Data": today_bahia(),
            "Congregação": names_order[0] if names_order else "",
            "Descrição": "",
            "Valor": 0.0,
            "_cong_id": (by_name.get(names_order[0]) if names_order else 0),
        }]

    df_full = pd.DataFrame(rows)
    df_view = df_full.drop(columns=["_cong_id"])

    edited_view = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "ID": st.column_config.Column("ID", disabled=True),
            "Data": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
            "Congregação": st.column_config.SelectboxColumn("Congregação", options=names_order, required=True),
            "Descrição": st.column_config.TextColumn("Descrição", max_chars=200),
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
        },
        key=f"missoes_out_{titulo}",
    )

    # === [BLOCO 4: Total de SAÍDAS de Missões (mês corrente) em destaque] ===
    try:
        _total_out_missions = 0.0
        if isinstance(edited_view, pd.DataFrame) and not edited_view.empty and ("Valor" in edited_view.columns):
            _ev = edited_view.copy()
            _ev["Valor"] = _ev["Valor"].map(_to_float_brl)
            _total_out_missions = float(_ev["Valor"].sum())
    except Exception:
        _total_out_missions = 0.0

    st.metric(
        "Total de SAÍDAS de Missões (mês corrente)",
        format_currency(_total_out_missions)
    )
    # === [FIM DO BLOCO 4] ===

    def _save():
        # mapear 'Congregação' -> _cong_id para persistir corretamente
        with_id = edited_view.copy()
        with_id["_cong_id"] = with_id["Congregação"].map(lambda x: int(by_name.get(str(x).strip(), 0)))
        _apply_tx_changes(
            df_full.assign(**{"Categoria": "Missões (Saída)"}),
            with_id.assign(**{"Categoria": "Missões (Saída)"}),
            TYPE_OUT,
            default_cong_id=None  # agora a congregação vem da coluna
        )
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, f"missoes_out_{titulo}")


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
        if abs(val) > 0.0001:
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
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
        },
        key=f"missoes_in_agg_{titulo}",
    )

    # === [BLOCO 3: Total de ENTRADAS de Missões (mês corrente) em destaque] ===
    try:
        _total_in_missions = 0.0
        if isinstance(edited_view, pd.DataFrame) and not edited_view.empty and ("Valor" in edited_view.columns):
            _ev = edited_view.copy()
            _ev["Valor"] = _ev["Valor"].map(_to_float_brl)
            _total_in_missions = float(_ev["Valor"].sum())
    except Exception:
        _total_in_missions = 0.0

    st.metric(
        "Total de ENTRADAS de Missões (mês corrente)",
        format_currency(_total_in_missions)
    )
    # === [FIM DO BLOCO 3] ===

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

            desired = {}
            for _, r in edited_view.iterrows():
                name = str(r.get("Congregação","")).strip()
                if not name: continue
                cid = by_name.get(name)
                if not cid: continue
                desired[cid] = float(_to_float_brl(r.get("Valor", 0.0)))

            all_cids = set(list(base_others.keys()) + list(desired.keys()))
            for cid in all_cids:
                want = float(desired.get(cid, 0.0))
                others = float(base_others.get(cid, 0.0))
                new_adj = want - others
                exist = adj_map.get(cid)
                if abs(new_adj) < 0.0001:
                    if exist: db.delete(exist)
                else:
                    if exist: 
                        exist.amount = float(new_adj); db.add(exist)
                    else:
                        db.add(Transaction(
                            date=start, type=TYPE_IN, category_id=cat_miss.id, amount=float(new_adj),
                            description=ADJ_MISS_IN_DESC, congregation_id=int(cid)
                        ))
            db.commit()
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, f"missoes_in_{titulo}")


# ====== EDITORES AGREGADOS (TODAS AS CONGREGAÇÕES) — ENTRADAS / SAÍDAS ======
def _editor_entradas_agg_all(congs_all: List["Congregation"], start: date, end: date):
    with SessionLocal() as db:
        rows = []
        for c in congs_all:
            totals = _collect_month_data(db, c.id, start, end)["totals"]
            rows.append({"Congregação": c.name, "Total (R$)": float(totals["entradas_total_sem_missoes"])})
        df_view = pd.DataFrame(rows).sort_values("Total (R$)", ascending=False).reset_index(drop=True)

    edited = st.data_editor(
        df_view, use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={
            "Congregação": st.column_config.SelectboxColumn("Congregação", options=[c.name for c in order_congs_sede_first(congs_all)], required=True),
            "Total (R$)": st.column_config.NumberColumn("Total (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
        },
        key="agg_in_all_editor",
    )

    def _save():
        with SessionLocal() as db:
            by_name = {c.name: c.id for c in congs_all}
            cat_oferta = db.scalar(select(Category).where(func.lower(Category.name) == "oferta"))
            if not cat_oferta:
                st.error("Categoria 'Oferta' não encontrada."); return

            def base_others_total(cid: int) -> float:
                # [EQ FIX]: equivalência de dízimos no total mensal
                tithe_sum = float(db.scalar(
                    select(func.coalesce(func.sum(Tithe.amount), 0.0))
                    .where(and_(Tithe.congregation_id == cid, Tithe.date >= start, Tithe.date < end))
                ) or 0.0)
                donations_sum_ex_adj = float(db.scalar(  # todas doações (exceto ajustes agregados)
                    select(func.coalesce(func.sum(Transaction.amount), 0.0))
                    .join(Category, Transaction.category_id == Category.id)
                    .where(
                        Transaction.congregation_id == cid,
                        Transaction.date >= start, Transaction.date < end,
                        Transaction.type.in_((TYPE_IN, "RECEITA")),
                        func.coalesce(Transaction.description, "") != ADJ_ENTRY_AGG_DESC
                    )
                ) or 0.0)
                donations_missoes = float(db.scalar(
                    select(func.coalesce(func.sum(Transaction.amount), 0.0))
                    .join(Category, Transaction.category_id == Category.id)
                    .where(
                        Transaction.congregation_id == cid,
                        Transaction.date >= start, Transaction.date < end,
                        Transaction.type.in_((TYPE_IN, "RECEITA")),
                        func.lower(Category.name).in_(("missões","missoes")),
                        func.coalesce(Transaction.description, "") != ADJ_ENTRY_AGG_DESC
                    )
                ) or 0.0)
                dizimo_tx_sum = float(db.scalar(
                    select(func.coalesce(func.sum(Transaction.amount), 0.0))
                    .join(Category, Transaction.category_id == Category.id)
                    .where(
                        Transaction.congregation_id == cid,
                        Transaction.date >= start, Transaction.date < end,
                        Transaction.type.in_((TYPE_IN, "RECEITA")),
                        func.lower(Category.name).in_(("dízimo","dizimo")),
                        func.coalesce(Transaction.description, "") != ADJ_ENTRY_AGG_DESC
                    )
                ) or 0.0)
                non_diz_non_miss = donations_sum_ex_adj - donations_missoes - dizimo_tx_sum
                diz_final = max(tithe_sum, dizimo_tx_sum)  # [EQ FIX]
                return diz_final + non_diz_non_miss

            existing_adj = {(t.congregation_id): t for t in db.scalars(
                select(Transaction).where(
                    Transaction.date == start,
                    Transaction.type.in_((TYPE_IN, "RECEITA")),
                    Transaction.category_id == cat_oferta.id,
                    func.coalesce(Transaction.description, "") == ADJ_ENTRY_AGG_DESC
                )
            ).all()}

            desired = {}
            for _, r in edited.iterrows():
                name = str(r.get("Congregação","")).strip()
                if not name: continue
                cid = by_name.get(name)
                if cid is None: continue
                desired[cid] = float(_to_float_brl(r.get("Total (R$)", 0.0)))

            for cid, want in desired.items():
                others = base_others_total(cid)
                new_adj = want - others
                adj = existing_adj.get(cid)
                if abs(new_adj) < 0.0001:
                    if adj: db.delete(adj)
                else:
                    if adj:
                        adj.amount = float(new_adj); db.add(adj)
                    else:
                        db.add(Transaction(
                            date=start, type=TYPE_IN, category_id=cat_oferta.id,
                            amount=float(new_adj), description=ADJ_ENTRY_AGG_DESC,
                            congregation_id=int(cid)
                        ))
            db.commit()
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, "agg_in_all")

def _editor_saidas_agg_all(congs_all: List["Congregation"], start: date, end: date):
    with SessionLocal() as db:
        rows = []
        for c in congs_all:
            totals = _collect_month_data(db, c.id, start, end)["totals"]
            rows.append({"Congregação": c.name, "Total Saídas (R$)": float(totals["saidas_total"])})
        df_view = pd.DataFrame(rows).sort_values("Total Saídas (R$)", ascending=False).reset_index(drop=True)

    edited = st.data_editor(
        df_view, use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={
            "Congregação": st.column_config.SelectboxColumn("Congregação", options=[c.name for c in order_congs_sede_first(congs_all)], required=True),
            "Total Saídas (R$)": st.column_config.NumberColumn("Total Saídas (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
        },
        key="agg_out_all_editor",
    )

    def _save():
        with SessionLocal() as db:
            by_name = {c.name: c.id for c in congs_all}
            cats_out = categories_for_type(db, TYPE_OUT)
            if not cats_out:
                st.error("Não há categorias de saída cadastradas."); return
            cat_out_id = cats_out[0].id

            def base_others_total(cid: int) -> float:
                return float(db.scalar(
                    select(func.coalesce(func.sum(Transaction.amount), 0.0))
                    .where(
                        Transaction.congregation_id == cid,
                        Transaction.date >= start, Transaction.date < end,
                        Transaction.type.in_((TYPE_OUT, "DESPESA")),
                        func.coalesce(Transaction.description, "") != ADJ_OUT_AGG_DESC
                    )
                ) or 0.0)

            existing_adj = {(t.congregation_id): t for t in db.scalars(
                select(Transaction).where(
                    Transaction.date == start,
                    Transaction.type.in_((TYPE_OUT, "DESPESA")),
                    Transaction.category_id == cat_out_id,
                    func.coalesce(Transaction.description, "") == ADJ_OUT_AGG_DESC
                )
            ).all()}

            desired = {}
            for _, r in edited.iterrows():
                name = str(r.get("Congregação","")).strip()
                if not name: continue
                cid = by_name.get(name); 
                if cid is None: continue
                desired[cid] = float(_to_float_brl(r.get("Total Saídas (R$)", 0.0)))

            for cid, want in desired.items():
                others = base_others_total(cid)
                new_adj = want - others
                adj = existing_adj.get(cid)
                if abs(new_adj) < 0.0001:
                    if adj: db.delete(adj)
                else:
                    if adj:
                        adj.amount = float(new_adj); db.add(adj)
                    else:
                        db.add(Transaction(
                            date=start, type=TYPE_OUT, category_id=cat_out_id,
                            amount=float(new_adj), description=ADJ_OUT_AGG_DESC,
                            congregation_id=int(cid)
                        ))
            db.commit()
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, "agg_out_all")

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

    total_dizimos_tithe = sum(float(t.amount) for t in tithes)
    total_dizimos_trans = sum(float(t.amount) for t in tx_in if _is_dizimo_tx(t))
    total_dizimos_final = max(total_dizimos_tithe, total_dizimos_trans)  # [EQ FIX] já existia essa lógica global

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
            "dizimos_nominais": total_dizimos_tithe,
            "dizimos_doacoes": total_dizimos_trans,
            "ofertas": total_ofertas,
            "missoes": total_missoes,
            "entradas_outros": total_entradas_outros,
            "entradas_total_sem_missoes": total_geral_entradas_outros if (total_geral_entradas_outros:=total_geral_entradas_sem_missoes) else total_geral_entradas_sem_missoes,  # preserva nome antigo
            "saidas_total": total_saidas,
            "saldo": saldo
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

        if user.role == "SEDE":
            congs_ordered = order_congs_sede_first(congs)
            cong_sel = st.selectbox("Selecione a congregação", [c.name for c in congs_ordered], key="lan_cong_sel")
            cong_obj = next(c for c in congs_ordered if c.name == cong_sel)
        else:
            cong_obj = congs[0]

        st.markdown(f"<div class='cong-title'>CONGREGAÇÃO: {cong_obj.name.upper()}</div>", unsafe_allow_html=True)

        # ENTRADA
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar ENTRADA (Doação)")
        with st.form("form_entrada", clear_on_submit=True):
            c1,c2,c3 = st.columns([1.1,1.4,2])
            ent_data = st.date_input("Data do Culto", value=today_bahia(), key="ent_data", format="DD/MM/YYYY")
            with c2:
                cats_in = categories_for_type(db, TYPE_IN)
                cats_in = [c for c in cats_in if "ajuste" not in _norm(c.name)]
                cat_names_in = [c.name for c in cats_in] or ["—"]
                desired = ["Dízimo", "Oferta", "Missões"]
                desired_norm = [_norm(x) for x in desired]
                top = [n for n in cat_names_in if _norm(n) in desired_norm]
                rest = [n for n in cat_names_in if _norm(n) not in desired_norm]
                cat_display = top + rest
                ent_cat = st.selectbox("Categoria (ordem: Dízimo, Oferta, Missões)", cat_display, key="ent_cat")
            ent_desc = st.text_input("Descrição (opcional)", key="ent_desc")
            ent_flag_missoes = _norm(ent_cat) == "oferta" and st.checkbox("Oferta de missões?", key="ent_flag_missoes")
            ent_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="ent_valor")

            if st.form_submit_button("Salvar ENTRADA", type="primary"):
                with SessionLocal() as _db:
                    cat_name = ent_cat
                    if ent_flag_missoes:
                        cat_name = "Missões"
                        if not _db.scalar(select(Category).where(Category.name == "Missões")):
                            _db.add(Category(name="Missões", type=TYPE_IN)); _db.commit()
                    cat_obj = _db.scalar(select(Category).where(Category.name == cat_name))
                    if not cat_obj:
                        st.error("Informe a categoria."); return
                    _db.add(Transaction(
                        date=ent_data, type=TYPE_IN, category_id=cat_obj.id,
                        amount=ent_valor, description=(ent_desc or None),
                        congregation_id=cong_obj.id, payment_method=None
                    ))
                    _db.commit()
                    st.success("Entrada registrada.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # DÍZIMOS
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Salvar DIZIMISTA")
        with st.form("form_dizimo", clear_on_submit=True):
            dz_data = st.date_input("Data do Culto", value=today_bahia(), key="dz_data", format="DD/MM/YYYY")
            dz_nome = st.text_input("Nome do dizimista", key="dz_nome")
            dz_valor = st.number_input("Valor dízimo (R$)", min_value=0.0, step=1.0, format="%.2f", key="dz_valor")
            dz_payment = st.selectbox("Forma de Pagamento", ["Dinheiro", "PIX"], key="dz_payment_method")

            if st.form_submit_button("Salvar DIZIMISTA", type="primary"):
                nome = (dz_nome or "").strip()
                if not nome:
                    st.error("Informe o nome do dizimista."); return
                with SessionLocal() as _db:
                    _db.add(Tithe(
                        date=dz_data, tither_name=nome, amount=float(dz_valor),
                        congregation_id=cong_obj.id, payment_method=dz_payment
                    ))
                    _db.commit()
                    st.success("Dízimo registrado.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # SAÍDA
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar SAÍDA")
        with st.form("form_saida", clear_on_submit=True):
            sai_data = st.date_input("Data", value=today_bahia(), key="sai_data", format="DD/MM/YYYY")
            cats_out = categories_for_type(db, TYPE_OUT)
            sai_cat = st.selectbox("Tipo da saída (Categoria)", [c.name for c in cats_out] or ["—"], key="sai_cat")
            sai_desc = st.text_input("Descrição (opcional)", key="sai_desc")
            sai_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="sai_valor")

            if st.form_submit_button("Salvar SAÍDA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name == sai_cat))
                    if not cat_obj:
                        st.error("Informe o tipo de saída."); return
                    _db.add(Transaction(
                        date=sai_data, type=TYPE_OUT, category_id=cat_obj.id,
                        amount=sai_valor, description=(sai_desc or None),
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
            esc = st.selectbox("Escopo", esc_opt, key="re_in_allopt")
            is_all = (esc == "Todas as congregações")
            cong_obj = None if is_all else next(c for c in ordered if c.name == esc)
        else:
            is_all = False
            cong_obj = congs[0] if congs else None

        # === MODO SEDE: TODAS AS CONGREGAÇÕES ===
        if is_all:
            st.info("Escopo: **Todas as congregações** — edite o total de entradas mensal por congregação abaixo.")
            _editor_entradas_agg_all(ordered, start, end)

            # Total geral de ENTRADAS (todas as congregações) em destaque
            with SessionLocal() as _db_tot_in:
                total_geral_in = 0.0
                for _c in ordered:
                    _t = _collect_month_data(_db_tot_in, _c.id, start, end)["totals"]
                    total_geral_in += float(_t["entradas_total_sem_missoes"])
            st.metric("Total geral de entradas (todas as congregações)", format_currency(total_geral_in))
            return

        # === CONGREGAÇÃO ESPECÍFICA ===
        if not cong_obj:
            st.info("Selecione uma congregação."); 
            return

        st.info(f"Escopo: **{cong_obj.name}**")

        base_df = _entrada_summary_df(db, cong_obj.id, start, end)
        if base_df.empty:
            base_df = pd.DataFrame(columns=["Data do Culto","Dízimo","Oferta","Total"])
        view_df = base_df.copy()

        edited = st.data_editor(
            view_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Data do Culto": st.column_config.DateColumn("Data do Culto", required=True, format="DD/MM/YYYY"),
                "Dízimo": st.column_config.NumberColumn("Dízimo (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
                "Oferta": st.column_config.NumberColumn("Oferta (R$)", min_value=-999999999.0, step=1.0, format="R$ %.2f"),
                "Total": st.column_config.NumberColumn("Total (R$)", disabled=True, format="R$ %.2f"),
            },
            key="re_entrada_sum_editor",
        )

        # Recalcula a coluna Total (visual)
        if not edited.empty:
            try:
                edited["Total"] = edited["Dízimo"].map(_to_float_brl) + edited["Oferta"].map(_to_float_brl)
            except Exception:
                pass

        # === TOTAL DO MÊS EM DESTAQUE (abaixo da tabela) ===
        try:
            _sum_total_mes = 0.0
            if isinstance(edited, pd.DataFrame) and not edited.empty and {"Dízimo","Oferta"}.issubset(set(edited.columns)):
                _calc = edited.copy()
                _calc["Dízimo"] = _calc["Dízimo"].map(_to_float_brl)
                _calc["Oferta"] = _calc["Oferta"].map(_to_float_brl)
                _calc["Total"]  = _calc["Dízimo"] + _calc["Oferta"]
                _sum_total_mes = float(_calc["Total"].sum())
        except Exception:
            _sum_total_mes = 0.0

        st.metric("Total de Entradas (Dízimo + Oferta) no mês", format_currency(_sum_total_mes))

        # Botão Salvar (persiste no banco via _apply_entrada_summary_changes)
        def _save_sum():
            _apply_entrada_summary_changes(cong_obj.id, start, end, edited)
            st.toast("💾 Alterações salvas.", icon="✅")
            st.rerun()

        _save_btn(_save_sum, "entrada_sum")

        # === [BLOCO: Apagar linhas do resumo de entrada] — logo após o botão Salvar ===
        if isinstance(edited, pd.DataFrame) and not edited.empty and ("Data do Culto" in edited.columns):
            try:
                # Converte as datas e cria rótulos "dd/mm/aaaa"
                _datas_ord = sorted({_to_date(d) for d in edited["Data do Culto"].tolist() if pd.notna(d)})
                _label_map = {format_date(d): d for d in _datas_ord}  # "dd/mm/aaaa" -> date
                _rotulos = list(_label_map.keys())
            except Exception:
                _rotulos = []
                _label_map = {}

            _sel_del = st.multiselect(
                "Selecione as datas que deseja APAGAR desta tabela-resumo",
                options=_rotulos,
                key="re_entrada_sum_del_dates"
            )

            def _delete_selected_rows():
                if not _sel_del:
                    st.warning("Selecione ao menos uma data para apagar.")
                    return

                # Datas selecionadas (tipo date)
                to_drop = {_label_map[x] for x in _sel_del if x in _label_map}

                # Remove linhas com essas datas e salva
                edited_clean = edited.copy()
                edited_clean["Data do Culto"] = edited_clean["Data do Culto"].map(_to_date)
                edited_clean = edited_clean[~edited_clean["Data do Culto"].isin(to_drop)]

                _apply_entrada_summary_changes(cong_obj.id, start, end, edited_clean)
                st.toast("🗑️ Linhas apagadas com sucesso.", icon="✅")
                st.rerun()

            st.button(
                "🗑️ Apagar linhas selecionadas",
                type="secondary",
                on_click=_delete_selected_rows,
                key="btn_del_entrada_sum"
            )
        # === [FIM: Apagar linhas] ===

        st.divider()

        # Download CSV do período
        csv = edited.assign(**{
            "Data do Culto": edited["Data do Culto"].map(lambda d: _to_date(d).strftime("%Y-%m-%d")),
        }).to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Baixar CSV (Entradas do período)",
            data=csv,
            file_name=f"entradas_resumo_{start.strftime('%Y-%m')}.csv",
            mime="text/csv"
        )

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

        if is_all:
            st.info("Escopo: **Todas as congregações** — edite o total mensal de saídas por congregação abaixo.")
            _editor_saidas_agg_all(ordered, start, end)

            # === [BLOCO 7: Total geral de SAÍDAS (todas as congregações)] ===
            with SessionLocal() as _db_tot_out:
                total_geral_out = 0.0
                for _c in ordered:
                    _t = _collect_month_data(_db_tot_out, _c.id, start, end)["totals"]
                    total_geral_out += float(_t["saidas_total"])
            st.metric("Total geral de SAÍDAS (todas as congregações)", format_currency(total_geral_out))
            # === [FIM DO BLOCO 7] ===

            st.divider()
            with SessionLocal() as db2:
                rows = []
                for c in ordered:
                    total = _collect_month_data(db2, c.id, start, end)["totals"]["saidas_total"]
                    rows.append({"Congregação": c.name, "Total Saídas (R$)": float(total)})
            csv = pd.DataFrame(rows).to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Baixar CSV (Saídas por congregação)", data=csv, file_name=f"saidas_congregacoes_{start.strftime('%Y-%m')}.csv", mime="text/csv")
            return

        if not cong_obj:
            st.info("Sem congregação vinculada."); return
        st.info(f"Escopo: **{cong_obj.name}**")

        q = select(Transaction).options(joinedload(Transaction.category)).where(
            Transaction.date >= start, Transaction.date < end, Transaction.type.in_(("SAÍDA", "DESPESA")),
            Transaction.congregation_id == cong_obj.id
        )
        txs = db.scalars(q).all()

        total_saidas = sum(float(t.amount) for t in txs)
        st.metric("Total de saídas", format_currency(total_saidas))

        st.divider()
        _editor_lancamentos(txs, "Saídas do período (editar na tabela)", tx_type_hint=TYPE_OUT)

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
    doc = SimpleDocTemplate(buf, pagesize=portrait(A4), leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('title', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=12, textColor=colors.black, spaceAfter=12)

    story = []
    story.append(Paragraph("Relatório de Pesquisa de Dizimistas", title_style))
    story.append(Paragraph(f"Ano: {ano_pesq} | Congregação: {cong_sel} | Mês: {mes_sel}", subtitle_style))
    if (nome_q or "").strip():
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
    tbl.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("ALIGN", (4, 1), (4, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
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
            all_tz = db.scalars(select(Tithe).where(Tithe.date >= start, Tithe.date < end)).all()
            by_cong = defaultdict(lambda: {"qtd":0, "valor":0.0})
            for t in all_tz:
                k = t.congregation.name
                by_cong[k]["qtd"] += 1
                by_cong[k]["valor"] += float(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Qtde de dizimistas": v["qtd"], "Total (R$)": format_currency(v["valor"])} for k,v in sorted(by_cong.items())])
            st.dataframe(df, use_container_width=True, hide_index=True, height=200)
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

            st.subheader("Resumo de Pagamentos de Dízimos")
            cols_metrics = st.columns(max(1, len(tithes_by_payment)))
            for i, (method, datax) in enumerate(tithes_by_payment.items()):
                cols_metrics[i].metric(f"Total ({method})", format_currency(datax["total"]), f"{datax['count']} dízimos")

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
                agg[key] = {"nome_display": t.tither_name, "congregacao": t.congregation.name if t.congregation else "—",
                            "total_ano": 0.0, "meses": set(), "primeiro": t.date, "ultimo": t.date}
            agg[key]["total_ano"] += float(t.amount)
            agg[key]["meses"].add(t.date.month)
            if t.date < agg[key]["primeiro"]: agg[key]["primeiro"] = t.date
            if t.date > agg[key]["ultimo"]: agg[key]["ultimo"] = t.date

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
                st.caption("Nomes que dizimaram por PIX (neste filtro):")
                st.write(", ".join(pix_names))

            csv = df_show.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Baixar CSV da pesquisa", data=csv, file_name=f"pesquisa_dizimistas_{ano_pesq}.csv", mime="text/csv")
            pdf_data = build_dizimista_search_pdf(df_show.assign(**{"Total no ano (R$)": df_show["Total no ano (R$)"]}), ano_pesq, cong_sel, mes_sel, nome_q)
            st.download_button("⬇️ Baixar PDF da pesquisa", data=pdf_data, file_name=f"pesquisa_dizimistas_{ano_pesq}.pdf", mime="application/pdf")
        else:
            st.caption("Nenhum resultado para os filtros informados.")

# ===================== PDFs =====================
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

        # [EQ FIX]: calcular Dízimo por data usando equivalência (max entre tithes e tx 'Dízimo'), e Oferta somada normalmente
        tithe_by_date = defaultdict(float)
        for t in data["tithes"]:
            tithe_by_date[t.date] += float(t.amount)

        diz_tx_by_date = defaultdict(float)
        oferta_by_date = defaultdict(float)
        for t in data["tx_in"]:
            if t.category and _norm(t.category.name) in ("dizimo","dízimo"):
                diz_tx_by_date[t.date] += float(t.amount)
            elif t.category and _norm(t.category.name) == "oferta":
                oferta_by_date[t.date] += float(t.amount)

        all_dates = sorted(set(list(tithe_by_date.keys()) + list(diz_tx_by_date.keys()) + list(oferta_by_date.keys())))
        tx_in_data = [["Data do Culto", "Dízimo", "Oferta", "Total"]]
        for d in all_dates:
            dz = max(float(tithe_by_date.get(d, 0.0)), float(diz_tx_by_date.get(d, 0.0)))  # [EQ FIX]
            ofe = float(oferta_by_date.get(d, 0.0))
            tx_in_data.append([d.strftime("%d/%m/%Y"), format_currency(dz), format_currency(ofe), format_currency(dz + ofe)])

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

    story.append(Paragraph("2. Saídas", heading_style))
    if len(tx_out_data) > 1:
        tbl_out = Table(tx_out_data, colWidths=[2.5*cm, 4.5*cm, 7.5*cm, 3.5*cm])
        tbl_out.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (3, 1), (3, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ]))
        story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída registrado.", styles['Normal']))
    story.append(Spacer(1, 1*cm))

    story.append(Paragraph("3. Resumo Financeiro do Mês", heading_style))
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

    story: List = []
    story.append(Paragraph("Relatório Mensal", title_style))
    story.append(Paragraph(f"Mês de Referência: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 1*cm))

    table_data = [["Congregação", "Entradas (D+O+Outras)", "Saídas", "Saldo"]]
    total_entradas = total_saidas = total_saldo = 0.0

    for c_name, entradas, saidas, saldo, _missoes in agg_total:
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
            st.info("Sem congregação vinculada.")
            return

        agg_total = []
        if is_all:
            for c in ordered:
                totals = _collect_month_data(db, c.id, start, end)["totals"]
                agg_total.append((
                    c.name,
                    totals["entradas_total_sem_missoes"],
                    totals["saidas_total"],
                    totals["saldo"],
                    totals["missoes"]
                ))
        elif congs:
            cong_obj = congs[0]
            totals = _collect_month_data(db, cong_obj.id, start, end)["totals"]
            agg_total.append((
                cong_obj.name,
                totals["entradas_total_sem_missoes"],
                totals["saidas_total"],
                totals["saldo"],
                totals["missoes"]
            ))

        # ==== SEDE (todas as congregações) ====
        if user.role == "SEDE":
            df_rank = pd.DataFrame([{
                "Congregação": n,
                "Entradas (D+O + Outras)": v,
                "Saídas": s,
                "Saldo": sal
            } for (n, v, s, sal, _m) in agg_total])

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
                    }),
                    use_container_width=True, hide_index=True, height=200
                )
            else:
                st.caption("Sem dados neste mês.")

            # === [BLOCO 9: Totais gerais — Entradas, Saídas e Saldo (todas as congregações)] ===
            try:
                _tot_in    = sum(float(v)    for (_n, v, _s, _sal, _m) in agg_total)
                _tot_out   = sum(float(_s)   for (_n, _v, _s, _sal, _m) in agg_total)
                _tot_saldo = sum(float(_sal) for (_n, _v, _s, _sal, _m) in agg_total)
            except Exception:
                _tot_in = _tot_out = _tot_saldo = 0.0

            c1, c2, c3 = st.columns(3)
            c1.metric("Total de Entradas (todas as congregações)", format_currency(_tot_in))
            c2.metric("Total de Saídas (todas as congregações)", format_currency(_tot_out))
            c3.metric("Saldo (todas as congregações)", format_currency(_tot_saldo))
            # === [FIM BLOCO 9] ===

            st.divider()
            st.subheader("Relatório Consolidado Mensal")
            st.download_button(
                "⬇️ Baixar PDF do Relatório Geral",
                data=build_consolidated_pdf(agg_total, ref),
                file_name=f"relatorio_mensal_{start.strftime('%Y-%m')}.pdf",
                mime="application/pdf",
                key=f"dl_pdf_relatorio_geral_{start.strftime('%Y_%m')}"
            )

        # ==== Tesoureiro (apenas sua congregação) ====
        if user.role != "SEDE" and agg_total:
            st.divider()
            st.subheader("Resumo Financeiro Mensal")

            # Totais detalhados para a congregação do usuário (dízimos e ofertas separadas)
            with SessionLocal() as _db_vg:
                _tot = _collect_month_data(_db_vg, cong_obj.id, start, end)["totals"]

            _dz = float(_tot.get("dizimos", 0.0))
            _of = float(_tot.get("ofertas", 0.0))
            _dz_of = _dz + _of
            _sa = float(_tot.get("saidas_total", 0.0))
            _saldo = float(_tot.get("saldo", 0.0))

            df_summary_5 = pd.DataFrame([{
                "Dízimos Total": format_currency(_dz),
                "Ofertas Total": format_currency(_of),
                "Dízimos + Ofertas": format_currency(_dz_of),
                "Total Saídas": format_currency(_sa),
                "Saldo": format_currency(_saldo),
            }])

            st.dataframe(df_summary_5, use_container_width=True, hide_index=True)

        # ==== PDF completo por congregação ====
        st.subheader("Prestação de contas (PDF completo)")
        if user.role == "SEDE":
            sel = st.selectbox(
                "Congregação",
                [c.name for c in ordered],
                key=f"pc_cong_sel_vg_{start.strftime('%Y_%m')}"
            )
            cong_obj = next(c for c in ordered if c.name == sel)
        else:
            cong_obj = ordered[0]

        st.download_button(
            "⬇️ Baixar PDF do mês (completo)",
            data=build_full_statement_pdf(cong_obj.id, cong_obj.name, ref),
            file_name=f"prestacao_{_norm(cong_obj.name)}_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf",
            key=f"dl_pdf_prestacao_{_norm(cong_obj.name)}_{start.strftime('%Y_%m')}"
        )

        # === [FIM BLOCO 8] ===

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

# ===================== COLETA MISSÕES =====================
def _collect_missions_data(db: Session, start: date, end: date, only_cong_id: Optional[int] = None):
    q_in = select(Transaction).options(joinedload(Transaction.congregation), joinedload(Transaction.category)).where(
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type == TYPE_IN,
        Transaction.category.has(Category.name.in_(("Missões", "missões")))
    ).order_by(Transaction.date)
    if only_cong_id:
        q_in = q_in.where(Transaction.congregation_id == only_cong_id)
    entradas_missoes = db.scalars(q_in).all()
    
    q_out = select(Transaction).options(joinedload(Transaction.congregation), joinedload(Transaction.category)).where(
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type == TYPE_OUT,
        Transaction.category.has(Category.name.in_(("Missões (Saída)", "missões (saída)")))
    ).order_by(Transaction.date)
    if only_cong_id:
        q_out = q_out.where(Transaction.congregation_id == only_cong_id)
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
            entradas_data.append([t.date.strftime("%d/%m/%Y"), t.congregation.name, format_currency(float(t.amount))])
        tbl_in = Table(entradas_data, colWidths=[3*cm, 9*cm, 5*cm])
        tbl_in.setStyle(table_style)
        story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada de missões registrada.", styles['Normal']))

    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("Saídas de Missões", heading_style))
    if saidas:
        saidas_data = [["Data", "Congregação", "Descrição", "Valor (R$)"]]
        for t in saidas:
            saidas_data.append([t.date.strftime("%d/%m/%Y"), t.congregation.name if t.congregation else "—", t.description or "—", format_currency(float(t.amount))])
        tbl_out = Table(saidas_data, colWidths=[3*cm, 6*cm, 7*cm, 3*cm])
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

# ======== Páginas de Missões ========
def page_relatorio_missoes(user: "User"):
    if user.role not in ["SEDE", "TESOUREIRO MISSIONÁRIO"]:
        st.warning("🔒 Acesso negado. Apenas usuários `SEDE` ou `TESOUREIRO MISSIONÁRIO` podem acessar este relatório.")
        return
    
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown("<h1 class='page-title'>Relatório de Missões</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        st.info("Escopo: **Todas as congregações**")
        
        congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()

        st.subheader("Entradas de Missões — por Congregação (editar na tabela)")
        _editor_missions_entries_agg(congs_all, start, end, "missoes_entradas_agg")

        st.subheader("Saídas de Missões (editar na tabela)")
        _, saidas_missoes = _collect_missions_data(db, start, end)
        _editor_missions_outflows(saidas_missoes, "missoes_saidas", congs_all)

        st.divider()
        st.subheader("Gerar Relatório de Missões (PDF)")
        entradas_missoes, saidas_missoes = _collect_missions_data(db, start, end)
        st.download_button(
            "⬇️ Baixar Relatório de Missões (PDF)",
            data=build_missions_report_pdf(ref, entradas_missoes, saidas_missoes),
            file_name=f"relatorio_missoes_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf"
        )

def page_relatorio_missoes_congregacao(user: "User"):
    # Página do TESOUREIRO (congregações) — mostra APENAS o SALDO do mês
    if user.role != "TESOUREIRO":
        st.warning("🔒 Acesso restrito aos usuários TESOUREIRO (congregações).")
        return

    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)
        st.markdown("<h1 class='page-title'>Relatório de Missões (Minha Congregação)</h1>", unsafe_allow_html=True)

        ref = get_month_selector()
        start, end = month_bounds(ref)

        if not user.congregation_id:
            st.info("Sua conta não está vinculada a uma congregação.")
            return

        # Coleta apenas os lançamentos da congregação do usuário
        entradas, saidas = _collect_missions_data(db, start, end, only_cong_id=user.congregation_id)
        total_in = sum(float(t.amount) for t in entradas)
        total_out = sum(float(t.amount) for t in saidas)
        saldo_mes = float(total_in - total_out)

        # === Somente SALDO em destaque ===
        st.metric("Saldo de Missões (mês corrente)", format_currency(saldo_mes))

        st.divider()
        st.subheader("Baixar Relatório (PDF)")
        st.download_button(
            "⬇️ Baixar PDF (Missões da minha congregação)",
            data=build_missions_report_pdf(ref, entradas, saidas),
            file_name=f"relatorio_missoes_congregacao_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf"
        )

# ===================== PAGE: CADASTRO =====================
def page_cadastro(user: "User"):
    if not is_admin_general(user):
        st.warning("🔒 Apenas o **administrador geral** (admin) pode acessar o Cadastro.")
        return
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)

        # (restante do cadastro preservado — sem editores que precisem de botão)

        # Congregações
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
                linhas = [l.strip() for l in (mass_text or "").splitlines()]
                linhas = [l for l in linhas if l]
                if not linhas:
                    st.warning("Informe ao menos um nome.")
                else:
                    inseridas = 0; repetidas = 0
                    for nome in linhas:
                        if db.scalar(select(Congregation).where(Congregation.name == nome)):
                            repetidas += 1
                        else:
                            db.add(Congregation(name=nome)); inseridas += 1
                    db.commit()
                    st.success(f"Inseridas: {inseridas} | Já existiam: {repetidas}")
                    st.rerun()

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
            cat_type = st.selectbox("Tipo", ["DOAÇÃO", "SAÍDA"], key="cad_cat_type")
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
        if u_role == "TESOUREIRO MISSIONÁRIO":
            try:
                index = cong_options.index("Sede")
            except ValueError:
                index = 0
            u_cong_name = st.selectbox("Congregação (vinculada a Saídas de Missões)", cong_options, index=index, key="cad_user_cong")
        else:
            u_cong_name = st.selectbox("Congregação", cong_options, key="cad_user_cong")

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
            if st.button("Excluir usuários selecionadas", disabled=btn_disabled, key="cad_del_users_btn"):
                with SessionLocal() as _db:
                    _db.query(User).filter(User.id.in_(ids_u)).delete(synchronize_session=False)
                    _db.commit()
                st.success(f"{len(ids_u)} usuário(s) excluído(s)."); st.rerun()

# ===================== MAIN =====================
def main():
    try:
        ensure_seed()

        # -------- sessão / cookies --------
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

        # -------- auth --------
        user = current_user()
        if not user:
            login_ui()
            return

        # Força o menu a ser redesenhado a cada execução
        st.session_state["sidebar_rendered"] = False

        # -------- menu lateral (uma vez) --------
        page = sidebar_common(user)

        # -------- roteamento --------
        if page == "Lançamentos":
            page_lancamentos(user)
        elif page == "Relatório de Entrada":
            page_relatorio_entrada(user)
        elif page == "Relatório de Saída":
            page_relatorio_saida(user)
        elif page == "Relatório de Dizimistas":
            page_relatorio_dizimistas(user)
        elif page == "Relatório de Missões":
            if getattr(user, "role", "") == "TESOUREIRO":
                page_relatorio_missoes_congregacao(user)
            else:
                page_relatorio_missoes(user)
        elif page == "Visão Geral":
            page_visao_geral(user)
        elif page == "Cadastro":
            page_cadastro(user)
        else:
            st.warning("Seleção de página inválida.")
    except Exception as e:
        st.error("Ocorreu um erro ao renderizar a aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
