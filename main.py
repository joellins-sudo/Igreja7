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

import math

# ===== UI extra (menu bonito com fallback) =====
try:
    import streamlit_antd_components as sac  # pip install streamlit-antd-components
except Exception:
    sac = None  # fallback p/ radio padrão
import hashlib
from sqlalchemy import select
# ... outras importações ...
from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine, and_
from sqlalchemy.exc import IntegrityError  # <-- ADICIONE ESTA LINHA
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
# ...
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
from reportlab.lib.enums import TA_CENTER, TA_RIGHT

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
ADJ_HIER_ENTRY_DESC = "[Ajuste via Relatório Hierárquico (Entrada)]"
ADJ_HIER_OUT_DESC = "[Ajuste via Relatório Hierárquico (Saída)]"

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
  --base-font: 17px;         /* aumente para 18/19/20px se quiser */
  --table-font-size: 1.90rem;   /* fonte das células */
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

/* ===== NOVO: AVISO DE DIVERGÊNCIA VERMELHO ===== */
.alert-danger {
    padding: 0.75rem 1rem;
    margin-bottom: 1rem;
    border: 1px solid transparent;
    border-radius: .375rem;
    background-color: #fee2e2; /* Vermelho claro */
    border-color: #fca5a5;   /* Borda vermelha */
    color: #991b1b;         /* Texto vermelho escuro */
    font-size: 0.9rem;      /* Letras pequenas */
}
.alert-danger strong {
    font-weight: 700;
}

</style>
"""

# === Cores dos botões por formulário (compat com chamada antiga BUTTONS_CSS) ===
# SUBSTITUA SEU CSS DE BOTÕES ANTIGO POR ESTE
FORM_BUTTONS_CSS = """
<style>
/* --- ENTRADAS (VERDE) --- */
.adrf-entrada [data-testid="stFormSubmitButton"] button {
    background-color: #16a34a !important;
    border-color: #16a34a !important;
    color: white !important;
}
.adrf-entrada [data-testid="stFormSubmitButton"] button:hover {
    background-color: #15803d !important;
    border-color: #15803d !important;
}

/* --- DIZIMISTAS (AZUL) --- */
.adrf-dizimo [data-testid="stFormSubmitButton"] button {
    background-color: #1d4ed8 !important;
    border-color: #1d4ed8 !important;
    color: white !important;
}
.adrf-dizimo [data-testid="stFormSubmitButton"] button:hover {
    background-color: #1e40af !important;
    border-color: #1e40af !important;
}

/* --- SAÍDAS (VERMELHO) --- */
.adrf-saida [data-testid="stFormSubmitButton"] button {
    background-color: #dc2626 !important;
    border-color: #dc2626 !important;
    color: white !important;
}
.adrf-saida [data-testid="stFormSubmitButton"] button:hover {
    background-color: #b91c1c !important;
    border-color: #b91c1c !important;
}
</style>
"""

# Garanta que a linha abaixo esteja no seu código, após a definição acima
st.markdown(FORM_BUTTONS_CSS, unsafe_allow_html=True)

# Alias para manter compatibilidade com a linha 256
BUTTONS_CSS = FORM_BUTTONS_CSS


st.markdown(BUTTONS_CSS, unsafe_allow_html=True)

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

# NOVO HELPER: Função genérica para limpar campos
def _clear_launch_fields(keys_to_clear: List[str]):
    """Limpa campos específicos no session state para permitir novos lançamentos."""
    for key in keys_to_clear:
        # Define valor padrão para os campos que devem ser limpos
        if key.endswith("valor"):
            st.session_state[key] = 0.0
        elif key.endswith("desc") or key.endswith("nome"):
            st.session_state[key] = ""

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

def get_month_selector(label: str = "Mês de referência", key_prefix: str = "main") -> date:
    """Cria os seletores de mês e ano com uma chave única baseada no prefixo."""
    today = today_bahia()
    colm, coly = st.columns([2, 1])
    with colm:
        m = st.selectbox(
            f"{label} — Mês", 
            list(range(1, 13)), 
            index=today.month-1, 
            format_func=lambda i: MONTHS[i-1],
            key=f"{key_prefix}_month_selector"  # Chave única
        )
    with coly:
        y = st.number_input(
            "Ano", 
            value=today.year, 
            step=1, 
            format="%d",
            key=f"{key_prefix}_year_selector"   # Chave única
        )
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
# ===================== DB BASE & MODELS =====================
from sqlalchemy import UniqueConstraint

class Base(DeclarativeBase):
    pass

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
    sub_congregations: Mapped[List["SubCongregation"]] = relationship(back_populates="congregation", cascade="all, delete-orphan")

class SubCongregation(Base):
    __tablename__ = "sub_congregations"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, index=True)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    congregation: Mapped["Congregation"] = relationship(back_populates="sub_congregations")
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="sub_congregation")
    tithes: Mapped[List["Tithe"]] = relationship(back_populates="sub_congregation")
    __table_args__ = (
        UniqueConstraint('name', 'congregation_id', name='_name_congregation_uc'),
    )

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
    sub_congregation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sub_congregations.id"))
    sub_congregation: Mapped[Optional["SubCongregation"]] = relationship(back_populates="transactions")
    category: Mapped["Category"] = relationship(back_populates="transactions", lazy="joined")
    congregation: Mapped["Congregation"] = relationship(back_populates="transactions")

    # COLE ESTA NOVA CLASSE JUNTO COM SEUS OUTROS MODELOS (User, Transaction, etc.)

class ServiceLog(Base):
    __tablename__ = "service_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    service_type: Mapped[str] = mapped_column(String)
    dizimo: Mapped[float] = mapped_column(Float, default=0.0)
    oferta: Mapped[float] = mapped_column(Float, default=0.0)
    
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    sub_congregation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sub_congregations.id"))

    # Relações para facilitar o acesso (opcional, mas boa prática)
    congregation: Mapped["Congregation"] = relationship()
    sub_congregation: Mapped[Optional["SubCongregation"]] = relationship()

    # Regra para evitar lançamentos duplicados (mesma data, tipo e congregação)
    __table_args__ = (
        UniqueConstraint('date', 'service_type', 'congregation_id', 'sub_congregation_id', name='_service_uc'),
    )

class Tithe(Base):
    __tablename__ = "tithes"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    tither_name: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)
    
    # Relação com SubCongregation (versão correta e única)
    sub_congregation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sub_congregations.id"))
    sub_congregation: Mapped[Optional["SubCongregation"]] = relationship(back_populates="tithes")

    # Relação com Congregation (versão correta e única)
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
    # Novo: Limpar o estado de navegação para forçar a página inicial no próximo login
    if "main_menu_page" in st.session_state:
        del st.session_state["main_menu_page"] 
        
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
    # CSS base (o seu ADRF_LOGIN_CSS pode continuar)
    st.markdown(ADRF_LOGIN_CSS, unsafe_allow_html=True)

    # === Centraliza a viewport e transforma o st.form em um "card" ===
    LOGIN_CARD_CSS = """
    <style>
      /* centraliza tudo vertical/horizontal */
      [data-testid="stAppViewContainer"] > .main{
        display:flex; align-items:center; justify-content:center;
        min-height:100vh; padding:0 !important;
      }
      header[data-testid="stHeader"]{ display:none; }
      footer{ visibility:hidden; }

      /* deixa o formulário com cara de cartão e largura fixa */
      form[data-testid="stForm"]{
        width:520px; max-width:92vw; margin:0 auto;
        background:#fff; border:1px solid #E6E8F0; border-radius:14px;
        box-shadow:0 10px 30px rgba(16,24,40,.08);
        padding:28px 22px;
      }
      /* inputs e botão mais bonitos dentro do card */
      form[data-testid="stForm"] .stTextInput>div>div>input,
      form[data-testid="stForm"] .stPassword>div>div>input{
        height:44px; font-size:1rem;
      }
      form[data-testid="stForm"] .stButton>button{
        width:100%; height:44px; font-weight:700; border-radius:10px;
      }
    </style>
    """
    st.markdown(LOGIN_CARD_CSS, unsafe_allow_html=True)

    # === (REMOVA) Qualquer wrapper HTML aberto antes tipo:
    # st.markdown("<div class='adrf-wrap'><div class='adrf-card'><div class='body'>", unsafe_allow_html=True)
    # ... e o fechamento correspondente. Eles não "abraçam" widgets do Streamlit e viram um card vazio no topo.

    # LOGO (opcional)
    try:
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, caption=None, use_container_width=False, width=120)
        else:
            st.markdown(
                "<div style='text-align:center; font:800 48px/1 Inter,system-ui; color:#1f66eb'>ADRF<span style='color:#74b816'>!</span></div>",
                unsafe_allow_html=True
            )
    except Exception:
        pass

    # === FORMULÁRIO DE LOGIN (dentro do card) ===
    with st.form("adrf_login_form", clear_on_submit=False):
        u = st.text_input("Usuário", placeholder="Usuário", key="adrf_user")
        p = st.text_input("Senha", type="password", placeholder="Senha", key="adrf_pass")

        # Se você tiver combo de órgão/perfil, coloque aqui também:
        # org = st.selectbox("Órgão", ["PCPE", "PMPE", "SDS", ...])

        ok = st.form_submit_button("ACESSAR")

    if ok:
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

def sidebar_common(user: "User") -> str:
    """Desenha o menu lateral e retorna a página selecionada."""
    MENU_PAGES = {
        "Lançamentos": "📥", "Relatório de Entrada": "📊", "Relatório de Saída": "📉",
        "Relatório de Missões": "🌍", "Relatório de Dizimistas": "🧾", "Visão Geral": "🏁",
        "Cadastro": "🛠️",
    }
    
    role = getattr(user, "role", "")
    if role == "SEDE":
        menu_options_plain = ["Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral", "Cadastro"]
    elif role == "TESOUREIRO":
        menu_options_plain = ["Lançamentos", "Relatório de Entrada", "Relatório de Saída", "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral"]
    elif role == "TESOUREIRO MISSIONÁRIO":
        menu_options_plain = ["Relatório de Missões"]
    else:
        menu_options_plain = ["Visão Geral"]

    menu_labels_pretty = [f"{MENU_PAGES.get(opt, '•')} {opt}" for opt in menu_options_plain]
    label_to_page = {label: page for label, page in zip(menu_labels_pretty, menu_options_plain)}

    session_key = "main_menu_page"  
    current_page_name = st.session_state.get(session_key, menu_options_plain[0])
    
    try:
        default_index = menu_options_plain.index(current_page_name)
    except ValueError:
        default_index = 0

    with st.sidebar:
        try:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
        except Exception:
            pass
        st.write(f"👤 **{getattr(user, 'username', 'Usuário')}** — *{getattr(user, 'role', '')}*")

        sel_label = st.radio(
            "Menu", options=menu_labels_pretty, index=default_index, 
            key=session_key, label_visibility="collapsed"
        )
        page = label_to_page.get(sel_label, menu_options_plain[0])
        
        st.divider()
        if st.button("Sair"):
            logout()
            
    return page

# ======= NOVO: helper padrão para botões 'Salvar alterações' =======
# ====== CORES P/ BOTÕES ======
BTN_COLORS = {
    "entrada":  "#16a34a",  # verde
    "dizimista":"#2563eb",  # azul
    "saida":    "#dc2626",  # vermelha
    "neutral":  "#1f6feb",  # fallback (azul padrão)
}

def _save_btn(on_click, key_suffix: str, theme: str = "neutral", label: str = "Salvar alterações"):
    """
    Botão 'Salvar alterações' com cor personalizada por tema:
      - 'entrada'  -> verde
      - 'dizimista'-> azul
      - 'saida'    -> vermelho
      - 'neutral'  -> cor padrão
    """
    color = BTN_COLORS.get(theme, BTN_COLORS["neutral"])
    with st.container():
        # marcador p/ escopar o CSS desse botão apenas
        st.markdown(f'<div id="mark-{key_suffix}"></div>', unsafe_allow_html=True)
        st.button(label, key=f"btn_save_{key_suffix}", type="primary", on_click=on_click)
        st.markdown(
            f"""
            <style>
              /* pinta SOMENTE o botão dentro deste bloco */
              #mark-{key_suffix} ~ div[data-testid="stButton"] > button {{
                background: {color} !important;
                border-color: {color} !important;
              }}
              #mark-{key_suffix} ~ div[data-testid="stButton"] > button:hover {{
                filter: brightness(0.93);
              }}
            </style>
            """,
            unsafe_allow_html=True
        )

def _submit_btn(label: str, key_suffix: str, theme: str = "neutral") -> bool:
    """
    Versão colorida para st.form_submit_button (forms de ENTRADA, DIZIMISTA, SAÍDA).
    Retorna True quando o usuário clica.
    """
    color = BTN_COLORS.get(theme, BTN_COLORS["neutral"])
    with st.container():
        st.markdown(f'<div id="mark-{key_suffix}"></div>', unsafe_allow_html=True)
        clicked = st.form_submit_button(label, type="primary")
        st.markdown(
            f"""
            <style>
              /* cobre tanto submit de form quanto um fallback de stButton */
              #mark-{key_suffix} ~ div[data-testid="stFormSubmitButton"] > button,
              #mark-{key_suffix} ~ div[data-testid="stButton"] > button {{
                background: {color} !important;
                border-color: {color} !important
              }}
              #mark-{key_suffix} ~ div[data-testid="stFormSubmitButton"] > button:hover,
              #mark-{key_suffix} ~ div[data-testid="stButton"] > button:hover {{
                filter: brightness(0.93);
              }}
            </style>
            """,
            unsafe_allow_html=True
        )
    return clicked

def _apply_tx_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, tx_type: str, default_cong_id: Optional[int], default_sub_cong_id: Optional[int] = None):
    def norm_df(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        if "Valor" in d.columns: d["Valor"] = d["Valor"].map(_to_float_brl)
        if "Data" in d.columns: d["Data"] = d["Data"].map(_to_date)
        for c in ("Categoria", "Descrição", "Congregação"):
            if c in d.columns: d[c] = d[c].astype(str).fillna("")
        return d

    o = norm_df(orig_df)
    n_bruto = norm_df(edited_df)

    # --- LÓGICA DE EXCLUSÃO CORRIGIDA ---
    # Primeiro, identifica as linhas que são válidas para manter/atualizar.
    n = n_bruto[
        (n_bruto["Valor"].abs() > 0.01) & 
        (n_bruto["Categoria"] != "")
    ].copy()

    old_ids = set(int(x) for x in o["ID"].tolist() if pd.notna(x) and x > 0)
    new_ids = set(int(x) for x in n["ID"].tolist() if pd.notna(x) and x > 0)
    to_delete = list(old_ids - new_ids)
    
    old_map = {int(r["ID"]): r for _, r in o.iterrows() if pd.notna(r["ID"])}

    with SessionLocal() as db:
        cats = categories_for_type(db, tx_type)
        cat_by_name = {c.name: c for c in cats}
        if to_delete:
            db.query(Transaction).filter(Transaction.id.in_(to_delete)).delete(synchronize_session=False)

        for rid in sorted(new_ids & old_ids):
            new = n.loc[n["ID"] == rid].iloc[0]
            t = db.get(Transaction, rid)
            if not t: continue
            
            changed = False
            if t.date != new["Data"]: t.date = new["Data"]; changed = True
            cat = cat_by_name.get(new["Categoria"])
            if cat and t.category_id != cat.id: t.category_id = cat.id; changed = True
            if t.amount != new["Valor"]: t.amount = new["Valor"]; changed = True
            if (t.description or "") != (new.get("Descrição", "") or ""): t.description = new.get("Descrição"); changed = True
            if "_cong_id" in n.columns and int(new["_cong_id"]) != t.congregation_id:
                t.congregation_id = int(new["_cong_id"]); changed = True
            if changed: db.add(t)

        for _, row in n.iterrows():
            rid = row.get("ID", None)
            if pd.notna(rid) and int(rid) > 0: continue # Já foi tratado como atualização

            cat = cat_by_name.get(row["Categoria"])
            if not cat: continue

            cong_id = int(row.get("_cong_id", 0) or 0) or default_cong_id
            if not cong_id: continue
            
            db.add(Transaction(
                date=row["Data"], type=tx_type, category_id=cat.id, 
                amount=row["Valor"], description=(row.get("Descrição") or None),
                congregation_id=cong_id, sub_congregation_id=default_sub_cong_id
            ))
        db.commit()

# ===================== APPLY CHANGES — LANÇAMENTOS / DÍZIMOS =====================


def _apply_tithe_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, default_cong_id: Optional[int], default_sub_cong_id: Optional[int] = None):
    def norm_df(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        if "Valor" in d.columns: d["Valor"] = d["Valor"].map(_to_float_brl)
        if "Data" in d.columns: d["Data"] = d["Data"].map(_to_date)
        for c in ("Dizimista", "Forma de Pagamento"):
            if c in d.columns: d[c] = d[c].astype(str).fillna("")
        return d

    o = norm_df(orig_df)
    n_bruto = norm_df(edited_df)

    # --- LÓGICA DE EXCLUSÃO CORRIGIDA ---
    # Considera válidas apenas as linhas com valor e nome de dizimista preenchidos
    n = n_bruto[
        (n_bruto["Valor"].abs() > 0.01) & 
        (n_bruto["Dizimista"] != "")
    ].copy()

    old_ids = set(int(x) for x in o["ID"].tolist() if pd.notna(x) and x > 0)
    new_ids = set(int(x) for x in n["ID"].tolist() if pd.notna(x) and x > 0)
    to_delete = list(old_ids - new_ids)

    old_map = {int(r["ID"]): r for _, r in o.iterrows() if pd.notna(r["ID"])}

    with SessionLocal() as db:
        if to_delete:
            db.query(Tithe).filter(Tithe.id.in_(to_delete)).delete(synchronize_session=False)

        for rid in sorted(new_ids & old_ids):
            new = n.loc[n["ID"] == rid].iloc[0]
            t = db.get(Tithe, rid)
            if not t: continue
            
            changed = False
            if t.date != new["Data"]: t.date = new["Data"]; changed = True
            if t.tither_name != new["Dizimista"]: t.tither_name = new["Dizimista"]; changed = True
            if t.amount != new["Valor"]: t.amount = new["Valor"]; changed = True
            if (t.payment_method or "") != (new["Forma de Pagamento"] or ""): t.payment_method = new["Forma de Pagamento"] or None; changed = True
            if changed: db.add(t)

        for _, row in n.iterrows():
            rid = row.get("ID", None)
            if pd.notna(rid) and int(rid) > 0: continue

            if default_cong_id is None: continue
            db.add(Tithe(
                date=row["Data"], tither_name=row["Dizimista"], amount=row["Valor"],
                congregation_id=int(default_cong_id), sub_congregation_id=default_sub_cong_id,
                payment_method=(row.get("Forma de Pagamento") or None)
            ))
        db.commit()
        # ================================================================

# ===================== RELATÓRIO DE ENTRADA — TABELA ÚNICA (EDIT SUMÁRIO) =====================
# ===================== RELATÓRIO DE ENTRADA — TABELA ÚNICA (EDIT SUMÁRIO) =====================
def _entrada_summary_df(db: Session, cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None) -> pd.DataFrame:
    # Base queries
    tithes_q = select(Tithe.date, func.sum(Tithe.amount)).where(
        Tithe.congregation_id == cong_id, Tithe.date >= start, Tithe.date < end
    )
    diz_trans_q = select(Transaction.date, func.sum(Transaction.amount)).join(Category).where(
        Transaction.congregation_id == cong_id, Transaction.date >= start, Transaction.date < end,
        Transaction.type.in_((TYPE_IN, "RECEITA")), func.lower(Category.name).in_(("dízimo","dizimo"))
    )
    oferta_trans_q = select(Transaction.date, func.sum(Transaction.amount)).join(Category).where(
        Transaction.congregation_id == cong_id, Transaction.date >= start, Transaction.date < end,
        Transaction.type.in_((TYPE_IN, "RECEITA")), func.lower(Category.name) == "oferta"
    )

    # --- LÓGICA DE FILTRO CORRIGIDA ---
    if sub_cong_id is not None:
        # Filtra por uma sub-congregação específica
        tithes_q = tithes_q.where(Tithe.sub_congregation_id == sub_cong_id)
        diz_trans_q = diz_trans_q.where(Transaction.sub_congregation_id == sub_cong_id)
        oferta_trans_q = oferta_trans_q.where(Transaction.sub_congregation_id == sub_cong_id)
    else:
        # Filtra APENAS para a congregação principal (onde não há sub_congregation_id)
        tithes_q = tithes_q.where(Tithe.sub_congregation_id.is_(None))
        diz_trans_q = diz_trans_q.where(Transaction.sub_congregation_id.is_(None))
        oferta_trans_q = oferta_trans_q.where(Transaction.sub_congregation_id.is_(None))

    # Executa queries
    tithes = db.execute(tithes_q.group_by(Tithe.date)).all()
    diz_trans = db.execute(diz_trans_q.group_by(Transaction.date)).all()
    oferta_trans = db.execute(oferta_trans_q.group_by(Transaction.date)).all()

    by_date_diz_tit = defaultdict(float)
    for d, s in tithes: by_date_diz_tit[d] += float(s or 0.0)
    by_date_diz_tx = defaultdict(float)
    for d, s in diz_trans: by_date_diz_tx[d] += float(s or 0.0)
    by_date_ofe = defaultdict(float)
    for d, s in oferta_trans: by_date_ofe[d] += float(s or 0.0)

    all_dates = sorted(set(list(by_date_diz_tit.keys()) + list(by_date_diz_tx.keys()) + list(by_date_ofe.keys())))
    rows = []
    for d in all_dates:
        dz = max(float(by_date_diz_tit.get(d, 0.0)), float(by_date_diz_tx.get(d, 0.0)))
        ofe = float(by_date_ofe.get(d, 0.0))
        rows.append({"Data do Culto": d, "Dízimo": dz, "Oferta": ofe, "Total": dz + ofe})
    
    return pd.DataFrame(rows)

def _apply_entrada_summary_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None):
    with SessionLocal() as db:
        cats_in = categories_for_type(db, TYPE_IN)
        cat_diz = next((c for c in cats_in if _norm(c.name) in ("dizimo", "dízimo")), None)
        cat_ofe = next((c for c in cats_in if _norm(c.name) == "oferta"), None)
        if not (cat_diz and cat_ofe):
            st.error("Categorias 'Dízimo' e/ou 'Oferta' não encontradas."); return

        edited = edited_df.copy()
        
        for col in ["Dízimo", "Oferta"]:
            edited[col] = edited[col].map(_to_float_brl)
        edited["Data do Culto"] = edited["Data do Culto"].map(lambda x: _to_date(x) if pd.notna(x) else None)
        edited.dropna(subset=["Data do Culto"], inplace=True)
        
        wanted = {r["Data do Culto"]: (float(r["Dízimo"]), float(r["Oferta"])) for _, r in edited.iterrows()}
        
        orig_dates = set(pd.to_datetime(orig_df["Data do Culto"]).dt.date)
        edited_dates = set(wanted.keys())
        all_dates = sorted(list(orig_dates.union(edited_dates)))
        
        for d in all_dates:
            if d is None: continue
            
            want_dz, want_of = wanted.get(d, (0.0, 0.0))

            tithe_sub_filter = Tithe.sub_congregation_id.is_(None) if sub_cong_id is None else Tithe.sub_congregation_id == sub_cong_id
            tx_sub_filter = Transaction.sub_congregation_id.is_(None) if sub_cong_id is None else Transaction.sub_congregation_id == sub_cong_id

            # [NOVO] Lógica para apagar dízimos nominais se o total do dia for zerado no resumo
            if d in orig_dates and abs(want_dz) < 0.01:
                # 1. Deletar todos os Dízimos Nominais (Tithe) para este dia/unidade
                db.query(Tithe).filter(
                    Tithe.congregation_id == cong_id,
                    Tithe.date == d,
                    tithe_sub_filter
                ).delete(synchronize_session=False)

                # 2. Deletar todas as Transações de Dízimo (Transaction) para este dia/unidade
                db.query(Transaction).filter(
                    Transaction.congregation_id == cong_id,
                    Transaction.date == d,
                    Transaction.category_id == cat_diz.id,
                    tx_sub_filter
                ).delete(synchronize_session=False)
                
                # Zera o valor de oferta também, pois a linha inteira foi removida
                want_of = 0.0
            # [FIM DO NOVO BLOCO]

            sum_dz_tithes_q = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                Tithe.congregation_id == cong_id, Tithe.date == d, tithe_sub_filter
            )
            sum_dz_tithes = float(db.scalar(sum_dz_tithes_q) or 0.0)
            
            sum_dz_tx_no_adj_q = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
                Transaction.congregation_id == cong_id, Transaction.date == d, Transaction.category_id == cat_diz.id,
                tx_sub_filter, func.coalesce(Transaction.description, "") != ADJ_ENTRY_DESC
            )
            sum_dz_tx_no_adj = float(db.scalar(sum_dz_tx_no_adj_q) or 0.0)
            sum_dz_others = max(sum_dz_tithes, sum_dz_tx_no_adj)
            
            sum_of_others_q = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                Transaction.congregation_id == cong_id, Transaction.date == d, Transaction.category_id == cat_ofe.id,
                tx_sub_filter, func.coalesce(Transaction.description, "") != ADJ_ENTRY_DESC
            )
            sum_of_others = float(db.scalar(sum_of_others_q) or 0.0)

            adj_dz_new = want_dz - sum_dz_others
            adj_of_new = want_of - sum_of_others

            adj_dz = db.scalar(select(Transaction).where(
                Transaction.congregation_id == cong_id, Transaction.date == d, Transaction.category_id == cat_diz.id,
                tx_sub_filter, func.coalesce(Transaction.description, "") == ADJ_ENTRY_DESC
            ))
            adj_of = db.scalar(select(Transaction).where(
                Transaction.congregation_id == cong_id, Transaction.date == d, Transaction.category_id == cat_ofe.id,
                tx_sub_filter, func.coalesce(Transaction.description, "") == ADJ_ENTRY_DESC
            ))

            if abs(adj_dz_new) < 0.001:
                if adj_dz: db.delete(adj_dz)
            else:
                if adj_dz: 
                    adj_dz.amount = float(adj_dz_new)
                else: 
                    db.add(Transaction(date=d, type=TYPE_IN, category_id=cat_diz.id, amount=float(adj_dz_new), description=ADJ_ENTRY_DESC, congregation_id=cong_id, sub_congregation_id=sub_cong_id))

            if abs(adj_of_new) < 0.001:
                if adj_of: db.delete(adj_of)
            else:
                if adj_of: 
                    adj_of.amount = float(adj_of_new)
                else: 
                    db.add(Transaction(date=d, type=TYPE_IN, category_id=cat_ofe.id, amount=float(adj_of_new), description=ADJ_ENTRY_DESC, congregation_id=cong_id, sub_congregation_id=sub_cong_id))
        
        db.commit()

# ===================== EDITORES INLINE REUTILIZÁVEIS (com botão Salvar) =====================
# ===== EDITOR DE LANÇAMENTOS (com force_cong_id e linha vazia) =====
# ===== EDITOR DE LANÇAMENTOS (com total abaixo da tabela) =====
def _editor_lancamentos(
    transactions: List["Transaction"],
    titulo: str,
    tx_type_hint: Optional[str] = None,
    force_cong_id: Optional[int] = None,
    force_sub_cong_id: Optional[int] = None
):
    tx_type = tx_type_hint or (transactions[0].type if transactions else TYPE_IN)

    with SessionLocal() as db:
        cats = categories_for_type(db, tx_type)
        if tx_type == TYPE_IN:
            cats = [c for c in cats if "ajuste" not in _norm(c.name)]
        cat_names = [c.name for c in cats] or ["—"]

    rows = []
    if transactions:
        for t in transactions:
            rows.append({
                "ID": t.id, "Data": t.date,
                "Categoria": (t.category.name if t.category else ""),
                "Valor": float(t.amount), "Descrição": t.description or "",
                "_cong_id": int(t.congregation_id or 0),
            })
    else:
        rows = [{"ID": None, "Data": today_bahia(), "Categoria": (cat_names[0] if cat_names else ""), "Valor": 0.0, "Descrição": "", "_cong_id": int(force_cong_id or 0)}]

    df_full = pd.DataFrame(rows)
    df_view = df_full.drop(columns=["_cong_id"])

    st.markdown(f"**{titulo}**")
    edited_view = st.data_editor(
        df_view, use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={
            "ID": st.column_config.Column("ID", disabled=True),
            "Data": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
            "Categoria": st.column_config.SelectboxColumn("Categoria", options=cat_names, required=True),
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
            "Descrição": st.column_config.TextColumn("Descrição", max_chars=200),
        },
        key=f"tx_editor_{titulo.replace(' ', '_')}_{force_cong_id}_{force_sub_cong_id}",
    )

    try:
        _total_val = 0.0
        if isinstance(edited_view, pd.DataFrame) and not edited_view.empty and ("Valor" in edited_view.columns):
            _ev = edited_view.copy()
            _ev["Valor"] = _ev["Valor"].map(_to_float_brl)
            _total_val = float(_ev["Valor"].sum())
    except Exception:
        _total_val = 0.0
    _label_total = "Total de Saídas (tabela)" if tx_type == TYPE_OUT else "Total de Entradas (tabela)"
    st.metric(_label_total, format_currency(_total_val))

    def _save():
        _apply_tx_changes(df_full, edited_view, tx_type, force_cong_id, force_sub_cong_id)
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, f"tx_{titulo.replace(' ', '_')}_{force_cong_id}_{force_sub_cong_id}", theme=("saida" if tx_type == TYPE_OUT else "entrada"))

# ===== EDITOR DE DÍZIMOS (com force_cong_id e linha vazia) =====
# ===== EDITOR DE DÍZIMOS (com total abaixo da tabela) =====
def _editor_dizimos(tithes: List["Tithe"], titulo: str, force_cong_id: Optional[int] = None, force_sub_cong_id: Optional[int] = None):
    rows = []
    if tithes:
        rows = [{"ID": t.id, "Data": t.date, "Dizimista": t.tither_name, "Valor": float(t.amount), "Forma de Pagamento": t.payment_method or "", "_cong_id": int(t.congregation_id or 0)} for t in tithes]
    else:
        rows = [{"ID": None, "Data": today_bahia(), "Dizimista": "", "Valor": 0.0, "Forma de Pagamento": "", "_cong_id": int(force_cong_id or 0)}]

    df_full = pd.DataFrame(rows)
    df_view = df_full.drop(columns=["_cong_id"])

    st.markdown(f"**{titulo}**")
    edited_view = st.data_editor(
        df_view, use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={
            "ID": st.column_config.Column("ID", disabled=True),
            "Data": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
            "Dizimista": st.column_config.TextColumn("Dizimista", max_chars=120, required=True),
            "Valor": st.column_config.NumberColumn("Valor (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
            "Forma de Pagamento": st.column_config.SelectboxColumn("Forma de Pagamento", options=["Dinheiro", "PIX", "Cartão", "Transferência", ""], required=False),
        },
        key=f"tithe_editor_{titulo.replace(' ', '_')}_{force_cong_id}_{force_sub_cong_id}",
    )

    try:
        _total_val = 0.0
        if isinstance(edited_view, pd.DataFrame) and not edited_view.empty and ("Valor" in edited_view.columns):
            _ev = edited_view.copy()
            _ev["Valor"] = _ev["Valor"].map(_to_float_brl)
            _total_val = float(_ev["Valor"].sum())
    except Exception:
        _total_val = 0.0
    st.metric("Total de DÍZIMOS (tabela)", format_currency(_total_val))

    def _save():
        _apply_tithe_changes(df_full, edited_view, force_cong_id, force_sub_cong_id)
        st.toast("💾 Alterações salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, f"tithe_{titulo.replace(' ', '_')}_{force_cong_id}_{force_sub_cong_id}", theme="dizimista")

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

def _editor_missions_entries_agg(congs_all: List[Congregation], start: date, end: date, titulo: str):
    with SessionLocal() as db:
        # Totais de Missões (Entrada) por congregação no período
        q = select(
            Congregation.name,
            func.sum(Transaction.amount)
        ).join(Transaction).join(Category).where(
            Transaction.date >= start, Transaction.date < end,
            Transaction.type == TYPE_IN,
            func.lower(Category.name).in_(("missões", "missoes"))
        ).group_by(Congregation.name)

        sums = db.execute(q).all()
        rows = [{"Congregação": name, "Valor": float(val or 0.0)} for name, val in sums]
        rows.sort(key=lambda x: x["Valor"], reverse=True)

    df_view = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["Congregação", "Valor"])
    df_orig = df_view.copy()  # guarda o estado original para comparação

    edited_view = st.data_editor(
        df_view,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "Congregação": st.column_config.SelectboxColumn(
                "Congregação",
                options=[c.name for c in order_congs_sede_first(congs_all)],
                required=True
            ),
            "Valor": st.column_config.NumberColumn(
                "Valor (R$)",
                min_value=0.0,
                step=1.0,
                format="R$ %.2f"
            ),
        },
        key=f"missoes_in_agg_{titulo}",
    )

    # Totalizador do mês
    try:
        _total_in_missions = 0.0
        if isinstance(edited_view, pd.DataFrame) and not edited_view.empty:
            _total_in_missions = edited_view["Valor"].map(_to_float_brl).sum()
    except Exception:
        _total_in_missions = 0.0
    st.metric("Total de ENTRADAS de Missões (mês corrente)", format_currency(_total_in_missions))

    def _save():
        with SessionLocal() as db:
            by_name = {c.name: c.id for c in congs_all}
            cat_miss = db.scalar(select(Category).where(func.lower(Category.name).in_(("missões", "missoes"))))
            if not cat_miss:
                st.error("Categoria 'Missões' não encontrada.")
                return

            # Mapeia valores antes/depois para gerar/atualizar o AJUSTE agregado
            orig_map = {row["Congregação"]: row["Valor"] for _, row in df_orig.iterrows()}
            edited_map = {row["Congregação"]: row["Valor"] for _, row in edited_view.iterrows()}
            all_congs = set(orig_map.keys()) | set(edited_map.keys())

            for cong_name in all_congs:
                cong_id = by_name.get(cong_name)
                if not cong_id:
                    continue

                valor_antigo = float(orig_map.get(cong_name, 0.0) or 0.0)
                valor_novo   = float(edited_map.get(cong_name, 0.0) or 0.0)
                if abs(valor_antigo - valor_novo) <= 0.01:
                    continue

                ajuste = valor_novo - valor_antigo

                # Procura um ajuste existente para este mês/unidade
                q_adj = select(Transaction).where(
                    Transaction.congregation_id == cong_id,
                    Transaction.category_id == cat_miss.id,
                    Transaction.description == ADJ_MISS_IN_DESC,
                    Transaction.date >= start, Transaction.date < end
                )
                adj_existente = db.scalar(q_adj)

                if adj_existente:
                    novo_valor_ajuste = float(adj_existente.amount or 0.0) + ajuste
                    if abs(novo_valor_ajuste) < 0.01:
                        db.delete(adj_existente)
                    else:
                        adj_existente.amount = novo_valor_ajuste
                        db.add(adj_existente)
                elif abs(ajuste) >= 0.01:
                    db.add(Transaction(
                        date=start,
                        type=TYPE_IN,
                        category_id=cat_miss.id,
                        amount=ajuste,
                        description=ADJ_MISS_IN_DESC,
                        congregation_id=cong_id
                    ))

            db.commit()
        st.toast("💾 Alterações salvas com sucesso!", icon="✅")
        st.rerun()

    _save_btn(_save, f"missoes_in_{titulo}")


def _editor_missions_entries_unit(cong_id: int, sub_cong_id: Optional[int], start: date, end: date, titulo: str = "Entradas de Missões (por culto)"):
    """
    Editor simples de ENTRADAS de Missões para uma unidade (principal ou sub).
    Colunas: Data do Culto, Oferta de Missões.
    Persistência: Transaction(type=TYPE_IN, category='Missões'), congregation_id/sub_congregation_id.
    """
    with SessionLocal() as db:
        # Garante a categoria "Missões" de ENTRADA
        cat_miss = db.scalar(select(Category).where(func.lower(Category.name).in_(("missões","missoes")), Category.type == TYPE_IN))
        if not cat_miss:
            st.error("Categoria 'Missões' (Entrada) não encontrada."); 
            return

        # Busca lançamentos existentes no mês/unidade
        base_q = select(Transaction).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type == TYPE_IN,
            Transaction.category_id == cat_miss.id,
        )
        if sub_cong_id is None:
            base_q = base_q.where(Transaction.sub_congregation_id.is_(None))
        else:
            base_q = base_q.where(Transaction.sub_congregation_id == sub_cong_id)

        txs = db.scalars(base_q.order_by(Transaction.date)).all()

    # Monta dataframe (apenas Data/Valor; ID e _cong_id ficam escondidos para salvar)
    if txs:
        rows = [{"ID": t.id, "Data do Culto": t.date, "Oferta de Missões": float(t.amount), "_cong_id": int(t.congregation_id or 0)} for t in txs]
    else:
        rows = [{"ID": None, "Data do Culto": today_bahia(), "Oferta de Missões": 0.0, "_cong_id": int(cong_id)}]

    df_full = pd.DataFrame(rows)
    df_view = df_full.drop(columns=["_cong_id"])

    st.markdown(f"**{titulo}**")
    edited_view = st.data_editor(
        df_view,
        use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={
            "ID": st.column_config.Column("ID", disabled=True),
            "Data do Culto": st.column_config.DateColumn("Data do Culto", required=True, format="DD/MM/YYYY"),
            "Oferta de Missões": st.column_config.NumberColumn("Oferta de Missões (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
        },
        key=f"missoes_in_unit_{cong_id}_{sub_cong_id}",
    )

    # Totalizador
    try:
        total_in = 0.0
        if isinstance(edited_view, pd.DataFrame) and not edited_view.empty and ("Oferta de Missões" in edited_view.columns):
            _ev = edited_view.copy()
            _ev["Oferta de Missões"] = _ev["Oferta de Missões"].map(_to_float_brl)
            total_in = float(_ev["Oferta de Missões"].sum())
    except Exception:
        total_in = 0.0
    st.metric("Total de ENTRADAS de Missões (tabela)", format_currency(total_in))

    # Salvar
    def _save():
        # Prepara dataframes no formato esperado por _apply_tx_changes
        _df_full = df_full.rename(columns={
            "Data do Culto": "Data",
            "Oferta de Missões": "Valor"
        }).assign(**{"Categoria": "Missões"})
        _edited = edited_view.rename(columns={
            "Data do Culto": "Data",
            "Oferta de Missões": "Valor"
        }).assign(**{"Categoria": "Missões"})

        # Cong id default; sub_cong específico passado no arg
        _apply_tx_changes(_df_full, _edited, TYPE_IN, default_cong_id=cong_id, default_sub_cong_id=sub_cong_id)
        st.toast("💾 Entradas de Missões salvas.", icon="✅")
        st.rerun()

    _save_btn(_save, f"missoes_in_unit_{cong_id}_{sub_cong_id}", theme="entrada")


# ====== EDITORES AGREGADOS (TODAS AS CONGREGAÇÕES) — ENTRADAS / SAÍDAS ======
# ====== EDITORES AGREGADOS (TODAS AS CONGREGAÇÕES) — ENTRADAS / SAÍDAS ======
# ====== EDITORES AGREGADOS (TODAS AS CONGREGAÇÕES) — ENTRADAS / SAÍDAS ======
def _editor_entradas_agg_all(congs_all: List[Congregation], start: date, end: date):
    with SessionLocal() as db:
        rows_data = []
        # Primeiro, colete os dados de todas as unidades (principais e subs)
        for c in congs_all:
            # Dados da congregação principal
            principal_totals = _collect_month_data(db, c.id, start, end, sub_cong_id=None)["totals"]
            rows_data.append({
                "unidade_display": f"{c.name} (Principal)",
                "valor": float(principal_totals["entradas_total_sem_missoes"]),
                "cong_id": c.id,
                "cong_name": c.name, # Adicionado para ordenação primária
                "sub_id": None,
                "is_sub": False
            })
            
            # Dados das sub-congregações
            sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == c.id)).all()
            for sub in sub_congs:
                sub_totals = _collect_month_data(db, c.id, start, end, sub_cong_id=sub.id)["totals"]
                rows_data.append({
                    "unidade_display": f"↳ {sub.name}",
                    "valor": float(sub_totals["entradas_total_sem_missoes"]),
                    "cong_id": c.id,
                    "cong_name": c.name, # Adicionado para ordenação primária
                    "sub_id": sub.id,
                    "is_sub": True
                })

        rows_data.sort(key=lambda x: (x["cong_name"], x["is_sub"], -x["valor"]))
        
        df_full = pd.DataFrame(rows_data)
        df_view = df_full[["unidade_display", "valor"]].rename(columns={"unidade_display": "Unidade", "valor": "Total (R$)"})

        # ALTERADO: st.data_editor virou st.dataframe
        st.dataframe(
            df_view.style.format({"Total (R$)": format_currency}), 
            use_container_width=True, 
            hide_index=True
        )

        total_geral = 0.0
        if not df_view.empty:
            total_geral = df_view["Total (R$)"].map(_to_float_brl).sum()
        st.metric("Total Geral de Entradas (todas as unidades)", format_currency(total_geral))
        # REMOVIDO: Botão de salvar e sua lógica

def _editor_saidas_agg_all(congs_all: List[Congregation], start: date, end: date):
    with SessionLocal() as db:
        rows = []
        for c in congs_all:
            totals = _collect_month_data(db, c.id, start, end)["totals"]
            rows.append({"Congregação": c.name, "Total Saídas (R$)": float(totals["saidas_total"])})
        df_view = pd.DataFrame(rows).sort_values("Total Saídas (R$)", ascending=False).reset_index(drop=True)

    # ALTERADO: st.data_editor virou st.dataframe
    st.dataframe(
        df_view.style.format({"Total Saídas (R$)": format_currency}),
        use_container_width=True, 
        hide_index=True
    )
    # REMOVIDO: Botão de salvar e toda sua lógica

# ===================== CORE COLETA =====================
# ===================== CORE COLETA =====================
def _collect_month_data(db, cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None):
    # Base queries
    tx_in_query = select(Transaction).options(joinedload(Transaction.category)).where(
        Transaction.date >= start, Transaction.date < end,
        Transaction.type.in_(("DOAÇÃO", "RECEITA")),
        Transaction.congregation_id == cong_id
    )
    tithes_query = select(Tithe).where(
        Tithe.date >= start, Tithe.date < end,
        Tithe.congregation_id == cong_id
    )
    tx_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
        Transaction.date >= start, Transaction.date < end,
        Transaction.type.in_(("SAÍDA", "DESPESA")),
        Transaction.congregation_id == cong_id
    )

    # --- LÓGICA DE FILTRO CORRIGIDA ---
    # Aplica o filtro da sub-congregação, tratando o caso "Principal" (None) explicitamente
    if sub_cong_id is not None:
        # Filtra por uma sub-congregação específica
        tx_in_query = tx_in_query.where(Transaction.sub_congregation_id == sub_cong_id)
        tithes_query = tithes_query.where(Tithe.sub_congregation_id == sub_cong_id)
        tx_out_query = tx_out_query.where(Transaction.sub_congregation_id == sub_cong_id)
    else:
        # Filtra APENAS para a congregação principal (onde não há sub_congregation_id)
        tx_in_query = tx_in_query.where(Transaction.sub_congregation_id.is_(None))
        tithes_query = tithes_query.where(Tithe.sub_congregation_id.is_(None))
        tx_out_query = tx_out_query.where(Transaction.sub_congregation_id.is_(None))
    
    # Executa as queries
    tx_in = db.scalars(tx_in_query.order_by(Transaction.date)).all()
    tithes = db.scalars(tithes_query.order_by(Tithe.date)).all()
    tx_out = db.scalars(tx_out_query.order_by(Transaction.date)).all()

    # O resto da lógica de cálculo de totais permanece a mesma
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
        "tx_in": tx_in, "tithes": tithes, "tx_out": tx_out,
        "totals": {
            "dizimos": total_dizimos_final, "ofertas": total_ofertas, "missoes": total_missoes,
            "entradas_outros": total_entradas_outros, "entradas_total_sem_missoes": total_geral_entradas_sem_missoes,
            "saidas_total": total_saidas, "saldo": saldo
        }
    }

# COLE ESTAS DUAS FUNÇÕES NO SEU CÓDIGO, ANTES DA "page_lancamentos"

# APAGUE AS FUNÇÕES _load_multi_service_data e _apply_multi_service_changes E SUBSTITUA POR ESTAS

# SUBSTITUA SUA FUNÇÃO _load_service_logs INTEIRA POR ESTA VERSÃO CORRIGIDA

def _load_service_logs(db: Session, cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None) -> pd.DataFrame:
    """Carrega os resumos de culto para a tabela de edição, com ordenação customizada."""
    
    log_filter = and_(
        ServiceLog.congregation_id == cong_id,
        ServiceLog.date >= start,
        ServiceLog.date < end,
        ServiceLog.sub_congregation_id == sub_cong_id
    )
    
    # ===== NOVA LÓGICA DE ORDENAÇÃO =====
    # Cria uma regra de "ranking" para o tipo de culto
    from sqlalchemy import case
    custom_sort_order = case(
        (ServiceLog.service_type == "Trabalhos pela Manhã (EBD, CO, FESTIVIDADES)", 1),
        (ServiceLog.service_type == "Culto da Noite (Padrão)", 2),
        (ServiceLog.service_type == "Evento Especial", 3),
        else_=4  # Garante que "Outro" e tipos futuros fiquem por último
    )

    # Aplica a ordenação primária por data e a secundária pela regra customizada
    query = select(ServiceLog).where(log_filter).order_by(ServiceLog.date, custom_sort_order)
    # ===== FIM DA NOVA LÓGICA =====
    
    logs = db.scalars(query).all()

    if not logs:
        return pd.DataFrame()

    data = []
    for log in logs:
        total = log.dizimo + log.oferta
        data.append({
            "ID": log.id,
            "Data do Culto": log.date,
            "Tipo de Culto": log.service_type,
            "Dízimo": log.dizimo,
            "Oferta": log.oferta,
            "Total": total
        })
    
    return pd.DataFrame(data)

def _apply_service_log_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, cong_id: int, sub_cong_id: Optional[int] = None):
    """Aplica as mudanças (cria, atualiza, deleta) na tabela service_logs."""
    
    orig_ids = set(orig_df['ID'].dropna())
    edited_ids = set(edited_df['ID'].dropna())

    to_delete = orig_ids - edited_ids
    to_update = orig_ids.intersection(edited_ids)
    
    with SessionLocal() as db:
        # Deletar logs que foram removidos
        if to_delete:
            db.query(ServiceLog).filter(ServiceLog.id.in_(to_delete)).delete(synchronize_session=False)

        # Atualizar logs existentes
        for log_id in to_update:
            log = db.get(ServiceLog, int(log_id))
            if log:
                row = edited_df[edited_df['ID'] == log_id].iloc[0]
                log.date = _to_date(row["Data do Culto"])
                log.service_type = str(row["Tipo de Culto"])
                log.dizimo = _to_float_brl(row["Dízimo"])
                log.oferta = _to_float_brl(row["Oferta"])

        # Inserir novos logs
        new_rows = edited_df[edited_df['ID'].isna()]
        for _, row in new_rows.iterrows():
            # Evita adicionar linhas vazias
            if _to_float_brl(row["Dízimo"]) > 0 or _to_float_brl(row["Oferta"]) > 0:
                new_log = ServiceLog(
                    date=_to_date(row["Data do Culto"]),
                    service_type=str(row["Tipo de Culto"]),
                    dizimo=_to_float_brl(row["Dízimo"]),
                    oferta=_to_float_brl(row["Oferta"]),
                    congregation_id=cong_id,
                    sub_congregation_id=sub_cong_id
                )
                db.add(new_log)
        
        try:
            db.commit()
            st.toast("Alterações salvas com sucesso!", icon="✅")
        except IntegrityError:
            db.rollback()
            st.error("Erro: Tentativa de criar um lançamento duplicado (mesma data, tipo e congregação). Por favor, verifique os dados.")
        except Exception as e:
            db.rollback()
            st.error(f"Ocorreu um erro ao salvar: {e}")

# ===================== PAGE: LANÇAMENTOS (com modo Tabela fora do form) =====================
# APAGUE SUA FUNÇÃO page_lancamentos ANTIGA E SUBSTITUA POR ESTA VERSÃO FINAL

# SUBSTITUA SUA page_lancamentos PELA VERSÃO FINAL ABAIXO

# SUBSTITUA SUA page_lancamentos PELA VERSÃO FINAL ABAIXO

# APAGUE SUA page_lancamentos ANTIGA E SUBSTITUA POR ESTA VERSÃO FINAL

# SUBSTITUA SUA page_lancamentos INTEIRA POR ESTA VERSÃO CORRIGIDA

# SUBSTITUA SUA page_lancamentos INTEIRA POR ESTA VERSÃO FINAL

# SUBSTITUA SUA page_lancamentos INTEIRA POR ESTA VERSÃO FINAL

# SUBSTITUA SUA page_lancamentos INTEIRA POR ESTA VERSÃO FINAL

def page_lancamentos(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        st.markdown(f"<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)

        parent_cong_obj = None
        if user.role == "SEDE":
            congs_all = order_congs_sede_first(cong_options_for(user, db))
            cong_sel_name = st.selectbox("Selecione a Congregação Principal:", [c.name for c in congs_all], key="lan_cong_sel_sede")
            parent_cong_obj = next((c for c in congs_all if c.name == cong_sel_name), None)
        else:  # TESOUREIRO
            parent_cong_obj = db.get(Congregation, user.congregation_id)

        if not parent_cong_obj:
            st.error("Nenhuma congregação selecionada ou encontrada."); return

        st.markdown(f"### CONGREGAÇÃO: {parent_cong_obj.name.upper()}")

        modo = st.radio(
            "Modo de lançamento:",
            ["Formulário único", "Editar direto na tabela"],
            horizontal=True,
            key="lan_modo_sel"
        )
        st.divider()

        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id)).all()

        # Tipos de culto (inalterado)
        tipos_de_culto = [
            "Culto da Noite (Padrão)",
            "Trabalhos pela Manhã (EBD, CO, FESTIVIDADES)",
            "Culto de missões(Registre a oferta no Relatório de Missões)",
            "Evento Especial",
            "Outro"
        ]

        if modo == "Formulário único":
            target_cong_obj = parent_cong_obj
            contexto_selecionado = f"{parent_cong_obj.name} (Principal)"
            target_sub_cong_id = None

            if sub_congs:
                opcoes = {f"{parent_cong_obj.name} (Principal)": None}
                for sub in sub_congs:
                    opcoes[sub.name] = sub.id
                contexto_selecionado = st.selectbox("Lançar em:", list(opcoes.keys()), key="lan_sub_sel_context_form")
                target_sub_cong_id = opcoes[contexto_selecionado]

            st.markdown(f"#### Unidade selecionada: *{contexto_selecionado}*")
            st.divider()

            with st.expander("➕ Lançar ENTRADA (Resumo do Culto)", expanded=True):
                st.markdown('<div class="adrf-entrada">', unsafe_allow_html=True)
                with st.form("form_entrada_resumo"):
                    ent_data = st.date_input("Data do Culto", value=today_bahia(), key="ent_data_form")
                    ent_tipo = st.selectbox("Tipo de Culto", options=tipos_de_culto, key="ent_tipo_form")
                    c1, c2 = st.columns(2)
                    ent_dizimo = c1.number_input("Valor do Dízimo", min_value=0.0, value=0.0, format="%.2f", key="ent_dizimo_form")
                    ent_oferta = c2.number_input("Valor da Oferta", min_value=0.0, value=0.0, format="%.2f", key="ent_oferta_form")

                    if st.form_submit_button("Salvar Entrada do Culto"):
                        if ent_dizimo > 0 or ent_oferta > 0:
                            log_existente = db.scalar(
                                select(ServiceLog).where(
                                    ServiceLog.date == ent_data,
                                    ServiceLog.service_type == ent_tipo,
                                    ServiceLog.congregation_id == target_cong_obj.id,
                                    ServiceLog.sub_congregation_id == target_sub_cong_id
                                )
                            )
                            if log_existente:
                                log_existente.dizimo += ent_dizimo
                                log_existente.oferta += ent_oferta
                                st.success("Valores adicionados ao registro do culto existente!")
                            else:
                                novo_log = ServiceLog(
                                    date=ent_data,
                                    service_type=ent_tipo,
                                    dizimo=ent_dizimo,
                                    oferta=ent_oferta,
                                    congregation_id=target_cong_obj.id,
                                    sub_congregation_id=target_sub_cong_id
                                )
                                db.add(novo_log)
                                st.success("Novo registro de culto salvo com sucesso!")
                            db.commit()
                            st.rerun()
                        else:
                            st.warning("Nenhum valor foi inserido.")
                st.markdown('</div>', unsafe_allow_html=True)

            with st.expander("👤 Lançar DÍZIMO (Nominal)"):
                st.markdown('<div class="adrf-dizimo">', unsafe_allow_html=True)
                with st.form("form_dizimo"):
                    dz_data = st.date_input("Data do Dízimo", value=today_bahia(), key="dz_data")
                    dz_nome = st.text_input("Nome do dizimista", key="dz_nome")
                    dz_valor = st.number_input("Valor (R$)", min_value=0.0, value=0.0, format="%.2f", key="dz_valor")
                    dz_payment = st.selectbox("Forma de Pagamento", ["Dinheiro", "PIX", "Cartão", "Transferência"], key="dz_pay")

                    if st.form_submit_button("Salvar DIZIMISTA"):
                        if dz_valor > 0 and dz_nome.strip():
                            db.add(
                                Tithe(
                                    date=dz_data,
                                    tither_name=dz_nome.strip(),
                                    amount=dz_valor,
                                    congregation_id=target_cong_obj.id,
                                    sub_congregation_id=target_sub_cong_id,
                                    payment_method=dz_payment
                                )
                            )
                            db.commit()
                            st.success("Dízimo registrado!")
                            st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            with st.expander("➖ Lançar SAÍDA"):
                st.markdown('<div class="adrf-saida">', unsafe_allow_html=True)
                with st.form("form_saida"):
                    cats_out = categories_for_type(db, "SAÍDA")
                    c1, c2 = st.columns(2)
                    with c1:
                        sai_data = st.date_input("Data da Saída", value=today_bahia(), key="sai_data")
                    with c2:
                        sai_cat_name = st.selectbox("Categoria", [c.name for c in cats_out] or ["—"], key="sai_cat")
                    sai_desc = st.text_input("Descrição (opcional)", key="sai_desc")
                    sai_valor = st.number_input("Valor (R$)", min_value=0.0, value=0.0, format="%.2f", key="sai_valor")

                    if st.form_submit_button("Salvar SAÍDA"):
                        cat_obj = next((c for c in cats_out if c.name == sai_cat_name), None)
                        if sai_valor > 0 and cat_obj:
                            db.add(
                                Transaction(
                                    date=sai_data,
                                    type="SAÍDA",
                                    category_id=cat_obj.id,
                                    amount=sai_valor,
                                    description=(sai_desc or None),
                                    congregation_id=target_cong_obj.id,
                                    sub_congregation_id=target_sub_cong_id
                                )
                            )
                            db.commit()
                            st.success("Saída registrada!")
                            st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

        elif modo == "Editar direto na tabela":
            contexto_tabela = f"{parent_cong_obj.name} (Principal)"
            target_sub_cong_id = None
            if sub_congs:
                opcoes_tabela = {f"{parent_cong_obj.name} (Principal)": None}
                for sub in sub_congs:
                    opcoes_tabela[sub.name] = sub.id
                contexto_tabela = st.selectbox("Selecione a unidade para editar:", list(opcoes_tabela.keys()), key="lan_tabela_contexto")
                target_sub_cong_id = opcoes_tabela[contexto_tabela]

            st.info(f"Editando lançamentos de: **{contexto_tabela}**")
            ref_tab = get_month_selector("Mês de referência da tabela")
            start_tab, end_tab = month_bounds(ref_tab)

            st.markdown("##### Resumo de Entradas por Culto")

            df_logs = _load_service_logs(db, parent_cong_obj.id, start_tab, end_tab, sub_cong_id=target_sub_cong_id)

            # ===================== AVISO ÚNICO (mês/unidade) =====================
            # Requisito: soma(Dízimo nos resumos) == soma(Tithe nominal)
            declarado_total = 0.0
            if isinstance(df_logs, pd.DataFrame) and not df_logs.empty and ("Dízimo" in df_logs.columns):
                try:
                    declarado_total = float(df_logs["Dízimo"].sum() or 0.0)
                except Exception:
                    declarado_total = 0.0

            with SessionLocal() as _db_chk:
                tithe_sub_filter = (Tithe.sub_congregation_id.is_(None) if target_sub_cong_id is None
                                    else (Tithe.sub_congregation_id == target_sub_cong_id))
                real_total = float(_db_chk.scalar(
                    select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                        Tithe.congregation_id == parent_cong_obj.id,
                        Tithe.date >= start_tab, Tithe.date < end_tab,
                        tithe_sub_filter
                    )
                ) or 0.0)

            diff_total = round(declarado_total - real_total, 2)
            if abs(diff_total) >= 0.01:
                # Uma linha (máx. duas) e direta ao ponto
                st.markdown(f"""
<div class="alert-danger">
  <strong>Divergência de Dízimos no período</strong> — Declarado no resumo: <strong>{format_currency(declarado_total)}</strong> • Nominal (dizimistas): <strong>{format_currency(real_total)}</strong> • Diferença: <strong>{format_currency(diff_total)}</strong>
</div>
""", unsafe_allow_html=True)
            # =================== FIM AVISO ÚNICO ===================

            if df_logs.empty:
                df_logs = pd.DataFrame(
                    [{
                        "Data do Culto": today_bahia(),
                        "Tipo de Culto": tipos_de_culto[0],
                        "Dízimo": 0.0,
                        "Oferta": 0.0,
                        "Total": 0.0,
                        "ID": None
                    }]
                )

            edited_df = st.data_editor(
                df_logs,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key=f"editor_service_logs_{parent_cong_obj.id}_{target_sub_cong_id}",
                column_config={
                    "ID": None,
                    "Data do Culto": st.column_config.DateColumn("Data do Culto", required=True, format="DD/MM/YYYY"),
                    "Tipo de Culto": st.column_config.SelectboxColumn("Tipo de Culto", options=tipos_de_culto, required=True),
                    "Dízimo": st.column_config.NumberColumn("Dízimo", format="R$ %.2f", required=True),
                    "Oferta": st.column_config.NumberColumn("Oferta", format="R$ %.2f", required=True),
                    "Total": st.column_config.NumberColumn("Total", help="Soma do Dízimo e Oferta. Atualiza após salvar.", format="R$ %.2f", disabled=True),
                },
                column_order=["Data do Culto", "Tipo de Culto", "Dízimo", "Oferta", "Total"]
            )

            st.divider()
            try:
                total_dizimo = _to_float_brl(edited_df["Dízimo"].sum())
                total_oferta = _to_float_brl(edited_df["Oferta"].sum())
                total_geral = total_dizimo + total_oferta

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Dízimos (na tabela)", format_currency(total_dizimo))
                col2.metric("Total Ofertas (na tabela)", format_currency(total_oferta))
                col3.metric("Total Geral (na tabela)", format_currency(total_geral))
            except Exception:
                st.caption("Calculando totais...")

            def on_save_click():
                _apply_service_log_changes(df_logs, edited_df, parent_cong_obj.id, sub_cong_id=target_sub_cong_id)
                st.rerun()

            st.markdown('<div class="adrf-entrada">', unsafe_allow_html=True)
            st.button("Salvar alterações na tabela", on_click=on_save_click, key=f"save_table_{parent_cong_obj.id}", type="primary")
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("---")
            tithes_query = select(Tithe).where(
                Tithe.congregation_id == parent_cong_obj.id,
                Tithe.date >= start_tab, Tithe.date < end_tab,
                Tithe.sub_congregation_id == target_sub_cong_id
            )
            tithes = db.scalars(tithes_query.order_by(Tithe.date)).all()
            _editor_dizimos(tithes, f"Dizimistas - {contexto_tabela}", force_cong_id=parent_cong_obj.id, force_sub_cong_id=target_sub_cong_id)

            st.markdown("---")
            txs_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
                Transaction.congregation_id == parent_cong_obj.id,
                Transaction.date >= start_tab, Transaction.date < end_tab,
                Transaction.type == "SAÍDA",
                Transaction.sub_congregation_id == target_sub_cong_id
            )
            txs_out = db.scalars(txs_out_query.order_by(Transaction.date)).all()
            _editor_lancamentos(
                txs_out,
                f"Saídas - {contexto_tabela}",
                tx_type_hint="SAÍDA",
                force_cong_id=parent_cong_obj.id,
                force_sub_cong_id=target_sub_cong_id
            )

# ===================== PAGE: RELATÓRIO DE SAÍDA =====================
def page_relatorio_saida(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Relatório de Saída</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        parent_cong_obj = None
        
        if user.role == "SEDE":
            congs_all = order_congs_sede_first(cong_options_for(user, db))
            escopo_opts = ["-- Relatório Hierárquico (Visualização) --", "-- Visão Agregada (Visualização) --"] + [c.name for c in congs_all]
            escopo_selecionado = st.selectbox("Selecione o escopo do relatório:", escopo_opts, key="rs_sede_escopo")
            
            if escopo_selecionado == "-- Relatório Hierárquico (Visualização) --":
                display_exit_hierarchy(user, congs_all, start, end, db)
                return
            elif escopo_selecionado == "-- Visão Agregada (Visualização) --": # Alterado o label
                st.info("Visualização do total de saídas por congregação principal.")
                _editor_saidas_agg_all(congs_all, start, end)
                return
            else:
                parent_cong_obj = next((c for c in congs_all if c.name == escopo_selecionado), None)
        else: # TESOUREIRO
            parent_cong_obj = db.get(Congregation, user.congregation_id)

        if not parent_cong_obj:
            st.info("Nenhuma congregação para analisar."); return

        st.divider()
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id).order_by(SubCongregation.name)).all()
        
        target_sub_cong_id_or_all = None
        contexto_selecionado = parent_cong_obj.name

        if sub_congs:
            opcoes = {"-- Todas (Principal + Subs) --": "ALL", f"{parent_cong_obj.name} (Principal)": None}
            for sub in sub_congs:
                opcoes[sub.name] = sub.id
            contexto_selecionado = st.selectbox("Filtrar por unidade:", list(opcoes.keys()), key="rs_sub_sel")
            target_sub_cong_id_or_all = opcoes[contexto_selecionado]
        
        st.info(f"Exibindo dados para: **{contexto_selecionado}**")

        if target_sub_cong_id_or_all == "ALL":
            all_units = [(f"{parent_cong_obj.name} (Principal)", None)] + [(s.name, s.id) for s in sub_congs]
            rows = []
            for name, sub_id in all_units:
                totals = _collect_month_data(db, parent_cong_obj.id, start, end, sub_cong_id=sub_id)["totals"]
                rows.append({"Unidade": name, "Total Saídas": totals["saidas_total"]})
            
            df_agg = pd.DataFrame(rows)
            st.dataframe(df_agg.style.format({"Total Saídas": format_currency}), use_container_width=True, hide_index=True)
        
        else:
            txs_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
                Transaction.congregation_id == parent_cong_obj.id, 
                Transaction.date >= start, Transaction.date < end, 
                Transaction.type == TYPE_OUT, 
                Transaction.sub_congregation_id == target_sub_cong_id_or_all
            )
            txs_out = db.scalars(txs_out_query.order_by(Transaction.date)).all()
            
            # ALTERADO: Chamada para _editor_lancamentos substituída por st.dataframe
            st.markdown(f"##### Saídas - {contexto_selecionado}")
            if txs_out:
                rows_out = [{
                    "Data": t.date,
                    "Categoria": t.category.name if t.category else "",
                    "Valor": t.amount,
                    "Descrição": t.description or ""
                } for t in txs_out]
                df_saidas = pd.DataFrame(rows_out)
                st.dataframe(
                    df_saidas.style.format({"Data":"{:%d/%m/%Y}", "Valor": format_currency}),
                    use_container_width=True,
                    hide_index=True
                )
                total_saidas_mes = df_saidas["Valor"].sum()
                st.metric("Total de Saídas (visualização)", format_currency(total_saidas_mes))
            else:
                st.caption("Nenhuma saída registrada neste período.")

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
# SUBSTITUA A SUA FUNÇÃO build_single_unit_report_pdf PELA VERSÃO CORRIGIDA ABAIXO

def build_single_unit_report_pdf(cong_id: int, sub_cong_id: Optional[int], unit_name: str, ref: date, db: Session) -> bytes:
    """
    Gera um PDF de prestação de contas para uma única unidade (principal ou sub).
    ALTERAÇÃO: se for a unidade principal (sub_cong_id=None) e existir sub-congregação,
    delega para build_full_statement_pdf(cong_id, ref, db) para incluir as subs no PDF.
    """
    # >>> ALTERAÇÃO (bloco curto de desvio para PDF consolidado) <<<
    try:
        has_subs = bool(db.scalar(select(func.count(SubCongregation.id)).where(SubCongregation.congregation_id == cong_id)) or 0)
    except Exception:
        has_subs = False
    if sub_cong_id is None and has_subs:
        # Gera o PDF consolidado (principal + subs) reaproveitando sua função existente
        return build_full_statement_pdf(parent_cong_id=cong_id, ref=ref, db=db)
    # >>> FIM DA ALTERAÇÃO <<<

    # ======= A PARTIR DAQUI, CÓDIGO ORIGINAL MANTIDO =======
    from io import BytesIO
    from reportlab.lib.pagesizes import A4, portrait
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
    from reportlab.lib.enums import TA_CENTER

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    start, end = month_bounds(ref)

    # Estilos
    TA_RIGHT = 2
    title_style = ParagraphStyle('title', parent=styles['h1'], alignment=TA_CENTER, fontSize=16, spaceAfter=4)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=11, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['h2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    normal_style = styles['Normal']
    right_align_style = ParagraphStyle('rightAlign', parent=styles['Normal'], alignment=TA_RIGHT)
    signature_style = ParagraphStyle('signature', parent=styles['Normal'], alignment=TA_CENTER, spaceBefore=0)
    
    story: List = []

    # Cabeçalho do Documento
    story.append(Paragraph("Prestação de Contas Mensal", title_style))
    story.append(Paragraph(f"Unidade: {unit_name}", subtitle_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 0.5*cm))

    # Coleta de dados gerais (para Saídas e Resumo Final)
    data_geral = _collect_month_data(db, cong_id, start, end, sub_cong_id=sub_cong_id)
    totals_gerais = data_geral["totals"]
    
    # ===== Tabela de Entradas (CORRIGIDA) =====
    story.append(Paragraph("1. Entradas (Resumo por Culto)", heading_style))
    
    # Usa a função correta para buscar os logs de serviço
    df_entradas = _load_service_logs(db, cong_id, start, end, sub_cong_id=sub_cong_id)
    
    if not df_entradas.empty:
        data_in = [["Data", "Tipo de Culto", "Dízimo", "Oferta", "Total"]]
        for _, row in df_entradas.iterrows():
            data_in.append([
                row["Data do Culto"].strftime("%d/%m/%Y"),
                Paragraph(str(row["Tipo de Culto"]), normal_style),
                format_currency(row["Dízimo"]),
                format_currency(row["Oferta"]),
                format_currency(row["Total"])
            ])
        
        total_dizimo_cultos = df_entradas['Dízimo'].sum()
        total_oferta_cultos = df_entradas['Oferta'].sum()
        total_geral_cultos = df_entradas['Total'].sum()

        data_in.append([
            Paragraph("<b>Totais</b>", normal_style), "",
            Paragraph(f"<b>{format_currency(total_dizimo_cultos)}</b>", right_align_style),
            Paragraph(f"<b>{format_currency(total_oferta_cultos)}</b>", right_align_style),
            Paragraph(f"<b>{format_currency(total_geral_cultos)}</b>", right_align_style)
        ])
        
        tbl_in = Table(data_in, colWidths=[2.5*cm, 6*cm, 2.5*cm, 2.5*cm, 3*cm], repeatRows=1)
        tbl_in.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 1, colors.grey), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('ALIGN', (2,1), (-1,-1), 'RIGHT'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('SPAN', (0,-1), (1,-1)),
            ('BACKGROUND', (0,-1), (-1,-1), colors.lightgreen)
        ]))
        story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada registada.", normal_style))
    story.append(Spacer(1, 0.5*cm))

    # Tabela de Saídas
    story.append(Paragraph("2. Saídas", heading_style))
    if data_geral["tx_out"]:
        data_out = [["Data", "Categoria", "Descrição", "Valor"]]
        for t in data_geral["tx_out"]:
            data_out.append([t.date.strftime("%d/%m/%Y"), t.category.name, t.description or "", format_currency(t.amount)])
        
        data_out.append([Paragraph("<b>Total de Saídas:</b>", right_align_style), "", "", Paragraph(f"<b>{format_currency(totals_gerais['saidas_total'])}</b>", right_align_style)])
        
        tbl_out = Table(data_out, colWidths=[2.5*cm, 4.5*cm, 6.5*cm, 3*cm], repeatRows=1)
        tbl_out.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 1, colors.grey), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('ALIGN', (3,1), (3,-1), 'RIGHT'), ('SPAN', (0,-1), (2,-1)),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,-1), (-1,-1), colors.lightgreen)
        ]))
        story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída registada.", normal_style))
    story.append(Spacer(1, 1*cm))

    # Tabela de Resumo Financeiro
    story.append(Paragraph("3. Resumo Financeiro da Unidade", heading_style))
    entradas_resumo = df_entradas['Total'].sum() if not df_entradas.empty else 0.0
    saidas_resumo = totals_gerais['saidas_total']
    saldo_resumo = entradas_resumo - saidas_resumo
    summary_data = [
        ["Total de Entradas", format_currency(entradas_resumo)],
        ["Total de Saídas", format_currency(saidas_resumo)],
        [Paragraph("<b>Saldo do Mês</b>", normal_style), Paragraph(f"<b>{format_currency(saldo_resumo)}</b>", normal_style)]
    ]
    tbl_summary = Table(summary_data, colWidths=[8*cm, 8.5*cm])
    tbl_summary.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.grey), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,2), (-1,2), colors.lightcyan)]))
    story.append(tbl_summary)

    # Assinaturas
    story.append(Spacer(1, 2.5*cm))
    assinaturas = ["Dirigente da Congregação", "Responsável pelas Ofertas"]
    for assinatura in assinaturas:
        story.append(Paragraph("_" * 40, signature_style))
        story.append(Paragraph(assinatura, signature_style))
        story.append(Spacer(1, 0.8*cm))

    doc.build(story)
    return buf.getvalue()


def page_relatorio_dizimistas(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Relatório de Dizimistas</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        congs = cong_options_for(user, db)
        if user.role == "SEDE":
            ordered = order_congs_sede_first(congs)
            esc_opt = ["Todas as congregações"] + [c.name for c in ordered]
            esc = st.selectbox("Escopo", esc_opt, key="rd_escopo")
            is_all = (esc == "Todas as congregações")
            cong_obj = None if is_all else next((c for c in ordered if c.name == esc), None)
        else:
            cong_obj = congs[0] if congs else None; is_all = False

        if not cong_obj and not is_all:
            st.info("Sem congregação vinculada."); return
        if cong_obj:
            st.info(f"Escopo: **{cong_obj.name}**")

        if is_all:
            all_tz_q = select(Tithe).where(Tithe.date >= start, Tithe.date < end).options(joinedload(Tithe.congregation))
            all_tz = db.scalars(all_tz_q).all()
            by_cong = defaultdict(lambda: {"qtd":0, "valor":0.0})
            for t in all_tz:
                k = t.congregation.name if t.congregation else "N/A"
                by_cong[k]["qtd"] += 1
                by_cong[k]["valor"] += float(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Qtde de dizimistas": v["qtd"], "Total (R$)": v["valor"]} for k,v in sorted(by_cong.items())])
            st.dataframe(df.style.format({"Total (R$)": format_currency}), use_container_width=True, hide_index=True)
            st.info("Selecione uma congregação específica para ver a lista nominal.")
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
            if tithes_by_payment:
                cols_metrics = st.columns(len(tithes_by_payment))
                for i, (method, datax) in enumerate(tithes_by_payment.items()):
                    cols_metrics[i].metric(f"Total ({method})", format_currency(datax["total"]), f"{datax['count']} dízimos")

            st.divider()
            
            st.markdown("##### Dizimistas do Período (Visualização)")
            if tithes:
                rows = [
                    {
                        "Data": t.date, 
                        "Dizimista": t.tither_name, 
                        "Valor": float(t.amount), 
                        "Forma de Pagamento": t.payment_method or "—"
                    } 
                    for t in tithes
                ]
                df_tithes = pd.DataFrame(rows)
                st.dataframe(
                    df_tithes.style.format({
                        "Data": "{:%d/%m/%Y}",
                        "Valor": format_currency
                    }),
                    use_container_width=True,
                    hide_index=True
                )
                total_mes = df_tithes["Valor"].sum()
                st.metric("Total de Dízimos (nominal) no período", format_currency(total_mes))
            else:
                st.caption("Nenhum dízimo nominal registrado para este período.")

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
                cong_sel = cong_obj.name if cong_obj else "N/A"
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
                cong_id_sel = next((c.id for c in congs if c.name == cong_sel), None)
                if cong_id_sel:
                    q = q.where(Tithe.congregation_id == cong_id_sel)
        elif cong_obj:
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
            
            df_display = df_show.copy()
            df_display["Total no ano (R$)"] = df_display["Total no ano (R$)"].map(format_currency)

            st.dataframe(df_display, use_container_width=True, hide_index=True, height=320)
            
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
            
            pdf_data = build_dizimista_search_pdf(df_show, ano_pesq, cong_sel, mes_sel, nome_q)
            st.download_button("⬇️ Baixar PDF da pesquisa", data=pdf_data, file_name=f"pesquisa_dizimistas_{ano_pesq}.pdf", mime="application/pdf")
        else:
            st.caption("Nenhum resultado para os filtros informados.")

# ===================== PDFs =====================
def build_full_statement_pdf(parent_cong_id: int, ref: date, db: Session) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    start, end = month_bounds(ref)

    # --- CORREÇÃO DEFINITIVA ---
    TA_RIGHT = 2
    
    # Estilos
    title_style = ParagraphStyle('title', parent=styles['h1'], alignment=TA_CENTER, fontSize=16, spaceAfter=4)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=11, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['h2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    normal_style = styles['Normal']
    right_align_style = ParagraphStyle('rightAlign', parent=styles['Normal'], alignment=TA_RIGHT)
    signature_style = ParagraphStyle('signature', parent=styles['Normal'], alignment=TA_CENTER, spaceBefore=0)
    
    story: List = []

    # Coleta de dados
    parent_cong_obj = db.get(Congregation, parent_cong_id)
    sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id).order_by(SubCongregation.name)).all()
    
    doc_title = f"{parent_cong_obj.name} e suas unidades" if sub_congs else parent_cong_obj.name
    all_units = [(f"{parent_cong_obj.name} (Principal)", None)] + [(s.name, s.id) for s in sub_congs] if sub_congs else [(parent_cong_obj.name, None)]

    grand_total_entradas = 0.0
    grand_total_saidas = 0.0

    # Cabeçalho do Documento
    story.append(Paragraph("Prestação de Contas Mensal", title_style))
    story.append(Paragraph(f"Congregação: {doc_title}", subtitle_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))

    # Loop para gerar seções para cada unidade
    for name, sub_id in all_units:
        story.append(Spacer(1, 1*cm))
        story.append(Paragraph(f"Detalhes da Unidade: {name}", heading_style))
        
        data = _collect_month_data(db, parent_cong_obj.id, start, end, sub_cong_id=sub_id)
        totals = data["totals"]
        unit_total_entradas = totals["entradas_total_sem_missoes"]
        unit_total_saidas = totals["saidas_total"]
        grand_total_entradas += unit_total_entradas
        grand_total_saidas += unit_total_saidas

        # Tabela de Entradas da Unidade
        story.append(Paragraph("<b>1. Entradas</b>", normal_style))
        df_entradas = _entrada_summary_df(db, parent_cong_obj.id, start, end, sub_cong_id=sub_id)
        if not df_entradas.empty:
            data_in = [["Data do Culto", "Dízimo", "Oferta", "Total"]]
            for _, row in df_entradas.iterrows():
                data_in.append([row["Data do Culto"].strftime("%d/%m/%Y"), format_currency(row["Dízimo"]), format_currency(row["Oferta"]), format_currency(row["Total"])])
            
            data_in.append([
                Paragraph("<b>Totais</b>", normal_style),
                Paragraph(f"<b>{format_currency(totals['dizimos'])}</b>", normal_style),
                Paragraph(f"<b>{format_currency(totals['ofertas'])}</b>", normal_style),
                Paragraph(f"<b>{format_currency(totals['entradas_total_sem_missoes'])}</b>", normal_style)
            ])
            
            tbl_in = Table(data_in, colWidths=[3.2*cm, 4.0*cm, 4.0*cm, 5.3*cm], repeatRows=1)
            tbl_in.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 1, colors.grey), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                ('ALIGN', (1,1), (-1,-1), 'RIGHT'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                ('BACKGROUND', (0,-1), (-1,-1), colors.lightgreen)
            ]))
            story.append(tbl_in)
        else:
            story.append(Paragraph("Nenhuma entrada registrada.", normal_style))
        story.append(Spacer(1, 0.5*cm))

        # Tabela de Saídas da Unidade
        story.append(Paragraph("<b>2. Saídas</b>", normal_style))
        txs_out = data["tx_out"]
        if txs_out:
            data_out = [["Data", "Categoria", "Descrição", "Valor"]]
            for t in txs_out:
                data_out.append([t.date.strftime("%d/%m/%Y"), t.category.name, t.description or "", format_currency(t.amount)])
            
            data_out.append([Paragraph("<b>Total de Saídas:</b>", right_align_style), "", "", Paragraph(f"<b>{format_currency(unit_total_saidas)}</b>", right_align_style)])
            
            tbl_out = Table(data_out, colWidths=[2.5*cm, 4.5*cm, 6.5*cm, 3*cm], repeatRows=1)
            tbl_out.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 1, colors.grey), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                ('ALIGN', (3,1), (3,-1), 'RIGHT'), ('SPAN', (0,-1), (2,-1)), 
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,-1), (-1,-1), colors.lightgreen)
            ]))
            story.append(tbl_out)
        else:
            story.append(Paragraph("Nenhuma saída registrada.", normal_style))
        story.append(Spacer(1, 0.5*cm))
        
        if sub_congs:
            story.append(Paragraph(f"<b>3. Resumo da Unidade: {name}</b>", normal_style))
            unit_saldo = unit_total_entradas - unit_total_saidas
            unit_summary_data = [
                ["Total de Entradas da Unidade", format_currency(unit_total_entradas)],
                ["Total de Saídas da Unidade", format_currency(unit_total_saidas)],
                [Paragraph("<b>Saldo da Unidade</b>", normal_style), Paragraph(f"<b>{format_currency(unit_saldo)}</b>", normal_style)]
            ]
            tbl_unit_summary = Table(unit_summary_data, colWidths=[8*cm, 8.5*cm])
            tbl_unit_summary.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.grey), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,2), (-1,2), colors.lightyellow)]))
            story.append(tbl_unit_summary)

    # Resumo Financeiro Geral
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph("Resumo Financeiro Geral", heading_style))
    saldo_final = grand_total_entradas - grand_total_saidas
    summary_data = [
        ["Total Geral de Entradas", format_currency(grand_total_entradas)],
        ["Total Geral de Saídas", format_currency(grand_total_saidas)],
        [Paragraph("<b>Saldo do Mês (Entradas - Saídas)</b>", normal_style), Paragraph(f"<b>{format_currency(saldo_final)}</b>", normal_style)]
    ]
    tbl_summary = Table(summary_data, colWidths=[8*cm, 8.5*cm])
    tbl_summary.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.grey), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,2), (-1,2), colors.lightcyan)]))
    story.append(tbl_summary)
    
    # Assinaturas
    story.append(Spacer(1, 2.5*cm))
    assinaturas = ["Dirigente da Congregação", "Responsável pelas Ofertas"]
    for assinatura in assinaturas:
        story.append(Paragraph("_" * 40, signature_style))
        story.append(Paragraph(assinatura, signature_style))
        story.append(Spacer(1, 0.8*cm))

    doc.build(story)
    return buf.getvalue()

def build_consolidated_pdf(congs_all: List[Congregation], ref: date, db: Session) -> bytes:
    """Gera o PDF consolidado hierárquico para a Sede, com o novo layout e ordenação."""
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    start, end = month_bounds(ref)

    # Estilos
    TA_RIGHT = 2
    title_style = ParagraphStyle('title', parent=styles['h1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=11, spaceAfter=16)
    heading_style = ParagraphStyle('heading', parent=styles['h2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    normal_style = styles['Normal']
    
    story: List = []
    story.append(Paragraph("Relatório Consolidado Mensal", title_style))
    story.append(Paragraph(f"Mês de Referência: {ref.strftime('%B de %Y')}", subtitle_style))

    grand_total_entradas = 0.0
    grand_total_saidas = 0.0

    # --- Tabela 1: Resumo de Entradas Hierárquico ---
    story.append(Paragraph("1. Resumo de Entradas por Unidade", heading_style))
    entry_data = [["Unidade", "Valor (R$)"]]
    
    # Coleta e processa os dados de entrada primeiro
    cong_data_list = []
    for cong in congs_all:
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == cong.id)).all()
        
        df_principal = _load_service_logs(db, cong.id, start, end, None)
        principal_entradas = df_principal['Total'].sum() if not df_principal.empty else 0.0
        
        total_subs = 0.0
        subs_data = []
        for sub in sub_congs:
            df_sub = _load_service_logs(db, cong.id, start, end, sub.id)
            sub_entradas = df_sub['Total'].sum() if not df_sub.empty else 0.0
            subs_data.append({"name": sub.name, "total": sub_entradas})
            total_subs += sub_entradas
            
        cong_total = principal_entradas + total_subs
        cong_data_list.append({
            "name": cong.name,
            "principal_total": principal_entradas,
            "subs_data": subs_data,
            "cong_total": cong_total
        })

    # Separa a Sede e ordena o resto por maior entrada
    sede_data = next((c for c in cong_data_list if _norm(c["name"]) == "sede"), None)
    other_congs_data = sorted([c for c in cong_data_list if _norm(c["name"]) != "sede"], key=lambda x: x["cong_total"], reverse=True)
    
    sorted_congs = ([sede_data] if sede_data else []) + other_congs_data

    for cong_data in sorted_congs:
        grand_total_entradas += cong_data["cong_total"]
        entry_data.append([Paragraph(f"{cong_data['name']} (Principal)", normal_style), format_currency(cong_data["principal_total"])])
        for sub_data in cong_data["subs_data"]:
            entry_data.append([Paragraph(f"↳ {sub_data['name']}", normal_style), format_currency(sub_data["total"])])
        
        if cong_data["subs_data"]: # Só mostra total do grupo se tiver subs
            entry_data.append([Paragraph(f"<b>{cong_data['name']} (Total)</b>", normal_style), Paragraph(f"<b>{format_currency(cong_data['cong_total'])}</b>", normal_style)])

    entry_data.append([Paragraph("<b>Total Geral de Entradas</b>", normal_style), Paragraph(f"<b>{format_currency(grand_total_entradas)}</b>", normal_style)])
    tbl_in = Table(entry_data, colWidths=[12*cm, 4*cm])
    tbl_in.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.grey), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,-1), (-1,-1), colors.lightyellow)]))
    story.append(tbl_in)
    story.append(Spacer(1, 0.8*cm))

    # --- Tabela 2: Detalhamento de Saídas por Categoria ---
    story.append(Paragraph("2. Total de Saídas por Categoria", heading_style))
    exit_data = [["Categoria de Saída", "Valor Total (R$)"]]
    
    cat_miss_saida = db.scalar(select(Category).where(func.lower(Category.name) == 'missões (saída)'))
    cat_miss_saida_id = cat_miss_saida.id if cat_miss_saida else -1

    saidas_por_categoria_q = select(
        Category.name, func.sum(Transaction.amount)
    ).join(Transaction).where(
        Transaction.date >= start, Transaction.date < end,
        Transaction.type == "SAÍDA",
        Transaction.category_id != cat_miss_saida_id
    ).group_by(Category.name).order_by(func.sum(Transaction.amount).desc())
    
    results = db.execute(saidas_por_categoria_q).all()
    for cat_name, total in results:
        exit_data.append([cat_name, format_currency(total)])
        grand_total_saidas += float(total or 0.0)
    
    exit_data.append([Paragraph("<b>Total Geral de Saídas</b>", normal_style), Paragraph(f"<b>{format_currency(grand_total_saidas)}</b>", normal_style)])
    tbl_out = Table(exit_data, colWidths=[12*cm, 4*cm])
    tbl_out.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.grey), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,-1), (-1,-1), colors.lightyellow)]))
    story.append(tbl_out)
    story.append(Spacer(1, 0.8*cm))

    # --- Tabela 3: Resumo Financeiro Geral ---
    story.append(Paragraph("3. Resumo Financeiro Geral", heading_style))
    saldo_final = grand_total_entradas - grand_total_saidas
    summary_data = [
        ["Total Geral de Entradas", format_currency(grand_total_entradas)],
        ["Total Geral de Saídas", format_currency(grand_total_saidas)],
        [Paragraph("<b>Saldo do Mês</b>", normal_style), Paragraph(f"<b>{format_currency(saldo_final)}</b>", normal_style)]
    ]
    tbl_summary = Table(summary_data, colWidths=[8*cm, 8*cm])
    tbl_summary.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.grey), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,2), (-1,2), colors.lightcyan)]))
    story.append(tbl_summary)

    # --- Bloco de Assinaturas ---
    story.append(Spacer(1, 2.5*cm))
    signature_style = ParagraphStyle('signature', parent=styles['Normal'], alignment=TA_CENTER, spaceBefore=0)
    
    col_width = doc.width / 2.0 - 0.5*cm
    
    left_signatures_text = """
    _________________________<br/>
    Pastor Presidente<br/><br/><br/>
    _________________________<br/>
    Primeiro Tesoureiro<br/><br/><br/>
    _________________________<br/>
    Segundo Tesoureiro
    """
    
    right_signatures_text = """
    _________________________<br/>
    Primeiro Conselho Fiscal<br/><br/><br/>
    _________________________<br/>
    Segundo Conselho Fiscal<br/><br/><br/>
    _________________________<br/>
    Terceiro Conselho Fiscal
    """
    
    left_paragraph = Paragraph(left_signatures_text, signature_style)
    right_paragraph = Paragraph(right_signatures_text, signature_style)
    
    signature_table = Table([[left_paragraph, right_paragraph]], colWidths=[col_width, col_width])
    signature_table.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'TOP')]))
    story.append(signature_table)

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
# ===================== PAGE: VISÃO GERAL =====================
def page_visao_geral(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)
        
        congs = cong_options_for(user, db)
        ordered_congs = order_congs_sede_first(congs)
        
        if not ordered_congs:
            st.info("Nenhuma congregação para analisar."); return

        display_congs = ordered_congs if user.role == "SEDE" else [db.get(Congregation, user.congregation_id)]
        if user.role == "SEDE":
            st.info("Escopo: **Todas as congregações**")
        else:
            st.info(f"Escopo: **{display_congs[0].name} e suas unidades**")

        report_data = []
        for cong in display_congs:
            sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == cong.id)).all()
            all_units = [(f"{cong.name} (Principal)", None)] + [(f"↳ {s.name}", s.id) for s in sub_congs]

            for unit_name, sub_id in all_units:
                df_entradas = _load_service_logs(db, cong.id, start, end, sub_id)
                total_dizimos = df_entradas['Dízimo'].sum() if not df_entradas.empty else 0.0
                total_ofertas = df_entradas['Oferta'].sum() if not df_entradas.empty else 0.0
                total_geral_entradas = total_dizimos + total_ofertas

                dados_saidas = _collect_month_data(db, cong.id, start, end, sub_id)
                total_saidas = dados_saidas["totals"]["saidas_total"]
                saldo_total = total_geral_entradas - total_saidas

                report_data.append({
                    "Unidade": unit_name,
                    "Total de Dízimos": total_dizimos,
                    "Total de Ofertas": total_ofertas,
                    "Total Geral (Entradas)": total_geral_entradas,
                    "Total de Saídas": total_saidas,
                    "Saldo Total": saldo_total
                })

        if not report_data:
            st.warning("Nenhum dado encontrado para o período selecionado."); return

        df_summary = pd.DataFrame(report_data)
        
        st.dataframe(
            df_summary.style.format({
                "Total de Dízimos": format_currency, "Total de Ofertas": format_currency,
                "Total Geral (Entradas)": format_currency, "Total de Saídas": format_currency,
                "Saldo Total": format_currency,
            }), 
            use_container_width=True, hide_index=True
        )

        st.divider()
        grand_total_entradas = df_summary["Total Geral (Entradas)"].sum()
        grand_total_saidas = df_summary["Total de Saídas"].sum()
        grand_saldo_total = df_summary["Saldo Total"].sum()

        st.markdown("#### Totais Gerais do Período")
        c1, c2, c3 = st.columns(3)
        c1.metric("Total de Entradas", format_currency(grand_total_entradas))
        c2.metric("Total de Saídas", format_currency(grand_total_saidas))
        c3.metric("Saldo Final", format_currency(grand_saldo_total))
        
        st.divider()
        st.subheader("Downloads de Relatórios (PDF)")
        
        if user.role == "SEDE":
            st.download_button(
                "⬇️ Baixar Relatório Geral Consolidado (PDF)",
                data=build_consolidated_pdf(ordered_congs, ref, db),
                file_name=f"relatorio_geral_consolidado_{ref.strftime('%Y-%m')}.pdf",
                mime="application/pdf",
                key="dl_pdf_geral_consolidado"
            )
        
        sel_cong_name = st.selectbox(
            "Selecione a congregação para gerar o relatório detalhado individual:",
            [c.name for c in display_congs],
            key="vg_sel_cong_pdf"
        )
        if sel_cong_name:
            selected_cong_obj = next((c for c in display_congs if c.name == sel_cong_name), None)
            if selected_cong_obj:
                st.download_button(
                    f"⬇️ Baixar PDF de {selected_cong_obj.name} (e suas subs)",
                    data=build_single_unit_report_pdf(selected_cong_obj.id, None, selected_cong_obj.name, ref, db),
                    file_name=f"prestacao_{_norm(selected_cong_obj.name)}_{ref.strftime('%Y-%m')}.pdf",
                    mime="application/pdf",
                    key=f"dl_pdf_cong_{_norm(selected_cong_obj.name)}"
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
    
    # Estilos
    title_style = ParagraphStyle('title', parent=styles['h1'], alignment=TA_CENTER, fontSize=16, spaceAfter=8)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=11, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['h2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    normal_style = styles['Normal']
    signature_style = ParagraphStyle('signature', parent=styles['Normal'], alignment=TA_CENTER, spaceBefore=0)
    table_style = TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (-1, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ])
    
    story: List = []
    
    story.append(Paragraph("Relatório Mensal de Missões", title_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))
    story.append(Spacer(1, 0.5*cm))

    story.append(Paragraph("Entradas de Missões", heading_style))
    if entradas:
        entradas_data = [["Data", "Congregação", "Valor (R$)"]]
        for t in entradas:
            entradas_data.append([t.date.strftime("%d/%m/%Y"), t.congregation.name, format_currency(float(t.amount))])
        tbl_in = Table(entradas_data, colWidths=[3*cm, 9*cm, 5*cm])
        tbl_in.setStyle(table_style)
        story.append(tbl_in)
    else:
        story.append(Paragraph("Nenhuma entrada de missões registrada.", normal_style))

    story.append(Spacer(1, 0.8*cm))
    story.append(Paragraph("Saídas de Missões", heading_style))
    if saidas:
        saidas_data = [["Data", "Congregação", "Descrição", "Valor (R$)"]]
        for t in saidas:
            saidas_data.append([t.date.strftime("%d/%m/%Y"), t.congregation.name if t.congregation else "—", t.description or "—", format_currency(float(t.amount))])
        tbl_out = Table(saidas_data, colWidths=[3*cm, 5*cm, 6*cm, 3*cm])
        tbl_out.setStyle(table_style)
        story.append(tbl_out)
    else:
        story.append(Paragraph("Nenhuma saída de missões registrada.", normal_style))

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
    summary_table = Table(summary_data, colWidths=[8*cm, 8.5*cm])
    summary_table.setStyle(TableStyle([
        ('GRID', (0,0), (-1,-1), 1, colors.grey),
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica'),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'),
        ('BACKGROUND', (0,2), (-1,2), colors.lightyellow),
        ('FONTNAME', (0,2), (-1,2), 'Helvetica-Bold'),
    ]))
    story.append(summary_table)

    story.append(Spacer(1, 2.5*cm))
    
    assinaturas = [
        "Pastor Presidente", "Tesoureiro de Missões",
        "1º Conselho Fiscal", "2º Conselho Fiscal", "3º Conselho Fiscal"
    ]
    for assinatura in assinaturas:
        story.append(Paragraph("_" * 40, signature_style))
        story.append(Paragraph(assinatura, signature_style))
        story.append(Spacer(1, 0.8*cm))

    doc.build(story)
    return buf.getvalue()

def _build_missions_analytics(db: Session, year: int, month_name: str):
    """
    Busca e agrega as contribuições de missões, identificando os maiores contribuintes.
    """
    # Define o período da pesquisa (ano inteiro ou um mês específico)
    month_num = None
    if month_name != "Todos":
        try:
            month_num = MONTHS.index(month_name) + 1
        except ValueError:
            month_name = "Todos"
    
    start_date = date(year, month_num, 1) if month_num else date(year, 1, 1)
    end_date = date(year + (1 if month_num == 12 else 0), (month_num % 12) + 1, 1) if month_num else date(year + 1, 1, 1)
    
    # Query para o período selecionado
    q_period = select(
        Congregation.name, func.sum(Transaction.amount)
    ).join(Transaction).join(Category).where(
        Transaction.date >= start_date, Transaction.date < end_date,
        Transaction.type == "DOAÇÃO", func.lower(Category.name) == 'missões'
    ).group_by(Congregation.name)

    # Query separada para o ano inteiro, para encontrar o maior contribuinte anual
    q_year = select(
        Congregation.name, func.sum(Transaction.amount)
    ).join(Transaction).join(Category).where(
        Transaction.date >= date(year, 1, 1), Transaction.date < date(year + 1, 1, 1),
        Transaction.type == "DOAÇÃO", func.lower(Category.name) == 'missões'
    ).group_by(Congregation.name)

    period_data = {name: val for name, val in db.execute(q_period).all()}
    year_data = {name: val for name, val in db.execute(q_year).all()}
    
    all_congs = set(list(period_data.keys()) + list(year_data.keys()))
    
    report_rows = []
    for cong_name in sorted(list(all_congs)):
        report_rows.append({
            "Congregação": cong_name,
            "Total no Período (R$)": period_data.get(cong_name, 0.0),
            "Total no Ano (R$)": year_data.get(cong_name, 0.0)
        })

    if not report_rows:
        return pd.DataFrame(), 0, 0, None, None

    df = pd.DataFrame(report_rows)
    total_periodo = df["Total no Período (R$)"].sum()
    num_congs_periodo = len(df[df["Total no Período (R$)"] > 0])

    top_period_contributor = None
    if num_congs_periodo > 0:
        top_period_row = df.loc[df['Total no Período (R$)'].idxmax()]
        top_period_contributor = (top_period_row['Congregação'], top_period_row['Total no Período (R$)'])

    top_year_contributor = None
    if not df[df["Total no Ano (R$)"] == 0].all():
        top_year_row = df.loc[df['Total no Ano (R$)'].idxmax()]
        top_year_contributor = (top_year_row['Congregação'], top_year_row['Total no Ano (R$)'])
    
    df_sorted = df.sort_values("Total no Período (R$)", ascending=False).reset_index(drop=True)
    return df_sorted, total_periodo, num_congs_periodo, top_period_contributor, top_year_contributor

def _build_missions_search_df(db: Session, year: int, month_name: str):
    """
    Busca e agrega as contribuições de missões, identificando o Top 5 de contribuintes.
    """
    month_num = None
    if month_name != "Todos":
        try:
            month_num = MONTHS.index(month_name) + 1
        except ValueError:
            month_name = "Todos"
    
    start_date = date(year, month_num, 1) if month_num else date(year, 1, 1)
    end_date = date(year + (1 if month_num == 12 else 0), (month_num % 12) + 1, 1) if month_num else date(year + 1, 1, 1)
    
    q_period = select(
        Congregation.name, func.sum(Transaction.amount)
    ).join(Transaction).join(Category).where(
        Transaction.date >= start_date, Transaction.date < end_date,
        Transaction.type == "DOAÇÃO", func.lower(Category.name) == 'missões'
    ).group_by(Congregation.name)

    q_year = select(
        Congregation.name, func.sum(Transaction.amount)
    ).join(Transaction).join(Category).where(
        Transaction.date >= date(year, 1, 1), Transaction.date < date(year + 1, 1, 1),
        Transaction.type == "DOAÇÃO", func.lower(Category.name) == 'missões'
    ).group_by(Congregation.name)

    period_data = {name: val for name, val in db.execute(q_period).all()}
    year_data = {name: val for name, val in db.execute(q_year).all()}
    
    all_congs = set(list(period_data.keys()) + list(year_data.keys()))
    
    report_rows = []
    for cong_name in sorted(list(all_congs)):
        report_rows.append({
            "Congregação": cong_name,
            "Total no Período (R$)": period_data.get(cong_name, 0.0),
            "Total no Ano (R$)": year_data.get(cong_name, 0.0)
        })

    if not report_rows:
        return pd.DataFrame(), 0.0, 0, pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(report_rows)
    total_periodo = df["Total no Período (R$)"].sum()
    num_congs_periodo = len(df[df["Total no Período (R$)"] > 0])

    # ===== NOVO: Calcula o Top 5 =====
    df_top_period = df[df["Total no Período (R$)"] > 0].sort_values("Total no Período (R$)", ascending=False).head(5)
    df_top_year = df[df["Total no Ano (R$)"] > 0].sort_values("Total no Ano (R$)", ascending=False).head(5)
    
    df_sorted = df.sort_values("Total no Período (R$)", ascending=False).reset_index(drop=True)
    
    return df_sorted, total_periodo, num_congs_periodo, df_top_period, df_top_year

# ======== Páginas de Missões ========
def page_relatorio_missoes(user: "User"):
    """Página de gestão de Missões com abas para Lançamento e Relatório."""
    if user.role not in ["SEDE", "TESOUREIRO MISSIONÁRIO"]:
        page_relatorio_missoes_congregacao(user)
        return
        
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Gestão de Missões</h1>", unsafe_allow_html=True)
        
        tab1, tab2 = st.tabs(["Lançamentos (Editar)", "Relatório e Análise (Visualizar)"])

        with tab1:
            # (O conteúdo desta aba permanece o mesmo)
            st.subheader("Editar Lançamentos de Missões")
            ref_lanc = get_month_selector("Mês para Lançamento", key_prefix="lanc_missions")
            start_lanc, end_lanc = month_bounds(ref_lanc)
            congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()

            st.markdown("###### Entradas de Missões — por Congregação")
            _editor_missions_entries_agg(congs_all, start_lanc, end_lanc, "missoes_entradas_agg")

            st.markdown("###### Saídas de Missões")
            _, saidas_missoes = _collect_missions_data(db, start_lanc, end_lanc)
            _editor_missions_outflows(saidas_missoes, "missoes_saidas", congs_all)
            
            st.divider()
            st.subheader("Gerar Relatório de Missões (PDF)")
            entradas_missoes_pdf, saidas_missoes_pdf = _collect_missions_data(db, start_lanc, end_lanc)
            st.download_button(
                "⬇️ Baixar PDF de Lançamentos de Missões",
                data=build_missions_report_pdf(ref_lanc, entradas_missoes_pdf, saidas_missoes_pdf),
                file_name=f"lancamentos_missoes_{start_lanc.strftime('%Y-%m')}.pdf",
                mime="application/pdf"
            )

        with tab2:
            st.subheader("Análise de Contribuições de Missões")
            
            c1, c2 = st.columns(2)
            with c1:
                ano_pesq = st.number_input("Ano da Pesquisa", value=today_bahia().year, step=1, format="%d", key="missions_search_year")
            with c2:
                mes_opt = ["Todos"] + MONTHS
                mes_sel = st.selectbox("Mês da Pesquisa", mes_opt, index=0, key="missions_search_month")

            df_search, total_periodo, num_congs, df_top_period, df_top_year = _build_missions_search_df(db, ano_pesq, mes_sel)

            st.divider()
            
            st.markdown("##### Destaques do Período Selecionado")
            c1, c2 = st.columns(2)
            c1.metric("Total de Entradas no Período", format_currency(total_periodo))
            c2.metric("Nº de Congregações Contribuintes", f"{num_congs}")
            
            st.markdown("---")
            
            # ===== NOVO: Exibição do Top 5 =====
            col_top1, col_top2 = st.columns(2)
            with col_top1:
                st.markdown(f"**Top 5 Contribuintes ({mes_sel if mes_sel != 'Todos' else 'Período'})**")
                if not df_top_period.empty:
                    st.dataframe(
                        df_top_period[['Congregação', 'Total no Período (R$)']].style.format({"Total no Período (R$)": format_currency}),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.caption("Nenhum contribuinte no período.")
            
            with col_top2:
                st.markdown(f"**Top 5 Contribuintes (Ano de {ano_pesq})**")
                if not df_top_year.empty:
                    st.dataframe(
                        df_top_year[['Congregação', 'Total no Ano (R$)']].style.format({"Total no Ano (R$)": format_currency}),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.caption("Nenhum contribuinte no ano.")

            st.divider()
            st.markdown("###### Tabela Geral de Contribuições")
            if not df_search.empty:
                st.dataframe(
                    df_search.style.format({"Total no Período (R$)": format_currency, "Total no Ano (R$)": format_currency}),
                    use_container_width=True, hide_index=True
                )
            else:
                st.caption("Nenhuma contribuição de missões encontrada para os filtros selecionados.")

def page_relatorio_missoes(user: "User"):
    """Página de gestão de Missões com abas para Lançamento e Relatório."""
    if user.role not in ["SEDE", "TESOUREIRO MISSIONÁRIO"]:
        page_relatorio_missoes_congregacao(user)
        return
        
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Gestão de Missões</h1>", unsafe_allow_html=True)
        
        tab1, tab2 = st.tabs(["Lançamentos (Editar)", "Relatório e Análise (Visualizar)"])

        with tab1:
            st.subheader("Editar Lançamentos de Missões")
            ref_lanc = get_month_selector("Mês para Lançamento", key_prefix="lanc_missions")
            start_lanc, end_lanc = month_bounds(ref_lanc)
            congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()

            st.markdown("###### Entradas de Missões — por Congregação")
            _editor_missions_entries_agg(congs_all, start_lanc, end_lanc, "missoes_entradas_agg")

            st.markdown("###### Saídas de Missões")
            _, saidas_missoes = _collect_missions_data(db, start_lanc, end_lanc)
            _editor_missions_outflows(saidas_missoes, "missoes_saidas", congs_all)
            
            st.divider()
            st.subheader("Gerar Relatório de Missões (PDF)")
            entradas_missoes_pdf, saidas_missoes_pdf = _collect_missions_data(db, start_lanc, end_lanc)
            st.download_button(
                "⬇️ Baixar PDF de Lançamentos de Missões",
                data=build_missions_report_pdf(ref_lanc, entradas_missoes_pdf, saidas_missoes_pdf),
                file_name=f"lancamentos_missoes_{start_lanc.strftime('%Y-%m')}.pdf",
                mime="application/pdf"
            )

        with tab2:
            st.subheader("Análise de Contribuições de Missões")
            
            c1, c2 = st.columns(2)
            with c1:
                ano_pesq = st.number_input("Ano da Pesquisa", value=today_bahia().year, step=1, format="%d", key="missions_search_year")
            with c2:
                mes_opt = ["Todos"] + MONTHS
                mes_sel = st.selectbox("Mês da Pesquisa", mes_opt, index=0, key="missions_search_month")

            df_search, total_periodo, num_congs, df_top_period, df_top_year = _build_missions_search_df(db, ano_pesq, mes_sel)

            st.divider()
            
            st.markdown("##### Destaques do Período Selecionado")
            c1, c2 = st.columns(2)
            c1.metric("Total de Entradas no Período", format_currency(total_periodo))
            c2.metric("Nº de Congregações Contribuintes", f"{num_congs}")
            
            st.markdown("---")
            
            col_top1, col_top2 = st.columns(2)
            with col_top1:
                st.markdown(f"**Top 5 Contribuintes ({mes_sel if mes_sel != 'Todos' else 'Período'})**")
                # ===== CORREÇÃO AQUI =====
                if not df_top_period.empty:
                    st.dataframe(
                        df_top_period[['Congregação', 'Total no Período (R$)']].style.format({"Total no Período (R$)": format_currency}),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.caption("Nenhum contribuinte no período.")
            
            with col_top2:
                st.markdown(f"**Top 5 Contribuintes (Ano de {ano_pesq})**")
                # ===== CORREÇÃO AQUI =====
                if not df_top_year.empty:
                    st.dataframe(
                        df_top_year[['Congregação', 'Total no Ano (R$)']].style.format({"Total no Ano (R$)": format_currency}),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.caption("Nenhum contribuinte no ano.")

            st.divider()
            st.markdown("###### Tabela Geral de Contribuições")
            if not df_search.empty:
                st.dataframe(
                    df_search.style.format({"Total no Período (R$)": format_currency, "Total no Ano (R$)": format_currency}),
                    use_container_width=True, hide_index=True
                )
            else:
                st.caption("Nenhuma contribuição de missões encontrada para os filtros selecionados.")


def page_relatorio_missoes_congregacao(user: "User"):
    """
    Relatório de Missões para login de congregação (TESOUREIRO):
    - Adiciona tabela EDITÁVEL de ENTRADAS de Missões por culto (Data do Culto, Oferta de Missões).
    - Mantém o restante das funcionalidades do app inalteradas.
    """
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Relatório de Missões</h1>", unsafe_allow_html=True)

        # Seleção de mês
        ref = get_month_selector("Mês de referência")
        start, end = month_bounds(ref)

        # Congregação do usuário
        congs_user = cong_options_for(user, db)
        parent_cong_obj = congs_user[0] if congs_user else None
        if not parent_cong_obj:
            st.error("Nenhuma congregação vinculada ao usuário.")
            return

        # Seleciona unidade (Principal ou Sub)
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id)).all()
        contexto = f"{parent_cong_obj.name} (Principal)"
        target_sub_cong_id = None
        if sub_congs:
            opcoes = {f"{parent_cong_obj.name} (Principal)": None}
            for sub in sub_congs:
                opcoes[sub.name] = sub.id
            contexto = st.selectbox("Unidade", list(opcoes.keys()), key="missoes_unidade_cong")
            target_sub_cong_id = opcoes[contexto]

        st.info(f"Unidade selecionada: **{contexto}**")

        # ===== NOVO: Tabela editável de ENTRADAS de Missões (por culto) =====
        _editor_missions_entries_unit(
            cong_id=parent_cong_obj.id,
            sub_cong_id=target_sub_cong_id,
            start=start, end=end,
            titulo=f"Entradas de Missões — {ref.strftime('%B/%Y')}"
        )

        # (Se você tinha outras seções específicas aqui, mantenha abaixo sem alterações.)
        # Ex.: visualizações, exportações, etc.


# ===================== PAGE: CADASTRO =====================
# ===================== PAGE: CADASTRO =====================
# ===================== PAGE: CADASTRO =====================
# ===================== PAGE: CADASTRO =====================
# ===================== PAGE: CADASTRO =====================
# ===================== PAGE: CADASTRO =====================
# ===================== PAGE: CADASTRO =====================
# ===================== PAGE: CADASTRO =====================
def page_cadastro(user: "User"):
    if not is_admin_general(user):
        st.warning("🔒 Apenas o **administrador geral** (admin) pode acessar o Cadastro.")
        return
        
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)

        tabs = st.tabs(["Congregações", "Sub-congregações", "Categorias", "Usuários"])

        # Aba de Congregações
        with tabs[0]:
            st.subheader("Congregações")
            col_single, col_mass = st.columns(2)
            with col_single:
                new_cong = st.text_input("Nova congregação (individual)", key="cad_new_cong")
                if st.button("Adicionar congregação", disabled=not new_cong.strip(), key="cad_add_cong"):
                    if db.scalar(select(Congregation).where(func.lower(Congregation.name) == new_cong.strip().lower())):
                        st.error("Já existe congregação com esse nome.")
                    else:
                        db.add(Congregation(name=new_cong.strip())); db.commit()
                        st.success("Congregação adicionada."); st.rerun()
            with col_mass:
                mass_text = st.text_area("Adicionar em massa (uma por linha)", height=140, key="cad_mass_cong")
                if st.button("Adicionar lista de congregações", key="cad_add_cong_mass"):
                    linhas = [l.strip() for l in (mass_text or "").splitlines() if l.strip()]
                    if linhas:
                        inseridas, repetidas = 0, 0
                        existentes = {c.name.lower() for c in db.scalars(select(Congregation))}
                        for nome in linhas:
                            if nome.lower() in existentes: repetidas += 1
                            else: db.add(Congregation(name=nome)); inseridas += 1
                        db.commit()
                        st.success(f"Inseridas: {inseridas} | Já existiam: {repetidas}")
                        st.rerun()

            st.divider()
            congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()
            if congs_all:
                st.markdown("##### Congregações existentes")
                dfc = pd.DataFrame([{"ID": c.id, "Nome": c.name} for c in congs_all])
                st.dataframe(dfc, use_container_width=True, hide_index=True)

                with st.expander("Excluir congregações"):
                    users_by_cong = {cid for cid, count in db.execute(select(User.congregation_id, func.count(User.id)).where(User.congregation_id.isnot(None)).group_by(User.congregation_id)).all() if count > 0}
                    tx_by_cong = {cid for cid, count in db.execute(select(Transaction.congregation_id, func.count(Transaction.id)).group_by(Transaction.congregation_id)).all() if count > 0}
                    tithes_by_cong = {cid for cid, count in db.execute(select(Tithe.congregation_id, func.count(Tithe.id)).group_by(Tithe.congregation_id)).all() if count > 0}
                    subs_by_cong = {cid for cid, count in db.execute(select(SubCongregation.congregation_id, func.count(SubCongregation.id)).group_by(SubCongregation.congregation_id)).all() if count > 0}
                    
                    ids_em_uso = users_by_cong.union(tx_by_cong).union(tithes_by_cong).union(subs_by_cong)
                    eligible_congs = [c for c in congs_all if c.id not in ids_em_uso and _norm(c.name) != "sede"]
                    
                    if not eligible_congs:
                        st.info("Nenhuma congregação pode ser excluída, pois todas possuem dados ou sub-congregações vinculadas.")
                    else:
                        names_del = st.multiselect("Selecione as congregações para excluir:", [c.name for c in eligible_congs], key="cad_del_cong_ids")
                        if st.button("Confirmar exclusão de congregações", disabled=not names_del):
                            ids_to_delete_final = [c.id for c in eligible_congs if c.name in names_del]
                            db.query(Congregation).filter(Congregation.id.in_(ids_to_delete_final)).delete(synchronize_session=False)
                            db.commit(); st.success("Congregações excluídas."); st.rerun()

        # Aba de Sub-congregações
        with tabs[1]:
            st.subheader("Sub-congregações")
            congs_all_subs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
            if not congs_all_subs:
                st.warning("Cadastre uma Congregação principal primeiro.")
            else:
                c1, c2 = st.columns(2)
                with c1:
                    cong_mae_nome = st.selectbox("Selecione a Congregação 'mãe'", [c.name for c in congs_all_subs], key="cad_sub_cong_mae_sel")
                with c2:
                    new_sub_cong_name = st.text_input("Nome da nova Sub-congregação", key="cad_new_sub_cong")

                if st.button("Adicionar Sub-congregação", key="cad_add_sub_cong"):
                    cong_mae_obj = next((c for c in congs_all_subs if c.name == cong_mae_nome), None)
                    nome_valido = new_sub_cong_name.strip()
                    if cong_mae_obj and nome_valido:
                        existe = db.scalar(select(SubCongregation).where(SubCongregation.name == nome_valido, SubCongregation.congregation_id == cong_mae_obj.id))
                        if existe:
                            st.error(f"A sub-congregação '{nome_valido}' já existe em '{cong_mae_obj.name}'.")
                        else:
                            db.add(SubCongregation(name=nome_valido, congregation_id=cong_mae_obj.id))
                            db.commit()
                            st.success(f"Sub-congregação '{nome_valido}' adicionada a '{cong_mae_obj.name}'.")
                            st.rerun()

            st.divider()
            subs = db.scalars(select(SubCongregation).options(joinedload(SubCongregation.congregation)).order_by(SubCongregation.name)).all()
            if subs:
                st.markdown("##### Sub-congregações existentes")
                df_subs = pd.DataFrame([{"ID": s.id, "Nome": s.name, "Congregação Mãe": s.congregation.name} for s in subs])
                st.dataframe(df_subs, use_container_width=True, hide_index=True)

                with st.expander("Excluir sub-congregações"):
                    tx_by_sub = {sid for sid, count in db.execute(select(Transaction.sub_congregation_id, func.count(Transaction.id)).where(Transaction.sub_congregation_id.isnot(None)).group_by(Transaction.sub_congregation_id)).all() if count > 0}
                    tithes_by_sub = {sid for sid, count in db.execute(select(Tithe.sub_congregation_id, func.count(Tithe.id)).where(Tithe.sub_congregation_id.isnot(None)).group_by(Tithe.sub_congregation_id)).all() if count > 0}
                    subs_in_use_ids = tx_by_sub.union(tithes_by_sub)
                    eligible_subs = [s for s in subs if s.id not in subs_in_use_ids]
                    if not eligible_subs:
                        st.info("Nenhuma sub-congregação pode ser excluída, pois todas possuem dados vinculados.")
                    else:
                        names_del = st.multiselect("Selecione as sub-congregações para excluir:", [s.name for s in eligible_subs], key="cad_del_sub_ids")
                        if st.button("Confirmar exclusão de sub-congregações", disabled=not names_del):
                            ids_to_delete_final = [s.id for s in eligible_subs if s.name in names_del]
                            db.query(SubCongregation).filter(SubCongregation.id.in_(ids_to_delete_final)).delete(synchronize_session=False)
                            db.commit(); st.success("Sub-congregações excluídas."); st.rerun()
                            
        # Aba de Categorias
        with tabs[2]:
            st.subheader("Categorias")
            col1_cat, col2_cat = st.columns(2)
            with col1_cat:
                cat_name = st.text_input("Nome da categoria", key="cad_cat_name")
            with col2_cat:
                cat_type = st.selectbox("Tipo", ["DOAÇÃO", "SAÍDA"], key="cad_cat_type")
            if st.button("Adicionar categoria", disabled=not cat_name.strip(), key="cad_add_cat"):
                if db.scalar(select(Category).where(func.lower(Category.name) == cat_name.strip().lower())):
                    st.error("Já existe categoria com esse nome.")
                else:
                    db.add(Category(name=cat_name.strip(), type=cat_type)); db.commit()
                    st.success("Categoria adicionada."); st.rerun()
            
            st.divider()
            cats = db.scalars(select(Category).order_by(Category.type, Category.name)).all()
            if cats:
                st.markdown("##### Categorias existentes")
                usage = {cid for cid, count in db.execute(select(Transaction.category_id, func.count(Transaction.id)).group_by(Transaction.category_id)).all() if count > 0}
                dfcat = pd.DataFrame([{"ID": c.id, "Nome": c.name, "Tipo": c.type, "Em Uso": "Sim" if c.id in usage else "Não"} for c in cats])
                st.dataframe(dfcat, use_container_width=True, hide_index=True)
                with st.expander("Excluir categorias"):
                    eligible_cats = [c for c in cats if c.id not in usage]
                    if not eligible_cats:
                        st.info("Nenhuma categoria pode ser excluída, pois todas estão em uso.")
                    else:
                        names_del = st.multiselect("Selecione as categorias para excluir:", [c.name for c in eligible_cats], key="cad_del_cat_ids")
                        if st.button("Confirmar exclusão de categorias", disabled=not names_del):
                            ids_to_delete_final = [c.id for c in eligible_cats if c.name in names_del]
                            db.query(Category).filter(Category.id.in_(ids_to_delete_final)).delete(synchronize_session=False)
                            db.commit(); st.success("Categorias excluídas."); st.rerun()

        # Aba de Usuários
        with tabs[3]:
            st.subheader("Usuários")
            u_user = st.text_input("Usuário (login)", key="cad_user_login")
            u_pwd = st.text_input("Senha", type="password", key="cad_user_pwd")
            u_role = st.selectbox("Perfil", ["SEDE", "TESOUREIRO", "TESOUREIRO MISSIONÁRIO"], key="cad_user_role")
            
            all_congs_users = db.scalars(select(Congregation).order_by(Congregation.name)).all()
            cong_options = ["—"] + [c.name for c in all_congs_users]
            u_cong_name = st.selectbox("Vincular à Congregação", cong_options, key="cad_user_cong")

            if st.button("Criar usuário", key="cad_user_add"):
                username_stripped = u_user.strip()
                user_exists = db.scalar(select(User).where(User.username == username_stripped))
                if not username_stripped or not u_pwd.strip():
                    st.error("Usuário e senha são obrigatórios.")
                elif user_exists:
                    st.error(f"O nome de usuário '{username_stripped}' já está em uso.")
                elif u_role == "TESOUREIRO" and u_cong_name == "—":
                    st.error("Selecione uma congregação para o perfil TESOUREIRO.")
                else:
                    cong_id = next((c.id for c in all_congs_users if c.name == u_cong_name), None) if u_cong_name != "—" else None
                    db.add(User(username=username_stripped, password_hash=hash_password(u_pwd.strip()), role=u_role, congregation_id=cong_id))
                    db.commit(); st.success("Usuário criado com sucesso!"); st.rerun()
            
            st.divider()
            users_list = db.scalars(select(User).options(joinedload(User.congregation)).order_by(User.username)).all()
            if users_list:
                st.markdown("##### Usuários existentes")
                dfu = pd.DataFrame([{"ID": u.id, "Usuário": u.username, "Perfil": u.role, "Congregação": u.congregation.name if u.congregation else "—"} for u in users_list])
                st.dataframe(dfu, use_container_width=True, hide_index=True)
                with st.expander("Excluir usuários"):
                    eligible_users = [u for u in users_list if u.id != user.id]
                    names_del = st.multiselect("Selecione os usuários para excluir:", [u.username for u in eligible_users], key="cad_del_users_ids")
                    if st.button("Confirmar exclusão de usuários", disabled=not names_del):
                        ids_to_delete_final = [u.id for u in eligible_users if u.username in names_del]
                        db.query(User).filter(User.id.in_(ids_to_delete_final)).delete(synchronize_session=False)
                        db.commit(); st.success("Usuários excluídos."); st.rerun()
                # ---- FIM DA VALIDAÇÃO ----
            # ... (seu código de usuários aqui, que deve estar funcionando) ...
            # ... (seu código de usuários aqui) ...
# ===================== PAGE: LANÇAMENTOS =====================

def display_entry_hierarchy(user: User, congs_all: List[Congregation], start: date, end: date, db: Session):
    st.info("Visualização hierárquica de todas as entradas (exceto Missões).")
    
    report_data = []
    for cong in congs_all:
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == cong.id).order_by(SubCongregation.name)).all()
        
        principal_totals = _collect_month_data(db, cong.id, start, end, sub_cong_id=None)["totals"]
        principal_entradas = principal_totals["entradas_total_sem_missoes"]
        
        # Adiciona a linha da congregação principal
        report_data.append({
            "Unidade": f"{cong.name} (Principal)", "Entradas": principal_entradas,
            "cong_id": cong.id, "sub_id": None
        })
        
        for sub in sub_congs:
            sub_totals = _collect_month_data(db, cong.id, start, end, sub_cong_id=sub.id)["totals"]
            sub_entradas = sub_totals["entradas_total_sem_missoes"]
            report_data.append({
                "Unidade": f"↳ {sub.name}", "Entradas": sub_entradas,
                "cong_id": cong.id, "sub_id": sub.id
            })

    if not report_data:
        st.warning("Nenhum dado de entrada encontrado para o período."); return

    df_report = pd.DataFrame(report_data)
    
    # Se for SEDE, mostra editor. Senão, mostra tabela normal.
    if user.role == "SEDE":
        st.warning("✏️ Modo de edição para SEDE ativado. As alterações aqui criarão lançamentos de ajuste.")
        
        df_editor_view = df_report[["Unidade", "Entradas"]].copy()

        edited_df = st.data_editor(
            df_editor_view,
            use_container_width=True,
            hide_index=True,
            key="hierarchical_entry_editor",
            column_config={
                "Unidade": st.column_config.TextColumn("Unidade", disabled=True),
                "Entradas": st.column_config.NumberColumn("Entradas (R$)", format="R$ %.2f", min_value=0.0)
            }
        )

        def _save_changes():
            # Mescla os dados originais (com IDs) com os dados editados
            merged_df = pd.merge(df_report, edited_df, on="Unidade", suffixes=('_orig', '_new'))
            
            with SessionLocal() as db_session:
                cat_oferta = db_session.scalar(select(Category).where(func.lower(Category.name) == "oferta"))
                if not cat_oferta:
                    st.error("Categoria 'Oferta' não encontrada, necessária para salvar ajustes.")
                    return

                for _, row in merged_df.iterrows():
                    valor_original = float(row['Entradas_orig'])
                    valor_novo = float(row['Entradas_new'])
                    
                    if abs(valor_original - valor_novo) < 0.01:
                        continue # Pula se não houver mudança

                    ajuste_necessario = valor_novo - valor_original
                    cong_id, sub_id = row['cong_id'], row['sub_id']
                    
                    tx_sub_filter = Transaction.sub_congregation_id.is_(None) if sub_id is None else Transaction.sub_congregation_id == sub_id
                    
                    q_adj = select(Transaction).where(
                        Transaction.congregation_id == cong_id, tx_sub_filter,
                        Transaction.date == start, Transaction.description == ADJ_HIER_ENTRY_DESC
                    )
                    adj_existente = db_session.scalar(q_adj)

                    if adj_existente:
                        novo_valor = adj_existente.amount + ajuste_necessario
                        if abs(novo_valor) < 0.01:
                            db_session.delete(adj_existente)
                        else:
                            adj_existente.amount = novo_valor
                    else:
                        db_session.add(Transaction(
                            date=start, type=TYPE_IN, category_id=cat_oferta.id,
                            amount=ajuste_necessario, description=ADJ_HIER_ENTRY_DESC,
                            congregation_id=cong_id, sub_congregation_id=sub_id
                        ))
                
                db_session.commit()
                st.toast("Ajustes de entrada salvos com sucesso!", icon="✅")
                st.rerun()

        _save_btn(_save_changes, "save_hier_entry", theme="entrada")

    else: # Visualização para outros usuários
        st.dataframe(
            df_report[["Unidade", "Entradas"]].style.format({"Entradas": format_currency}),
            use_container_width=True, hide_index=True
        )

    grand_total = df_report["Entradas"].sum()
    st.metric("Total Geral de Entradas (todas as unidades)", format_currency(grand_total))

def display_exit_hierarchy(user: User, congs_all: List[Congregation], start: date, end: date, db: Session):
    st.info("Visualização hierárquica de todas as saídas.")
    
    report_data = []
    for cong in congs_all:
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == cong.id).order_by(SubCongregation.name)).all()
        
        principal_totals = _collect_month_data(db, cong.id, start, end, sub_cong_id=None)["totals"]
        principal_saidas = principal_totals["saidas_total"]
        
        report_data.append({
            "Unidade": f"{cong.name} (Principal)", "Saídas": principal_saidas,
            "cong_id": cong.id, "sub_id": None
        })
        
        for sub in sub_congs:
            sub_totals = _collect_month_data(db, cong.id, start, end, sub_cong_id=sub.id)["totals"]
            sub_saidas = sub_totals["saidas_total"]
            report_data.append({
                "Unidade": f"↳ {sub.name}", "Saídas": sub_saidas,
                "cong_id": cong.id, "sub_id": sub.id
            })

    if not report_data:
        st.warning("Nenhum dado de saída encontrado para o período."); return

    df_report = pd.DataFrame(report_data)

    if user.role == "SEDE":
        st.warning("✏️ Modo de edição para SEDE ativado. As alterações aqui criarão lançamentos de ajuste.")

        df_editor_view = df_report[["Unidade", "Saídas"]].copy()
        
        edited_df = st.data_editor(
            df_editor_view,
            use_container_width=True, hide_index=True,
            key="hierarchical_exit_editor",
            column_config={
                "Unidade": st.column_config.TextColumn("Unidade", disabled=True),
                "Saídas": st.column_config.NumberColumn("Saídas (R$)", format="R$ %.2f", min_value=0.0)
            }
        )

        def _save_changes():
            merged_df = pd.merge(df_report, edited_df, on="Unidade", suffixes=('_orig', '_new'))
            
            with SessionLocal() as db_session:
                cat_out_default = db_session.scalars(select(Category).where(Category.type == TYPE_OUT)).first()
                if not cat_out_default:
                    st.error("Nenhuma categoria de SAÍDA encontrada, necessária para salvar ajustes.")
                    return

                for _, row in merged_df.iterrows():
                    valor_original = float(row['Saídas_orig'])
                    valor_novo = float(row['Saídas_new'])
                    
                    if abs(valor_original - valor_novo) < 0.01:
                        continue

                    ajuste_necessario = valor_novo - valor_original
                    cong_id, sub_id = row['cong_id'], row['sub_id']
                    
                    tx_sub_filter = Transaction.sub_congregation_id.is_(None) if sub_id is None else Transaction.sub_congregation_id == sub_id
                    
                    q_adj = select(Transaction).where(
                        Transaction.congregation_id == cong_id, tx_sub_filter,
                        Transaction.date == start, Transaction.description == ADJ_HIER_OUT_DESC
                    )
                    adj_existente = db_session.scalar(q_adj)

                    if adj_existente:
                        novo_valor = adj_existente.amount + ajuste_necessario
                        if abs(novo_valor) < 0.01:
                            db_session.delete(adj_existente)
                        else:
                            adj_existente.amount = novo_valor
                    else:
                        db_session.add(Transaction(
                            date=start, type=TYPE_OUT, category_id=cat_out_default.id,
                            amount=ajuste_necessario, description=ADJ_HIER_OUT_DESC,
                            congregation_id=cong_id, sub_congregation_id=sub_id
                        ))
                
                db_session.commit()
                st.toast("Ajustes de saída salvos com sucesso!", icon="✅")
                st.rerun()

        _save_btn(_save_changes, "save_hier_exit", theme="saida")

    else: # Visualização para outros usuários
        st.dataframe(
            df_report[["Unidade", "Saídas"]].style.format({"Saídas": format_currency}),
            use_container_width=True, hide_index=True
        )

    grand_total = df_report["Saídas"].sum()
    st.metric("Total Geral de Saídas (todas as congregações)", format_currency(grand_total))
                   
def _build_entry_report_df(db: Session, cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None) -> pd.DataFrame:
    """
    Constrói o DataFrame para o Relatório de Entrada a partir dos ServiceLog.
    Agrupa os múltiplos cultos de um mesmo dia numa única linha.
    """
    log_filter = and_(
        ServiceLog.congregation_id == cong_id,
        ServiceLog.date >= start,
        ServiceLog.date < end,
        ServiceLog.sub_congregation_id == sub_cong_id
    )

    # Agrupa por data e soma os dízimos e ofertas de todos os cultos do dia
    query = select(
        ServiceLog.date,
        func.sum(ServiceLog.dizimo),
        func.sum(ServiceLog.oferta)
    ).where(log_filter).group_by(ServiceLog.date).order_by(ServiceLog.date)

    results = db.execute(query).all()

    if not results:
        return pd.DataFrame(columns=["Data do Culto", "Dízimo", "Oferta", "Total"])

    data = []
    for log_date, total_dizimo, total_oferta in results:
        total_dia = (total_dizimo or 0.0) + (total_oferta or 0.0)
        data.append({
            "Data do Culto": log_date,
            "Dízimo": total_dizimo or 0.0,
            "Oferta": total_oferta or 0.0,
            "Total": total_dia
        })
    
    return pd.DataFrame(data)

def display_entry_hierarchy(user: "User", congs_all: List[Congregation], start: date, end: date, db: Session):
    st.info("Visualização hierárquica de todas as entradas, com permissão de ajuste para a Sede.")
    
    report_data = []
    # Itera sobre todas as congregações para construir a estrutura de dados
    for cong in congs_all:
        # Busca dados da congregação principal
        principal_df = _load_service_logs(db, cong.id, start, end, sub_cong_id=None)
        principal_entradas = principal_df['Total'].sum() if not principal_df.empty else 0.0
        report_data.append({
            "Unidade": f"{cong.name} (Principal)", "Entradas": principal_entradas,
            "cong_id": cong.id, "sub_id": None
        })
        
        # Busca dados das sub-congregações
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == cong.id).order_by(SubCongregation.name)).all()
        for sub in sub_congs:
            sub_df = _load_service_logs(db, cong.id, start, end, sub_cong_id=sub.id)
            sub_entradas = sub_df['Total'].sum() if not sub_df.empty else 0.0
            report_data.append({
                "Unidade": f"↳ {sub.name}", "Entradas": sub_entradas,
                "cong_id": cong.id, "sub_id": sub.id
            })

    if not report_data:
        st.warning("Nenhum dado de entrada encontrado para o período."); return

    df_report = pd.DataFrame(report_data)
    
    if user.role == "SEDE":
        st.warning("✏️ Modo de edição para SEDE ativado. As alterações aqui criarão lançamentos de ajuste na categoria 'Oferta'.")
        
        df_editor_view = df_report[["Unidade", "Entradas"]].copy()

        edited_df = st.data_editor(
            df_editor_view,
            use_container_width=True,
            hide_index=True,
            key="hierarchical_entry_editor",
            column_config={
                "Unidade": st.column_config.TextColumn("Unidade", disabled=True),
                "Entradas": st.column_config.NumberColumn("Entradas (R$)", format="R$ %.2f", min_value=0.0)
            }
        )

        def _save_changes():
            # Mescla os dados originais (com IDs) com os dados editados
            merged_df = pd.merge(df_report, edited_df, on="Unidade", suffixes=('_orig', '_new'))
            
            with SessionLocal() as db_session:
                cat_oferta = db_session.scalar(select(Category).where(func.lower(Category.name) == "oferta"))
                if not cat_oferta:
                    st.error("Categoria 'Oferta' não encontrada, necessária para salvar ajustes.")
                    return

                for _, row in merged_df.iterrows():
                    valor_original = float(row['Entradas_orig'])
                    valor_novo = _to_float_brl(row['Entradas_new'])
                    
                    if abs(valor_original - valor_novo) < 0.01:
                        continue # Pula se não houver mudança

                    ajuste_necessario = valor_novo - valor_original
                    cong_id, sub_id = row['cong_id'], row['sub_id']
                    
                    tx_sub_filter = Transaction.sub_congregation_id.is_(None) if sub_id is None else Transaction.sub_congregation_id == sub_id
                    
                    q_adj = select(Transaction).where(
                        Transaction.congregation_id == cong_id, tx_sub_filter,
                        Transaction.date == start, Transaction.description == ADJ_HIER_ENTRY_DESC
                    )
                    adj_existente = db_session.scalar(q_adj)

                    if adj_existente:
                        novo_valor = adj_existente.amount + ajuste_necessario
                        if abs(novo_valor) < 0.01:
                            db_session.delete(adj_existente)
                        else:
                            adj_existente.amount = novo_valor
                    else:
                        db_session.add(Transaction(
                            date=start, type="DOAÇÃO", category_id=cat_oferta.id,
                            amount=ajuste_necessario, description=ADJ_HIER_ENTRY_DESC,
                            congregation_id=cong_id, sub_congregation_id=sub_id
                        ))
                
                db_session.commit()
                st.toast("Ajustes de entrada salvos com sucesso!", icon="✅")
                st.rerun()

        st.button("Salvar Ajustes no Relatório Hierárquico", on_click=_save_changes, key="save_hier_entry", type="primary")

    else: # Visualização para outros usuários
        st.dataframe(
            df_report[["Unidade", "Entradas"]].style.format({"Entradas": format_currency}),
            use_container_width=True, hide_index=True
        )

    grand_total = df_report["Entradas"].sum()
    st.metric("Total Geral de Entradas (todas as unidades)", format_currency(grand_total))

# ===================== PAGE: RELATÓRIO DE ENTRADA =====================
def page_relatorio_entrada(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Relatório de Entrada</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)

        parent_cong_obj = None
        
        if user.role == "SEDE":
            congs_all = order_congs_sede_first(cong_options_for(user, db))
            escopo_opts = ["-- Relatório Hierárquico (Edição) --"] + [c.name for c in congs_all]
            
            escopo_selecionado = st.selectbox("Selecione o escopo do relatório:", escopo_opts, key="re_sede_escopo")
            
            if escopo_selecionado == "-- Relatório Hierárquico (Edição) --":
                display_entry_hierarchy(user, congs_all, start, end, db)
                return
            else:
                parent_cong_obj = next((c for c in congs_all if c.name == escopo_selecionado), None)
        else: # TESOUREIRO
            parent_cong_obj = db.get(Congregation, user.congregation_id)

        if not parent_cong_obj:
            st.info("Nenhuma congregação para analisar."); return

        st.divider()
        st.markdown(f"### Detalhes de: {parent_cong_obj.name.upper()}")

        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id).order_by(SubCongregation.name)).all()
        
        target_sub_cong_id_or_all = None
        contexto_selecionado = parent_cong_obj.name
        
        if sub_congs:
            opcoes = {"-- Todas (Principal + Subs) --": "ALL", f"{parent_cong_obj.name} (Principal)": None}
            for sub in sub_congs:
                opcoes[sub.name] = sub.id
            contexto_selecionado = st.selectbox("Filtrar por unidade:", list(opcoes.keys()), key="re_sub_sel_unified")
            target_sub_cong_id_or_all = opcoes[contexto_selecionado]
        
        st.info(f"Exibindo dados para: **{contexto_selecionado}**")

        if target_sub_cong_id_or_all == "ALL":
            all_units_data = []
            df_principal = _load_service_logs(db, parent_cong_obj.id, start, end, sub_cong_id=None)
            all_units_data.append({"Unidade": f"{parent_cong_obj.name} (Principal)", "Total Entradas": df_principal['Total'].sum()})
            for sub in sub_congs:
                df_sub = _load_service_logs(db, parent_cong_obj.id, start, end, sub_cong_id=sub.id)
                all_units_data.append({"Unidade": f"↳ {sub.name}", "Total Entradas": df_sub['Total'].sum()})
            
            df_agg = pd.DataFrame(all_units_data)
            st.dataframe(df_agg.style.format({"Total Entradas": format_currency}), use_container_width=True, hide_index=True)
            total_geral = df_agg["Total Entradas"].sum()
            st.metric("Total Geral da Congregação", format_currency(total_geral))
        else:
            report_df = _load_service_logs(db, parent_cong_obj.id, start, end, sub_cong_id=target_sub_cong_id_or_all)
            
            st.dataframe(
                report_df.style.format({
                    "Data do Culto": "{:%d/%m/%Y}", "Dízimo": format_currency,
                    "Oferta": format_currency, "Total": format_currency
                }),
                use_container_width=True, hide_index=True,
                column_order=["Data do Culto", "Tipo de Culto", "Dízimo", "Oferta", "Total"]
            )
            
            # ===== NOVO BLOCO DE MÉTRICAS PARA TESOUREIRO =====
            st.divider()
            try:
                total_dizimo, total_oferta, total_geral = 0.0, 0.0, 0.0
                if not report_df.empty:
                    total_dizimo = report_df["Dízimo"].sum()
                    total_oferta = report_df["Oferta"].sum()
                    total_geral = report_df["Total"].sum()
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total de Dízimos", format_currency(total_dizimo))
                col2.metric("Total de Ofertas", format_currency(total_oferta))
                col3.metric("Total Geral Entradas", format_currency(total_geral))
            except Exception:
                st.caption("Calculando totais...")
            
            # REMOVIDO: Botão de salvar e toda a sua lógica
# ===================== MAIN =====================
def main():
    try:
        ensure_seed()

        # Tenta carregar o usuário a partir da sessão ou dos cookies
        user = current_user()
        if not user:
            try:
                cm = get_cookie_manager()
                tok = cm.get(COOKIE_NAME)
                data = _read_token(tok)
                if data:
                    with SessionLocal() as db:
                        u = db.get(User, int(data["uid"]))
                        if u:
                            st.session_state.uid = u.id
                            st.rerun()
            except Exception:
                # Ignora erros do cookie manager se ele não estiver instalado
                pass

        # Estrutura Lógica Principal: OU mostra o login, OU mostra o app.
        if 'uid' not in st.session_state or not st.session_state.uid:
            # ESTADO DESLOGADO: Mostra apenas a UI de login
            login_ui()
        else:
            # ESTADO LOGADO: Carrega o usuário e mostra a interface principal
            user = current_user()
            if user:
                page = sidebar_common(user)

                # Roteamento de páginas
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
                    page_visao_geral(user)
            else:
                # Caso raro: UID na sessão mas usuário não encontrado no DB. Força logout.
                logout()

    except Exception as e:
        st.error("Ocorreu um erro crítico na aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
