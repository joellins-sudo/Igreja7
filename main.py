# main.py — AD Relatório Financeiro — v8.71 (Mobile scroll fix + sidebar aberta)

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

st.set_page_config(
    page_title="AD Relatório Financeiro",
    page_icon="⛪",
    layout="wide",
    initial_sidebar_state="expanded",     # <<=== mantém a sidebar (abas) aberta no mobile
)

CSS = """
<style>
:root{ --brand:#0f172a; --brand-2:#1d4ed8; --accent:#16a34a; --danger:#dc2626; --muted:#6b7280;
--bg:#f8fafc; --card:#ffffff; --ring:#e5e7eb; }

html, body { 
  background: var(--bg);
  height:auto!important; 
  min-height:100vh!important;
  overflow-y:auto!important; 
  overscroll-behavior-y:contain; 
  -webkit-overflow-scrolling:touch;
}
.stApp{
  height:auto!important; 
  min-height:100vh!important;
  overflow:visible!important; 
  padding-bottom:max(18px, env(safe-area-inset-bottom))!important;
}
section.main>div{overflow:visible!important;}
.block-container{
  min-height:100vh!important;
  padding-top: .9rem !important;
}

/* Esconder flutuantes que travam rolagem no mobile (Manage app, balões etc.) */
@media (pointer:coarse){
  footer,
  [data-testid="stToolbar"],
  [aria-label*="Manage app"],
  [title*="Manage app"],
  /* Qualquer elemento 'fixed' colado no rodapé vira display:none */
  div[style*="position: fixed"][style*="bottom"]{
    display:none!important;
  }
}

/* Cabeçalho e sidebar */
header[data-testid="stHeader"]{ 
  background: linear-gradient(180deg,#ffffff, #f6f9ff) !important; 
  border-bottom:1px solid var(--ring); 
}
[data-testid="stSidebar"]{ 
  background: linear-gradient(180deg,#ffffff,#fbfdff); 
  border-right:1px solid var(--ring); 
}
[data-testid="stSidebar"] img{ 
  border-radius: .6rem; border:1px solid var(--ring); 
  box-shadow: 0 4px 16px rgba(0,0,0,.06); 
}

/* Títulos e cartões */
.page-title{ font-family:'Nunito','Inter',system-ui; font-size:36px; font-weight:900; letter-spacing:.4px; color: var(--brand); margin:.25rem 0 1rem; }
h2, .stMarkdown h2, .st-subheader{ color:#0f172a!important; font-weight:900 !important; }

.st-container-card{ border: 1px solid var(--ring); border-radius: 1rem; padding: 1rem; margin-bottom: 1.15rem; background: var(--card); box-shadow:0 10px 30px rgb(31 58 138 / .06), 0 2px 8px rgb(0 0 0 / .04); position: relative; }
.st-container-card::before{ content:""; position: absolute; left:0; top:0; bottom:0; width:6px; background: linear-gradient(180deg,var(--brand-2), var(--brand)); border-top-left-radius:1rem; border-bottom-left-radius:1rem; }

/* Métricas */
div[data-testid="stMetric"]{ padding: .75rem .9rem; border:1px solid var(--ring); border-radius:.9rem; background:var(--card); box-shadow:0 1px 2px rgb(0 0 0 / .06); }
div[data-testid="stMetricLabel"]{ color:#334155; font-weight:800; }
div[data-testid="stMetricValue"]{ font-size:26px!important; line-height:1.15!important; white-space:nowrap!important; font-weight:900!important; }

.stat-card{ border:1px solid var(--ring); border-radius:.9rem; background:#fff; padding:.9rem 1rem; box-shadow:0 1px 2px rgba(0,0,0,.06); height:86px; display:flex; flex-direction:column; justify-content:center; }
.stat-label{ font-size:.92rem; color:#334155; font-weight:800; margin-bottom:.18rem; }
.stat-value{ font-size:1.25rem; font-weight:900; color:#111827; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.stat-card .tooltip{ display:none!important; } /* remove tooltip (evita duplicação visual) */

/* Inputs e botões */
label, .stTextInput label, .stSelectbox label, .stNumberInput label{ color:#0f172a; font-weight:800; }
.stTextInput input, .stNumberInput input, .stDateInput input{ border:1px solid var(--ring)!important; border-radius:.65rem!important; background:#fff!important; }
.stSelectbox [data-baseweb="select"]>div{ border:1px solid var(--ring)!important; border-radius:.65rem!important; background:#fff!important; }
.stTextInput input:focus, .stNumberInput input:focus, .stDateInput input:focus{ outline:none!important; border-color:#93c5fd!important; box-shadow:0 0 0 4px rgba(37,99,235,.18)!important; }
.stButton>button{ border:1px solid #2563eb!important; color:#fff!important; background: linear-gradient(180deg,#3b82f6,#2563eb)!important; font-weight:900!important; border-radius:.7rem!important; padding:.52rem .95rem!important; box-shadow:0 2px 6px rgba(37,99,235,.25)!important; }
.stButton>button:hover{ filter:brightness(1.03); transform:translateY(-1px); }

/* Tabelas */
[data-testid="stDataFrame"]{ border:1px solid var(--ring); border-radius:.9rem; overflow:hidden; box-shadow:0 1px 2px rgba(0,0,0,.04); }
[data-testid="stDataFrame"] thead tr th{ position:sticky; top:0; z-index:2; background:#f1f5f9!important; font-weight:900!important; color:#0f172a!important; border-bottom:1px solid var(--ring)!important; }
[data-testid="stDataFrame"] tbody tr:nth-child(even) td{ background:#fcfcfd; }
[data-testid="stDataFrame"] tbody tr:hover td{ background:#f8fbff; }
.cong-title{ font-weight:900; font-size:1.05rem; color:#0f172a; margin-bottom:.35rem; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# Script “no-op” para manter compat c/ mobile
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
import hashlib, os

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
# (mesmas funções do envio anterior)
# ... (por brevidade no comentário, mas mantenho iguais às já fornecidas)
# >>> As funções build_dizimista_search_pdf, build_full_statement_pdf, build_consolidated_pdf
#     seguem idênticas às do último envio. (Estão no seu arquivo exatamente como antes.)

# [COLE AQUI as três funções de PDF exatamente iguais às do envio anterior]
# ====== (Para não alongar demais esta mensagem, elas foram mantidas iguais) ======

# Para manter a resposta curta, não repito aqui o miolo das funções PDF
# Use-as exatamente como no arquivo que te enviei na mensagem anterior (v8.70).

# ===================== PÁGINAS =====================
# (todo o restante é idêntico ao envio anterior v8.70)
# >>> page_lancamentos, page_relatorio_entrada, page_relatorio_saida,
#     page_relatorio_dizimistas, page_relatorio_missoes, page_cadastro,
#     page_visao_geral — copie iguais do arquivo anterior (v8.70).

# -------------- IMPORTANTE --------------
# Se você preferir, pode simplesmente manter o arquivo anterior e:
#   1) Substituir APENAS o st.set_page_config(...) no topo
#   2) Substituir o bloco CSS (variável CSS) por este novo
# O restante do código continua idêntico.
# ---------------------------------------

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
            st.button("Sair", on_click=do_logout)

        # roteamento
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
