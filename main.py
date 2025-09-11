# main.py — Igreja Finance CHMS — Persistência de login (URL token + Cookie) — v9

from __future__ import annotations
import os, hashlib, unicodedata as ud
from datetime import date
from typing import Optional, List, Tuple
from collections import defaultdict
from io import BytesIO

import pandas as pd
import streamlit as st
from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
from sqlalchemy.ext.declarative import declarative_base

from reportlab.lib.pagesizes import A4, portrait
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

from st_aggrid import AgGrid, GridOptionsBuilder
import extra_streamlit_components as stx

# ===================== CONFIG BÁSICA =====================
st.set_page_config(page_title="Igreja Finance CHMS", page_icon="⛪", layout="wide")
ADMIN_USERNAME = "admin"
TYPE_IN, TYPE_OUT = "DOAÇÃO", "SAÍDA"

# ===================== ESTILO =====================
st.markdown("""
<style>
:root{ --ring:#e5e7eb; --bg:#f8fafc; --card:#fff; }
.block-container{ padding-top:.9rem!important }
[data-testid="stSidebar"]{ background:linear-gradient(180deg,#fff,#fbfdff); border-right:1px solid var(--ring) }
.page-title{ font:900 36px/1.1 Inter,system-ui; margin:.25rem 0 1rem }
.st-container-card{ border:1px solid var(--ring); border-radius:1rem; padding:1rem; margin-bottom:1rem; background:#fff; box-shadow:0 8px 24px rgba(0,0,0,.05) }
</style>
""", unsafe_allow_html=True)

# ===================== COOKIES + TOKEN NA URL =====================
COOKIE_UID, COOKIE_VH = "uid", "vh"
def _norm(s:str)->str:
    s=(s or "").strip().lower(); s=ud.normalize("NFD",s)
    return "".join(c for c in s if ud.category(c)!="Mn")
def _vh_from_pwdhash(pwd_hash:str)->str:
    return hashlib.sha256(pwd_hash.encode("utf-8")).hexdigest()  # 64 hex

# Renderizamos o componente o mais cedo possível e guardamos no estado
def ensure_cookie_component():
    if "cookie_mgr_placeholder" not in st.session_state:
        st.session_state.cookie_mgr_placeholder = st.empty()
    if "cookie_mgr" not in st.session_state:
        with st.session_state.cookie_mgr_placeholder:
            st.session_state.cookie_mgr = stx.CookieManager(key="auth_cookies")
    return st.session_state.cookie_mgr

def set_auth_cookies(user, days_valid: int = 30):
    cm = ensure_cookie_component()
    import datetime as dt
    exp = dt.datetime.utcnow() + dt.timedelta(days=days_valid)
    cm.set(COOKIE_UID, str(user.id), expires_at=exp, key="uid_set")
    cm.set(COOKIE_VH, _vh_from_pwdhash(user.password_hash)[:24], expires_at=exp, key="vh_set")

def clear_auth_cookies():
    cm = ensure_cookie_component()
    cm.delete(COOKIE_UID, key="uid_del")
    cm.delete(COOKIE_VH, key="vh_del")

def make_url_token(user)->str:
    # token curto: "uid.vh24"
    return f"{user.id}.{_vh_from_pwdhash(user.password_hash)[:24]}"

def read_url_token()->Optional[str]:
    try:
        # 1.49 tem API nova `st.query_params`, mas mantemos compat com experimental
        q = getattr(st, "query_params", None)
        if q is not None:
            return q.get("auth")
        return st.experimental_get_query_params().get("auth", [None])[0]
    except Exception:
        return None

def set_url_token(token: Optional[str]):
    try:
        if getattr(st, "query_params", None) is not None:
            qp = st.query_params  # dict-like
            if token:
                qp.update({"auth": token})
            else:
                # Limpa o parâmetro
                if "auth" in qp: del qp["auth"]
            return
        # compat
        if token:
            st.experimental_set_query_params(auth=token)
        else:
            st.experimental_set_query_params()
    except Exception:
        pass

def bootstrap_auth_from_url_or_cookie(get_user_by_id):
    """
    Ordem:
      1) URL ?auth=... (não depende do componente de cookies)
      2) Cookie (com handshake de 1 rerun)
    """
    # 1) URL
    tok = read_url_token()
    if tok:
        try:
            uid_s, vh_s = tok.split(".", 1)
            u = get_user_by_id(int(uid_s))
            if u and _vh_from_pwdhash(u.password_hash)[:24] == vh_s:
                st.session_state.uid = u.id
                return
        except Exception:
            pass

    # 2) Cookie (precisa de 1 rerun na 1ª carga pós-refresh)
    cm = ensure_cookie_component()
    if not st.session_state.get("_cookie_handshake_done"):
        st.session_state["_cookie_handshake_done"] = True
        st.rerun()

    cookies = cm.get_all() or {}
    uid, vh = cookies.get(COOKIE_UID), cookies.get(COOKIE_VH)
    if uid and vh:
        try:
            u = get_user_by_id(int(uid))
            if u and _vh_from_pwdhash(u.password_hash)[:24] == vh:
                st.session_state.uid = u.id
        except Exception:
            clear_auth_cookies()

# ===================== DB / MODELOS =====================
Base = declarative_base()
class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)
    congregation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("congregations.id"))
    congregation = relationship("Congregation", back_populates="users")

class Congregation(Base):
    __tablename__ = "congregations"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    users = relationship("User", back_populates="congregation")
    transactions = relationship("Transaction", back_populates="congregation")
    tithes = relationship("Tithe", back_populates="congregation")

class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    type: Mapped[str] = mapped_column(String)

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
    category = relationship("Category", lazy="joined")
    congregation = relationship("Congregation", back_populates="transactions")

class Tithe(Base):
    __tablename__ = "tithes"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    tither_name: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)
    congregation = relationship("Congregation", back_populates="tithes")

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

# ===================== SEED / UTILS =====================
def hash_password(password: str) -> str:
    salt = os.urandom(16)
    pwdhash = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return salt.hex() + ":" + pwdhash.hex()

def verify_password(password: str, stored: str) -> bool:
    salt_hex, pwd_hex = stored.split(":")
    salt = bytes.fromhex(salt_hex); want = bytes.fromhex(pwd_hex)
    got = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100000)
    return got == want

CONGREGACOES_PADRAO = ["Sede", "Rodeadouro", "Dr. Humberto"]
def ensure_seed():
    with SessionLocal() as db:
        if db.scalar(select(func.count(Category.id))) == 0:
            for n,t in [("Dízimo",TYPE_IN),("Oferta",TYPE_IN),("Missões",TYPE_IN),
                        ("Aluguel",TYPE_OUT),("Energia",TYPE_OUT),("Missões (Saída)",TYPE_OUT)]:
                db.add(Category(name=n, type=t))
        existentes = set(db.scalars(select(Congregation.name)).all())
        for n in CONGREGACOES_PADRAO:
            if n not in existentes: db.add(Congregation(name=n))
        if db.scalar(select(User).where(User.username==ADMIN_USERNAME)) is None:
            sede_id = db.scalar(select(Congregation.id).where(Congregation.name=="Sede"))
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password("123456"), role="SEDE", congregation_id=sede_id))
        db.commit()

def format_currency(v: float) -> str:
    try: v=float(v or 0.0)
    except: v=0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X",".")

def month_bounds(ref: date)->Tuple[date,date]:
    start = ref.replace(day=1)
    end = date(start.year + (start.month==12), (start.month%12)+1, 1)
    return start, end

# ===================== AUTH/UI =====================
def get_user_by_id(uid:int)->Optional[User]:
    with SessionLocal() as db: return db.get(User, uid)

def current_user()->Optional[User]:
    uid = st.session_state.get("uid")
    return get_user_by_id(uid) if uid else None

def login_ui():
    st.markdown("<h1 class='page-title'>Igreja Finance CHMS</h1>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username==u))
            if user and verify_password(p, user.password_hash):
                st.session_state.uid = user.id
                set_auth_cookies(user, 30)
                set_url_token(make_url_token(user))
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

def sidebar_common(user:User):
    with st.sidebar:
        st.write(f"👤 **{user.username}** — *{user.role}*")
        if st.button("Sair"):
            clear_auth_cookies()
            set_url_token(None)           # remove ?auth=...
            st.session_state.clear()
            st.rerun()

# ===================== PÁGINAS (mesmas do seu app) =====================
def page_lancamentos(user:User):
    sidebar_common(user)
    st.markdown("<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)
    st.info("Página resumida aqui só para exemplo; mantenha seus formulários de entrada/saída/dízimo…")

def page_visao_geral(user:User):
    sidebar_common(user)
    st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
    st.success("Autenticado e persistente. Atualize a página — você continuará logado.")

def page_cadastro(user:User):
    if _norm(user.username)!="admin":
        st.warning("Apenas o admin acessa o Cadastro.")
        return
    sidebar_common(user)
    st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)
    st.caption("… mantenha aqui o conteúdo completo do seu cadastro.")

# ===================== MAIN =====================
def main():
    ensure_seed()

    # 1) Força render do componente de cookie e tenta restaurar auth
    ensure_cookie_component()
    bootstrap_auth_from_url_or_cookie(get_user_by_id)

    user = current_user()
    if not user:
        login_ui(); return

    # Menu
    with st.sidebar:
        if user.role=="SEDE":
            page = st.radio("Menu", ["Visão Geral","Lançamentos","Cadastro"], index=0)
        else:
            page = st.radio("Menu", ["Visão Geral","Lançamentos"], index=0)

    if page=="Lançamentos": page_lancamentos(user)
    elif page=="Cadastro": page_cadastro(user)
    else: page_visao_geral(user)

if __name__ == "__main__":
    main()
