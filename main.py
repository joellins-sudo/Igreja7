# main.py — Igreja Finance CHMS — v8.61 (login persiste após refresh com handshake de cookies)

from __future__ import annotations
import os
import datetime as dt
from datetime import date
from typing import Optional, List, Tuple
from collections import defaultdict
import pandas as pd
import streamlit as st
from sqlalchemy import select, func, String, Date, Float, ForeignKey, create_engine
from sqlalchemy.orm import relationship, Mapped, mapped_column, sessionmaker, joinedload, Session
from sqlalchemy.ext.declarative import declarative_base
import unicodedata as ud
import hashlib
from io import BytesIO
from reportlab.lib.pagesizes import A4, portrait
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER
from st_aggrid import AgGrid, GridOptionsBuilder
import extra_streamlit_components as stx  # cookies

# ===================== CONFIG BÁSICA =====================
st.set_page_config(page_title="Igreja Finance CHMS", page_icon="⛪", layout="wide")
ADMIN_USERNAME = "admin"

# ===================== COOKIE MANAGER (deve estar no escopo global) =====================
cookie_manager = stx.CookieManager(key="auth_cookies")

# >>> FIX: nomes de cookies
COOKIE_UID = "uid"
COOKIE_VH  = "vh"  # verificador derivado do hash de senha

# ===================== MODELOS / DB =====================
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)  # 'SEDE', 'TESOUREIRO', 'TESOUREIRO MISSIONÁRIO'
    congregation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("congregations.id"))

class Congregation(Base):
    __tablename__ = "congregations"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)

class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    type: Mapped[str] = mapped_column(String)  # 'DOAÇÃO' ou 'SAÍDA'

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

class Tithe(Base):
    __tablename__ = "tithes"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    tither_name: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    payment_method: Mapped[Optional[str]] = mapped_column(String, default=None)

@st.cache_resource
def get_engine():
    db_url = st.secrets.get("DATABASE_URL", os.environ.get("DATABASE_URL")) or "sqlite:///database.db"
    return create_engine(db_url, pool_pre_ping=True)

@st.cache_resource
def get_sessionmaker():
    engine = get_engine()
    Base.metadata.create_all(engine)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

SessionLocal = get_sessionmaker()

# ===================== UTIL / AUTH =====================
TYPE_IN = "DOAÇÃO"
TYPE_OUT = "SAÍDA"

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    return "".join(c for c in ud.normalize("NFD", s) if ud.category(c) != "Mn")

def format_currency(v: float) -> str:
    try: v = float(v or 0)
    except: v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def month_bounds(ref: date) -> Tuple[date, date]:
    start = ref.replace(day=1)
    end = date(start.year + (start.month == 12), (start.month % 12) + 1, 1)
    return start, end

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

def _verifier_from_password_hash(pwd_hash: str) -> str:
    return hashlib.sha256(pwd_hash.encode("utf-8")).hexdigest()

# >>> FIX: persistência de login (cookies por 30 dias)
def set_auth_cookies(user: User, days_valid: int = 30):
    expires_at = dt.datetime.utcnow() + dt.timedelta(days=days_valid)
    cookie_manager.set(COOKIE_UID, str(user.id), expires_at=expires_at, key="c_uid_set")
    cookie_manager.set(COOKIE_VH, _verifier_from_password_hash(user.password_hash), expires_at=expires_at, key="c_vh_set")

def clear_auth_cookies():
    cookie_manager.delete(COOKIE_UID, key="c_uid_del")
    cookie_manager.delete(COOKIE_VH, key="c_vh_del")

# >>> FIX: handshake de cookies (1 rerun controlado ao carregar)
def bootstrap_auth_from_cookie():
    """
    1ª execução após refresh: monta o componente, marca flag e rerun.
    2ª execução: cookies já disponíveis; restauramos uid.
    """
    cookies = cookie_manager.get_all() or {}
    if not st.session_state.get("_cookies_handshake_done"):
        st.session_state["_cookies_handshake_done"] = True
        # Monta o componente e garante segunda execução com cookies populados
        st.rerun()

    if st.session_state.get("uid"):
        return

    uid = cookies.get(COOKIE_UID)
    vh  = cookies.get(COOKIE_VH)
    if not uid or not vh:
        return
    try:
        with SessionLocal() as db:
            u = db.get(User, int(uid))
            if u and _verifier_from_password_hash(u.password_hash) == vh:
                st.session_state.uid = u.id
    except Exception:
        clear_auth_cookies()

# ===================== SEED MÍNIMO =====================
def ensure_seed():
    with SessionLocal() as db:
        if db.scalar(select(func.count(Category.id))) == 0:
            for nm, tp in [("Dízimo", TYPE_IN), ("Oferta", TYPE_IN), ("Missões", TYPE_IN),
                           ("Missões (Saída)", TYPE_OUT)]:
                if not db.scalar(select(Category).where(Category.name == nm)):
                    db.add(Category(name=nm, type=tp))
        if db.scalar(select(func.count(Congregation.id))) == 0:
            db.add(Congregation(name="Sede"))
        sede = db.scalar(select(Congregation).where(Congregation.name == "Sede"))
        if db.scalar(select(User).where(User.username == ADMIN_USERNAME)) is None:
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password("123456"),
                        role="SEDE", congregation_id=sede.id))
        db.commit()

# ===================== SESSÃO / LOGIN UI =====================
if "uid" not in st.session_state:
    st.session_state.uid = None

def current_user() -> Optional[User]:
    uid = st.session_state.get("uid")
    if not uid: return None
    with SessionLocal() as db:
        return db.get(User, uid)

def login_ui():
    st.markdown("## Igreja Finance CHMS")
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username == u))
            if user and verify_password(p, user.password_hash):
                st.session_state.uid = user.id
                set_auth_cookies(user, days_valid=30)     # persiste
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

# ===================== PÁGINAS (resumo mínimo só p/ exemplo) =====================
def page_home(user: User):
    with st.sidebar:
        st.write(f"👤 **{user.username}** — *{user.role}*")
        if st.button("Sair"):
            clear_auth_cookies()
            st.session_state.clear()
            st.rerun()
    st.success("Você está autenticado e continuará assim ao atualizar a página. ✅")

# ===================== MAIN =====================
def main():
    ensure_seed()

    # >>> FIX: recupera autenticação dos cookies ANTES de qualquer UI
    bootstrap_auth_from_cookie()

    user = current_user()
    if not user:
        # Se chegou aqui, é porque não havia cookie válido; mostra login.
        login_ui()
        return

    # App logado
    page_home(user)

if __name__ == "__main__":
    main()
