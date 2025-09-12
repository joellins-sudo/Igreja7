# main.py — AD Relatório Financeiro — v9.2
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

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

from io import BytesIO
from reportlab.lib.pagesizes import A4, portrait
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.enums import TA_CENTER

ADMIN_USERNAME = "admin"

st.set_page_config(page_title="AD Relatório Financeiro", page_icon="⛪", layout="wide")
CSS = """<style> ... (seu CSS completo aqui, sem cortes) ... </style>"""
st.markdown(CSS, unsafe_allow_html=True)

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
LOGO_PATH = os.path.join(ASSETS_DIR, "logo.png")

def _set_locale_ptbr():
    for loc in ("pt_BR.utf8", "pt_BR.UTF-8", "pt_BR", "Portuguese_Brazil.1252"):
        try:
            _locale.setlocale(_locale.LC_TIME, loc); return
        except Exception:
            continue
_set_locale_ptbr()

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
    return str(val or "").strip().upper() == "EXCLUIR"

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
    category: Mapped["Category"] = relationship(back_populates="transactions")
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
# ========== SEED E CONFIG PADRÃO ==========

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
            db.add(sede_cong)
            db.flush()
        if db.scalar(select(User).where(User.username == ADMIN_USERNAME)) is None:
            db.add(User(
                username=ADMIN_USERNAME,
                password_hash=hash_password("123456"),
                role="SEDE",
                congregation_id=sede_cong.id,
            ))
        db.commit()

# ========== SESSÃO E LOGIN ==========

if "uid" not in st.session_state:
    st.session_state.uid = None

def current_user() -> Optional["User"]:
    uid = st.session_state.get("uid")
    if not uid:
        return None
    with SessionLocal() as db:
        return db.get(User, uid)

def restore_user_from_url():
    try:
        uid_qp = st.query_params.get("uid")
        if uid_qp and not st.session_state.get("uid"):
            with SessionLocal() as db:
                u = db.get(User, int(uid_qp))
                if u:
                    st.session_state.uid = u.id
    except Exception:
        pass

def push_uid_to_url(uid: int):
    try:
        if st.query_params.get("uid") != str(uid):
            q = dict(st.query_params)
            q["uid"] = str(uid)
            st.query_params.clear()
            st.query_params.update(q)
    except Exception:
        pass

def clear_uid_from_url():
    try:
        if "uid" in st.query_params:
            q = dict(st.query_params)
            q.pop("uid", None)
            st.query_params.clear()
            if q:
                st.query_params.update(q)
    except Exception:
        pass

def do_logout():
    st.session_state.clear()
    clear_uid_from_url()
    st.rerun()

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
                push_uid_to_url(user.id)
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

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
            st.button("Sair", on_click=do_logout)

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
# ===================== RELATÓRIO DE ENTRADA =====================
def page_relatorio_entrada(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

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

        # Exclusão — agora liberado para qualquer usuário!
        if not is_all and cong_obj is not None:
            st.divider()
            st.subheader("Excluir ENTRADAS / DÍZIMOS (da sua congregação)")

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
                            _db.query(Transaction).filter(
                                Transaction.id.in_(ids),
                                Transaction.congregation_id == cong_obj.id
                            ).delete(synchronize_session=False)
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
                            _db.query(Tithe).filter(
                                Tithe.id.in_(ids2),
                                Tithe.congregation_id == cong_obj.id
                            ).delete(synchronize_session=False)
                            _db.commit()
                        st.success(f"{len(ids2)} dízimo(s) excluído(s).")
                        st.rerun()
                else:
                    st.caption("Sem dízimos no período/escopo.")

# ===================== RELATÓRIO DE SAÍDA =====================
def page_relatorio_saida(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

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

        # Exclusão — agora liberado para qualquer usuário!
        if not is_all and cong_obj is not None:
            st.divider(); st.subheader("Excluir SAÍDAS (da sua congregação)")
            if 'df_list' in locals() and not df_list.empty:
                ids = st.multiselect("IDs para excluir", df_list["ID"].tolist(), key="del_out_ids")
                conf = st.text_input("Digite EXCLUIR para confirmar", key="del_out_conf")
                btn_disabled = (not ids) or (not _confirm_ok(conf))
                if st.button("Excluir selecionados", disabled=btn_disabled, key="del_out_btn"):
                    with SessionLocal() as _db:
                        _db.query(Transaction).filter(
                            Transaction.id.in_(ids),
                            Transaction.congregation_id == cong_obj.id
                        ).delete(synchronize_session=False)
                        _db.commit()
                    st.success(f"{len(ids)} saída(s) excluída(s).")
                    st.rerun()
            else:
                st.caption("Sem saídas para exclusão neste escopo.")
# ===================== PAGE: RELATÓRIO DE SAÍDA =====================
def page_relatorio_saida(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

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

        # === EXCLUSÃO DE SAÍDAS: TODAS AS CONGREGAÇÕES ===
        if not is_all and cong_obj:  # para todos, menos escopo "Todas"
            st.divider(); st.subheader("Excluir SAÍDAS")
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
# ===================== PAGE: RELATÓRIO DE DIZIMISTAS =====================
def page_relatorio_dizimistas(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        with st.sidebar:
            if os.path.exists(LOGO_PATH):
                st.image(LOGO_PATH, use_column_width=True)
            st.write(f"👤 **{user.username}** — *{user.role}*")
            st.button("Sair", on_click=do_logout)

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

        # ===== visão mensal padrão =====
        if is_all:
            all_tz = db.scalars(select(Tithe).where(Tithe.date >= start, Tithe.date < end)).all()
            by_cong = defaultdict(lambda: {"qtd":0, "valor":0.0})
            for t in all_tz:
                k = t.congregation.name
                by_cong[k]["qtd"] += 1
                by_cong[k]["valor"] += float(t.amount)
            df = pd.DataFrame([{"Congregação": k, "Qtde de dizimistas": v["qtd"], "Total (R$)": format_currency(v["valor"])} for k,v in sorted(by_cong.items())])
            st.dataframe(df, use_container_width=True, hide_index=True, height=200)
            st.info("Selecione uma congregação específica para ver a lista nominal.")
        else:
            tithes = db.scalars(select(Tithe).where(
                Tithe.date >= start, Tithe.date < end, Tithe.congregation_id == cong_obj.id
            ).order_by(Tithe.date)).all()

            tithes_by_payment = defaultdict(lambda: {"count": 0, "total": 0.0})
            for tithe in tithes:
                method = tithe.payment_method or "Não Informado"
                tithes_by_payment[method]["count"] += 1
                tithes_by_payment[method]["total"] += float(tithe.amount)

            st.subheader("Resumo de Pagamentos de Dízimos")
            cols_metrics = st.columns(max(1, len(tithes_by_payment)))
            for i, (method, data) in enumerate(tithes_by_payment.items()):
                cols_metrics[i].metric(f"Total ({method})", format_currency(data["total"]), f"{data['count']} dízimos")

            st.divider()
            df = pd.DataFrame([{
                "Data": format_date(t.date),
                "Dizimista": t.tither_name,
                "Valor (R$)": format_currency(float(t.amount)),
                "Forma de Pagamento": t.payment_method or "Não Informado"
            } for t in tithes])
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True, height=200)
                st.metric("Total de dízimos", format_currency(sum(float(t.amount) for t in tithes)))
            else:
                st.caption("Sem dízimos neste período.")

        # ===== PESQUISA AVANÇADA POR ANO =====
        # ... (o restante da função segue igual ao seu código)
        # [Cortei por limite de tamanho, mas se quiser, mando o bloco inteiro dessa função]
def main():
    try:
        ensure_seed()
        restore_user_from_url()
        user = current_user()
        if not user:
            login_ui(); return
        if st.query_params.get("uid") != str(user.id):
            push_uid_to_url(user.id)

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
