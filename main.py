# main.py — AD Relatório Financeiro (mobile scroll + login persistente + páginas)
# Observação: este arquivo é autossuficiente. Não use experimental_* do Streamlit.

from __future__ import annotations

import os
import hashlib
from io import BytesIO
from datetime import date, datetime
from typing import Optional, List, Tuple, Dict

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from sqlalchemy import (
    select, func, String, Date, Float, ForeignKey, create_engine
)
from sqlalchemy.orm import (
    relationship, Mapped, mapped_column, sessionmaker, Session, joinedload
)
from sqlalchemy.ext.declarative import declarative_base

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER

# ===================== CONFIG BÁSICA =====================

ADMIN_USERNAME = "admin"      # usuário mestre
APP_TITLE = "AD Relatório Financeiro"

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="⛪",
    layout="wide",
    initial_sidebar_state="expanded",   # sidebar sempre aberta no mobile
)

# ===================== CSS (inclui correção de rolagem) =====================

CSS = """
<style>
:root{ --ring:#e5e7eb; }

html, body {
  height:auto!important; min-height:100vh!important;
  overflow-y:auto!important; overscroll-behavior-y:contain;
  -webkit-overflow-scrolling:touch;
}
.stApp{ min-height:100vh!important; overflow:visible!important; }
.block-container{ min-height:100vh!important; padding-top:.8rem!important; }
header[data-testid="stHeader"]{ background:linear-gradient(180deg,#fff,#f7faff)!important; border-bottom:1px solid var(--ring); }
[data-testid="stSidebar"]{ background:linear-gradient(180deg,#fff,#fbfdff)!important; border-right:1px solid var(--ring); }

/* esconder bolhas/overlays que travam rolagem no rodapé no celular */
@media (pointer:coarse){
  footer, [data-testid="stToolbar"], [aria-label*="Manage app"], [title*="Manage app"],
  div[style*="position: fixed"][style*="bottom"]{ display:none!important; }
}

/* títulos e cartões */
.page-title{ font:900 36px/1.1 "Inter",system-ui; color:#0f172a; margin:.2rem 0 1rem; }
.st-card{ border:1px solid var(--ring); border-radius:1rem; background:#fff; padding:1rem; box-shadow:0 10px 30px rgba(31,58,138,.06),0 2px 8px rgba(0,0,0,.04); }
.st-metric{ border:1px solid var(--ring); border-radius:.9rem; background:#fff; padding:.6rem .8rem; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)
components.html("<script>/* noop for mobile */</script>", height=0)

# ===================== DB (SQLAlchemy 2.0) =====================

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)  # SEDE | TESOUREIRO | TESOUREIRO MISSIONÁRIO
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
    type: Mapped[str] = mapped_column(String)  # DOAÇÃO | SAÍDA
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="category")

class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    type: Mapped[str] = mapped_column(String)  # DOAÇÃO | SAÍDA
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    amount: Mapped[float] = mapped_column(Float)
    description: Mapped[Optional[str]] = mapped_column(String)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    category: Mapped["Category"] = relationship(back_populates="transactions", lazy="joined")
    congregation: Mapped["Congregation"] = relationship(back_populates="transactions")

class Tithe(Base):
    __tablename__ = "tithes"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[date] = mapped_column(Date)
    tither_name: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    congregation: Mapped["Congregation"] = relationship(back_populates="tithes")

TYPE_IN = "DOAÇÃO"
TYPE_OUT = "SAÍDA"

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

# ===================== UTILS =====================

def brl(v) -> str:
    try: x = float(v or 0.0)
    except Exception: x = 0.0
    s = f"{x:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

def month_bounds(ref: date) -> Tuple[date, date]:
    start = ref.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start, end

def month_selector(label="Mês de referência") -> date:
    today = date.today()
    cols = st.columns([2, 1])
    with cols[0]:
        mes = st.selectbox(label+" — Mês", list(range(1,13)), index=today.month-1,
                           format_func=lambda i: ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"][i-1])
    with cols[1]:
        ano = st.number_input("Ano", value=today.year, step=1, format="%d")
    return date(int(ano), int(mes), 1)

# ===================== SEED =====================

CONG_DEFAULT = [
    "Sede","Abreus","Alto Alencar","Alto da Aliança","Alto do Cruzeiro",
    "Rodeadouro","Dr. Humberto","Jatobá","Massaroca","Riacho Seco"
]

def ensure_seed():
    with SessionLocal() as db:
        if db.scalar(select(func.count(Congregation.id))) == 0:
            for n in CONG_DEFAULT:
                db.add(Congregation(name=n))
        if db.scalar(select(func.count(Category.id))) == 0:
            for nm,tp in [("Dízimo",TYPE_IN),("Oferta",TYPE_IN),("Missões",TYPE_IN),
                          ("Aluguel",TYPE_OUT),("Energia",TYPE_OUT),("Transporte",TYPE_OUT),
                          ("Material de Expediente",TYPE_OUT),("Produtos de Limpeza",TYPE_OUT),
                          ("Assistência Social",TYPE_OUT),("Missões (Saída)",TYPE_OUT)]:
                db.add(Category(name=nm, type=tp))
        if db.scalar(select(User).where(User.username==ADMIN_USERNAME)) is None:
            sede = db.scalar(select(Congregation).where(Congregation.name=="Sede"))
            db.add(User(username=ADMIN_USERNAME, password_hash=hash_password("123456"),
                        role="SEDE", congregation_id=sede.id if sede else None))
        db.commit()

# ===================== AUTH (persistência por query params) =====================

APP_SECRET = st.secrets.get("APP_SECRET", os.environ.get("APP_SECRET", "dev-secret"))

def hash_password(pwd: str) -> str:
    salt = os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt, 100000)
    return salt.hex()+":"+h.hex()

def verify_password(pwd: str, stored: str) -> bool:
    salt_hex, h_hex = stored.split(":")
    salt = bytes.fromhex(salt_hex)
    h = bytes.fromhex(h_hex)
    nh = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt, 100000)
    return nh == h

def make_token(user: User) -> str:
    base = f"{user.id}:{user.password_hash}:{APP_SECRET}"
    return hashlib.sha256(base.encode()).hexdigest()[:32]

def _qp_get_first(key: str) -> Optional[str]:
    try:
        val = st.query_params.get(key)
    except Exception:
        return None
    if val is None: return None
    if isinstance(val, (list, tuple)): return val[0] if val else None
    return str(val)

def set_auth_qp(uid: int, token: str):
    try:
        st.query_params["uid"] = str(uid)
        st.query_params["t"] = token
    except Exception:
        pass

def clear_auth_qp():
    try:
        if "uid" in st.query_params: del st.query_params["uid"]
        if "t" in st.query_params: del st.query_params["t"]
    except Exception:
        pass

if "uid" not in st.session_state:
    st.session_state.uid = None

def current_user() -> Optional[User]:
    uid = st.session_state.get("uid")
    if not uid: return None
    with SessionLocal() as db:
        return db.get(User, uid)

def set_logged_user(user: User):
    st.session_state.uid = user.id
    st.session_state.username = user.username
    st.session_state.role = user.role
    st.session_state.congregation_id = user.congregation_id
    set_auth_qp(user.id, make_token(user))

def hydrate_from_qp():
    uid_s = _qp_get_first("uid")
    tok = _qp_get_first("t")
    if uid_s and tok and not st.session_state.get("uid"):
        try:
            uid = int(uid_s)
        except Exception:
            return
        with SessionLocal() as db:
            user = db.get(User, uid)
            if user and tok == make_token(user):
                set_logged_user(user)

def do_logout():
    st.session_state.clear()
    clear_auth_qp()
    st.rerun()

def login_ui():
    st.markdown(f"<h1 class='page-title'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    u = st.text_input("Usuário")
    p = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        with SessionLocal() as db:
            user = db.scalar(select(User).where(User.username == u))
            if user and verify_password(p, user.password_hash):
                set_logged_user(user)
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")

# ===================== PÁGINAS =====================

def cong_options_for(user: User, db: Session) -> List[Congregation]:
    if user.role == "SEDE":
        return db.scalars(select(Congregation).order_by(Congregation.name)).all()
    else:
        c = db.get(Congregation, user.congregation_id) if user.congregation_id else None
        return [c] if c else []

def page_lancamentos(user: User):
    st.markdown("<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)

    with SessionLocal() as db:
        congs = cong_options_for(user, db)
        if not congs:
            st.info("Sem congregação vinculada.")
            return

        if user.role == "SEDE":
            cong = st.selectbox("Selecione a congregação", congs, index=0, format_func=lambda c: c.name)
            cong_id = cong.id
            cong_name = cong.name
        else:
            cong = congs[0]
            cong_id = cong.id; cong_name = cong.name
            st.write(f"**CONGREGAÇÃO:** {cong_name}")

        st.divider()
        st.subheader("Lançar ENTRADA (Doação)")

        col1, col2 = st.columns([1, 1])
        with col1:
            d = st.date_input("Data do Culto", value=date.today(), format="DD/MM/YYYY")
        with col2:
            cats = db.scalars(select(Category).where(Category.type==TYPE_IN).order_by(Category.name)).all()
            cat = st.selectbox("Categoria (ordem fixa: Dízimo, Oferta, Missões)",
                               options=cats, index=0, format_func=lambda c: c.name)

        desc = st.text_input("Descrição (opcional)")
        colv, _, _ = st.columns([2,1,1])
        with colv:
            val = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f")

        if st.button("Salvar ENTRADA", type="primary"):
            if val <= 0:
                st.warning("Informe um valor maior que zero.")
                return
            if "diz" in cat.name.lower():  # Dízimo vai para tabela própria (evita duplicidade)
                db.add(Tithe(date=d, tither_name="Dízimo (coletivo)", amount=float(val), congregation_id=cong_id))
            else:
                db.add(Transaction(date=d, type=TYPE_IN, category_id=cat.id, amount=float(val),
                                   description=desc or None, congregation_id=cong_id))
            db.commit()
            st.success(f"Entrada registrada para **{cong_name}**: {cat.name} — {brl(val)}")

def _df_transactions(db: Session, kind: str, cong_id: Optional[int], start: date, end: date) -> pd.DataFrame:
    q = select(Transaction).options(joinedload(Transaction.category), joinedload(Transaction.congregation))\
        .where(Transaction.type == kind, Transaction.date >= start, Transaction.date < end)
    if cong_id:
        q = q.where(Transaction.congregation_id == cong_id)
    rows = db.scalars(q).all()
    data = [{
        "Data": r.date.strftime("%d/%m/%Y"),
        "Congregação": r.congregation.name if r.congregation else "",
        "Categoria": r.category.name if r.category else "",
        "Valor": float(r.amount),
        "Descrição": r.description or "",
    } for r in rows]
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values(["Data","Congregação","Categoria"])
    return df

def _df_tithes(db: Session, cong_id: Optional[int], start: date, end: date) -> pd.DataFrame:
    q = select(Tithe).options(joinedload(Tithe.congregation))\
        .where(Tithe.date >= start, Tithe.date < end)
    if cong_id:
        q = q.where(Tithe.congregation_id == cong_id)
    rows = db.scalars(q).all()
    data = [{
        "Data": r.date.strftime("%d/%m/%Y"),
        "Congregação": r.congregation.name if r.congregation else "",
        "Dizimista": r.tither_name,
        "Valor": float(r.amount),
    } for r in rows]
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values(["Data","Congregação","Dizimista"])
    return df

def _totals_cards(df: pd.DataFrame, label_main="Total") -> None:
    if df.empty:
        st.info("Sem lançamentos para o período.")
        return
    total = float(df["Valor"].sum())
    col = st.container()
    with col:
        st.metric(label_main, brl(total))

def _simple_pdf(title: str, df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, rightMargin=18, leftMargin=18, topMargin=24, bottomMargin=18)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="H1", parent=styles["Heading1"], alignment=TA_CENTER))
    elems = []
    elems.append(Paragraph(title, styles["H1"]))
    elems.append(Spacer(1, 12))
    if df.empty:
        elems.append(Paragraph("Sem registros no período.", styles["Normal"]))
    else:
        # tabela
        cols = list(df.columns)
        data = [cols] + df.values.tolist()
        # formata valores
        for i in range(1, len(data)):
            for j, c in enumerate(cols):
                if c == "Valor":
                    data[i][j] = brl(data[i][j])
        t = Table(data, colWidths=[5*cm, 5*cm, 5*cm, 3*cm, 5*cm][:len(cols)])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("TEXTCOLOR", (0,0), (-1,0), colors.black),
            ("ALIGN", (0,0), (-1,-1), "LEFT"),
            ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("BOTTOMPADDING", (0,0), (-1,0), 8),
        ]))
        elems.append(t)
        elems.append(Spacer(1, 8))
        total = df["Valor"].sum() if "Valor" in df.columns else 0.0
        elems.append(Paragraph(f"<b>Total:</b> {brl(total)}", styles["Normal"]))
    doc.build(elems)
    return buf.getvalue()

def page_relatorio_entrada(user: User):
    st.markdown("<h1 class='page-title'>Relatório de Entrada</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None
        if user.role != "SEDE":
            cong_id = user.congregation_id
        df = _df_transactions(db, TYPE_IN, cong_id, start, end)
        _totals_cards(df, "Total de Entradas")
        st.dataframe(df, use_container_width=True)
        pdf = _simple_pdf(f"Relatório de Entrada — {ref.strftime('%m/%Y')}", df)
        st.download_button("Baixar PDF", data=pdf, file_name="relatorio_entrada.pdf", mime="application/pdf")

def page_relatorio_saida(user: User):
    st.markdown("<h1 class='page-title'>Relatório de Saída</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None
        if user.role != "SEDE":
            cong_id = user.congregation_id
        df = _df_transactions(db, TYPE_OUT, cong_id, start, end)
        _totals_cards(df, "Total de Saídas")
        st.dataframe(df, use_container_width=True)
        pdf = _simple_pdf(f"Relatório de Saída — {ref.strftime('%m/%Y')}", df)
        st.download_button("Baixar PDF", data=pdf, file_name="relatorio_saida.pdf", mime="application/pdf")

def page_relatorio_dizimistas(user: User):
    st.markdown("<h1 class='page-title'>Relatório de Dizimistas</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None
        if user.role != "SEDE":
            cong_id = user.congregation_id
        df = _df_tithes(db, cong_id, start, end)
        _totals_cards(df, "Total de Dízimos")
        st.dataframe(df, use_container_width=True)
        pdf = _simple_pdf(f"Relatório de Dizimistas — {ref.strftime('%m/%Y')}", df)
        st.download_button("Baixar PDF", data=pdf, file_name="relatorio_dizimistas.pdf", mime="application/pdf")

def page_relatorio_missoes(user: User):
    st.markdown("<h1 class='page-title'>Relatório de Missões</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None
        if user.role != "SEDE":
            cong_id = user.congregation_id
        # Missões: entradas pela categoria "Missões" (não confundir com "Missões (Saída)")
        cat_ids = [c.id for c in db.scalars(select(Category).where(Category.type==TYPE_IN)).all()
                   if "miss" in c.name.lower()]
        q = select(Transaction).options(joinedload(Transaction.category), joinedload(Transaction.congregation))\
            .where(Transaction.date >= start, Transaction.date < end, Transaction.category_id.in_(cat_ids))
        if cong_id:
            q = q.where(Transaction.congregation_id==cong_id)
        rows = db.scalars(q).all()
        data = [{
            "Data": r.date.strftime("%d/%m/%Y"),
            "Congregação": r.congregation.name if r.congregation else "",
            "Categoria": r.category.name if r.category else "",
            "Valor": float(r.amount),
            "Descrição": r.description or "",
        } for r in rows]
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values(["Data","Congregação"])
        _totals_cards(df, "Total de Missões")
        st.dataframe(df, use_container_width=True)
        pdf = _simple_pdf(f"Relatório de Missões — {ref.strftime('%m/%Y')}", df)
        st.download_button("Baixar PDF", data=pdf, file_name="relatorio_missoes.pdf", mime="application/pdf")

def page_cadastro(user: User):
    st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)
    st.info("Área simples para cadastrar categorias.")
    with SessionLocal() as db:
        with st.expander("Nova categoria"):
            nome = st.text_input("Nome da categoria")
            tipo = st.selectbox("Tipo", [TYPE_IN, TYPE_OUT])
            if st.button("Salvar categoria"):
                if not nome.strip():
                    st.warning("Informe o nome.")
                elif db.scalar(select(Category).where(Category.name==nome)):
                    st.error("Categoria já existe.")
                else:
                    db.add(Category(name=nome.strip(), type=tipo))
                    db.commit()
                    st.success("Categoria cadastrada.")
                    st.rerun()
        st.subheader("Categorias cadastradas")
        cats = db.scalars(select(Category).order_by(Category.type, Category.name)).all()
        df = pd.DataFrame([{"Tipo":c.type, "Nome":c.name} for c in cats])
        st.dataframe(df, use_container_width=True)

def page_visao_geral(user: User):
    st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)

    with SessionLocal() as db:
        # total por congregação (somando Entradas — inclusive Dízimo/Tithe — menos Saídas)
        congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()

        ranking: List[Tuple[str,float]] = []
        for c in congs:
            # entradas comuns
            tx_in = db.scalars(
                select(func.sum(Transaction.amount))
                .where(Transaction.congregation_id==c.id,
                       Transaction.type==TYPE_IN,
                       Transaction.date>=start, Transaction.date<end)
            ).first() or 0.0
            # dízimos (tabela própria)
            tithes = db.scalars(
                select(func.sum(Tithe.amount))
                .where(Tithe.congregation_id==c.id,
                       Tithe.date>=start, Tithe.date<end)
            ).first() or 0.0
            # saídas
            tx_out = db.scalars(
                select(func.sum(Transaction.amount))
                .where(Transaction.congregation_id==c.id,
                       Transaction.type==TYPE_OUT,
                       Transaction.date>=start, Transaction.date<end)
            ).first() or 0.0

            total = float(tx_in) + float(tithes) - float(tx_out)
            ranking.append((c.name, total))

        # ordena por total desc. (evita duplicações — cada congregação aparece uma vez)
        ranking.sort(key=lambda x: x[1], reverse=True)

        # cards
        total_geral = sum(v for _,v in ranking)
        st.metric("Saldo consolidado do mês", brl(total_geral))
        st.write("")

        # top 5
        for i, (nome, val) in enumerate(ranking[:5], start=1):
            st.write(f"**{i}º lugar** — {nome} — {brl(val)}")

        # tabela completa
        st.write("")
        df = pd.DataFrame([{"Congregação":n, "Total do mês": brl(v)} for n,v in ranking])
        st.dataframe(df, use_container_width=True)

# ===================== MAIN =====================

def main():
    try:
        ensure_seed()
        hydrate_from_qp()
        user = current_user()
        if not user:
            login_ui()
            return

        with st.sidebar:
            if user.role == "SEDE":
                menu = ["Lançamentos","Relatório de Entrada","Relatório de Saída",
                        "Relatório de Dizimistas","Relatório de Missões","Visão Geral","Cadastro"]
            elif user.role == "TESOUREIRO":
                menu = ["Lançamentos","Relatório de Entrada","Relatório de Saída",
                        "Relatório de Dizimistas","Visão Geral"]
            elif user.role == "TESOUREIRO MISSIONÁRIO":
                menu = ["Relatório de Missões"]
            else:
                menu = ["Visão Geral"]
            choice = st.radio("Menu", options=menu, index=0, key="menu_main")
            st.button("Sair", on_click=do_logout)

        if choice == "Lançamentos":
            page_lancamentos(user)
        elif choice == "Relatório de Entrada":
            page_relatorio_entrada(user)
        elif choice == "Relatório de Saída":
            page_relatorio_saida(user)
        elif choice == "Relatório de Dizimistas":
            page_relatorio_dizimistas(user)
        elif choice == "Relatório de Missões":
            page_relatorio_missoes(user)
        elif choice == "Visão Geral":
            page_visao_geral(user)
        elif choice == "Cadastro":
            page_cadastro(user)

    except Exception as e:
        st.error("Ocorreu um erro ao renderizar a aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
