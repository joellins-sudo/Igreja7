# main.py — AD Relatório Financeiro
# Sidebar no modelo original + login persistente + rolagem mobile corrigida.

from __future__ import annotations

import os
import hashlib
from io import BytesIO
from datetime import date, datetime
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from sqlalchemy import (
    select, func, String, Date, Float, ForeignKey, create_engine
)
from sqlalchemy.orm import (
    relationship, Mapped, mapped_column, sessionmaker, joinedload
)
from sqlalchemy.ext.declarative import declarative_base

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER


# ===================== CONFIG =====================

APP_TITLE = "AD Relatório Financeiro"
ADMIN_USERNAME = "admin"

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="⛪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ===================== CSS (rolagem mobile + layout original) =====================

CSS = """
<style>
html, body { height:auto!important; min-height:100vh!important; overflow-y:auto!important;
  -webkit-overflow-scrolling:touch; overscroll-behavior-y:contain; }
.stApp{ min-height:100vh!important; overflow:visible!important; }
.block-container{ min-height:100vh!important; padding-top:.6rem!important; }

/* remover overlays flutuantes que travam rolagem no celular */
@media (pointer:coarse){
  footer, [data-testid="stToolbar"], [aria-label*="Manage app"], [title*="Manage app"],
  div[style*="position: fixed"][style*="bottom"]{ display:none!important; }
}

/* títulos */
.page-title{ font:900 36px/1.1 "Inter",system-ui; color:#0f172a; margin:.2rem 0 1rem; }

/* sidebar */
[data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3{ margin-top:.25rem; }

/* botões/inputs */
button[kind="primary"]{ font-weight:700; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)
components.html("<script>/* mobile helpers */</script>", height=0)


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


# ===================== UTIL / SEED =====================

def brl(v) -> str:
    try:
        x = float(v or 0.0)
    except Exception:
        x = 0.0
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
        mes = st.selectbox(
            label + " — Mês",
            list(range(1, 13)),
            index=today.month - 1,
            format_func=lambda i: ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"][i - 1],
            key=f"mes_{label}",
        )
    with cols[1]:
        ano = st.number_input("Ano", value=today.year, step=1, format="%d", key=f"ano_{label}")
    return date(int(ano), int(mes), 1)

CONG_DEFAULT = [
    "Sede", "Abreus", "Alto Alencar", "Alto da Aliança", "Alto do Cruzeiro",
    "Rodeadouro", "Dr. Humberto", "Jatobá", "Massaroca", "Riacho Seco"
]

def ensure_seed():
    with SessionLocal() as db:
        if db.scalar(select(func.count(Congregation.id))) == 0:
            for n in CONG_DEFAULT:
                db.add(Congregation(name=n))
        if db.scalar(select(func.count(Category.id))) == 0:
            preset = [
                ("Dízimo", TYPE_IN), ("Oferta", TYPE_IN), ("Missões", TYPE_IN),
                ("Aluguel", TYPE_OUT), ("Energia", TYPE_OUT), ("Transporte", TYPE_OUT),
                ("Material de Expediente", TYPE_OUT), ("Produtos de Limpeza", TYPE_OUT),
                ("Assistência Social", TYPE_OUT), ("Missões (Saída)", TYPE_OUT)
            ]
            for nm, tp in preset:
                db.add(Category(name=nm, type=tp))
        if db.scalar(select(User).where(User.username == ADMIN_USERNAME)) is None:
            sede = db.scalar(select(Congregation).where(Congregation.name == "Sede"))
            db.add(
                User(
                    username=ADMIN_USERNAME,
                    password_hash=hash_password("123456"),
                    role="SEDE",
                    congregation_id=sede.id if sede else None,
                )
            )
        db.commit()


# ===================== AUTH (persistência com query params) =====================

APP_SECRET = st.secrets.get("APP_SECRET", os.environ.get("APP_SECRET", "dev-secret"))

def hash_password(pwd: str) -> str:
    salt = os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt, 100000)
    return salt.hex() + ":" + h.hex()

def verify_password(pwd: str, stored: str) -> bool:
    salt_hex, h_hex = stored.split(":")
    salt = bytes.fromhex(salt_hex)
    h = bytes.fromhex(h_hex)
    nh = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt, 100000)
    return nh == h

def make_token(user: "User") -> str:
    base = f"{user.id}:{user.password_hash}:{APP_SECRET}"
    return hashlib.sha256(base.encode()).hexdigest()[:32]

def _qp_get_first(key: str) -> Optional[str]:
    try:
        val = st.query_params.get(key)
    except Exception:
        return None
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        return val[0] if val else None
    return str(val)

def set_auth_qp(uid: int, token: str):
    try:
        st.query_params["uid"] = str(uid)
        st.query_params["t"] = token
    except Exception:
        pass

def clear_auth_qp():
    try:
        for k in ("uid", "t", "p"):
            if k in st.query_params:
                del st.query_params[k]
    except Exception:
        pass

if "uid" not in st.session_state:
    st.session_state.uid = None

def current_user() -> Optional["User"]:
    uid = st.session_state.get("uid")
    if not uid:
        return None
    with SessionLocal() as db:
        return db.get(User, uid)

def set_logged_user(user: "User"):
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

def cong_options_for(user: "User", db) -> List["Congregation"]:
    if user.role == "SEDE":
        return db.scalars(select(Congregation).order_by(Congregation.name)).all()
    c = db.get(Congregation, user.congregation_id) if user.congregation_id else None
    return [c] if c else []

def page_lancamentos(user: "User"):
    st.markdown("<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)
    with SessionLocal() as db:
        congs = cong_options_for(user, db)
        if not congs:
            st.info("Sem congregação vinculada.")
            return
        if user.role == "SEDE":
            cong = st.selectbox(
                "Selecione a congregação", congs, index=0, format_func=lambda c: c.name
            )
        else:
            cong = congs[0]
            st.write(f"**CONGREGAÇÃO:** {cong.name}")

        st.divider()
        st.subheader("Lançar ENTRADA (Doação)")
        c1, c2 = st.columns([1, 1])
        with c1:
            d = st.date_input("Data do Culto", value=date.today(), format="DD/MM/YYYY")
        with c2:
            cats = db.scalars(
                select(Category).where(Category.type == TYPE_IN).order_by(Category.name)
            ).all()
            cat = st.selectbox(
                "Categoria (ordem fixa: Dízimo, Oferta, Missões)",
                options=cats,
                index=0,
                format_func=lambda c: c.name,
            )
        desc = st.text_input("Descrição (opcional)")
        v = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f")
        if st.button("Salvar ENTRADA", type="primary"):
            if v <= 0:
                st.warning("Informe um valor maior que zero.")
                return
            if "diz" in cat.name.lower():
                db.add(
                    Tithe(
                        date=d,
                        tither_name="Dízimo (coletivo)",
                        amount=float(v),
                        congregation_id=cong.id,
                    )
                )
            else:
                db.add(
                    Transaction(
                        date=d,
                        type=TYPE_IN,
                        category_id=cat.id,
                        amount=float(v),
                        description=desc or None,
                        congregation_id=cong.id,
                    )
                )
            db.commit()
            st.success(
                f"Entrada registrada para **{cong.name}**: {cat.name} — {brl(v)}"
            )

def _df_transactions(db, kind: str, cong_id: Optional[int], start: date, end: date) -> pd.DataFrame:
    q = (
        select(Transaction)
        .options(joinedload(Transaction.category), joinedload(Transaction.congregation))
        .where(Transaction.type == kind, Transaction.date >= start, Transaction.date < end)
    )
    if cong_id:
        q = q.where(Transaction.congregation_id == cong_id)
    rows = db.scalars(q).all()
    data = [
        {
            "Data": r.date.strftime("%d/%m/%Y"),
            "Congregação": r.congregation.name if r.congregation else "",
            "Categoria": r.category.name if r.category else "",
            "Valor": float(r.amount),
            "Descrição": r.description or "",
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values(["Data", "Congregação", "Categoria"])
    return df

def _df_tithes(db, cong_id: Optional[int], start: date, end: date) -> pd.DataFrame:
    q = select(Tithe).options(joinedload(Tithe.congregation)).where(
        Tithe.date >= start, Tithe.date < end
    )
    if cong_id:
        q = q.where(Tithe.congregation_id == cong_id)
    rows = db.scalars(q).all()
    data = [
        {
            "Data": r.date.strftime("%d/%m/%Y"),
            "Congregação": r.congregation.name if r.congregation else "",
            "Dizimista": r.tither_name,
            "Valor": float(r.amount),
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values(["Data", "Congregação", "Dizimista"])
    return df

def _totals_cards(df: pd.DataFrame, label="Total"):
    if df.empty:
        st.info("Sem lançamentos para o período.")
        return
    st.metric(label, brl(float(df["Valor"].sum())))

def _simple_pdf(title: str, df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, rightMargin=18, leftMargin=18, topMargin=24, bottomMargin=18
    )
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="H1", parent=styles["Heading1"], alignment=TA_CENTER))
    elems = [Paragraph(title, styles["H1"]), Spacer(1, 12)]
    if df.empty:
        elems.append(Paragraph("Sem registros no período.", styles["Normal"]))
    else:
        cols = list(df.columns)
        data = [cols] + df.values.tolist()
        for i in range(1, len(data)):
            for j, c in enumerate(cols):
                if c == "Valor":
                    data[i][j] = brl(data[i][j])
        t = Table(data, colWidths=[5*cm, 5*cm, 5*cm, 3*cm, 5*cm][:len(cols)])
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                ]
            )
        )
        elems += [
            t,
            Spacer(1, 8),
            Paragraph(
                f"<b>Total:</b> {brl(df['Valor'].sum() if 'Valor' in df.columns else 0.0)}",
                styles["Normal"],
            ),
        ]
    doc.build(elems)
    return buf.getvalue()

def page_relatorio_entrada(user: "User"):
    st.markdown("<h1 class='page-title'>Relatório de Entrada</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None if user.role == "SEDE" else user.congregation_id
        df = _df_transactions(db, TYPE_IN, cong_id, start, end)
        _totals_cards(df, "Total de Entradas")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Baixar PDF",
            data=_simple_pdf(f"Relatório de Entrada — {ref.strftime('%m/%Y')}", df),
            file_name="relatorio_entrada.pdf",
            mime="application/pdf",
        )

def page_relatorio_saida(user: "User"):
    st.markdown("<h1 class='page-title'>Relatório de Saída</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None if user.role == "SEDE" else user.congregation_id
        df = _df_transactions(db, TYPE_OUT, cong_id, start, end)
        _totals_cards(df, "Total de Saídas")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Baixar PDF",
            data=_simple_pdf(f"Relatório de Saída — {ref.strftime('%m/%Y')}", df),
            file_name="relatorio_saida.pdf",
            mime="application/pdf",
        )

def page_relatorio_dizimistas(user: "User"):
    st.markdown("<h1 class='page-title'>Relatório de Dizimistas</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None if user.role == "SEDE" else user.congregation_id
        df = _df_tithes(db, cong_id, start, end)
        _totals_cards(df, "Total de Dízimos")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Baixar PDF",
            data=_simple_pdf(f"Relatório de Dizimistas — {ref.strftime('%m/%Y')}", df),
            file_name="relatorio_dizimistas.pdf",
            mime="application/pdf",
        )

def page_relatorio_missoes(user: "User"):
    st.markdown("<h1 class='page-title'>Relatório de Missões</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        cong_id = None if user.role == "SEDE" else user.congregation_id
        miss_in_ids = [
            c.id
            for c in db.scalars(select(Category).where(Category.type == TYPE_IN)).all()
            if "miss" in c.name.lower()
        ]
        q = (
            select(Transaction)
            .options(joinedload(Transaction.category), joinedload(Transaction.congregation))
            .where(
                Transaction.date >= start,
                Transaction.date < end,
                Transaction.category_id.in_(miss_in_ids),
            )
        )
        if cong_id:
            q = q.where(Transaction.congregation_id == cong_id)
        rows = db.scalars(q).all()
        data = [
            {
                "Data": r.date.strftime("%d/%m/%Y"),
                "Congregação": r.congregation.name if r.congregation else "",
                "Categoria": r.category.name if r.category else "",
                "Valor": float(r.amount),
                "Descrição": r.description or "",
            }
            for r in rows
        ]
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values(["Data", "Congregação"])
        _totals_cards(df, "Total de Missões")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Baixar PDF",
            data=_simple_pdf(f"Relatório de Missões — {ref.strftime('%m/%Y')}", df),
            file_name="relatorio_missoes.pdf",
            mime="application/pdf",
        )

def page_cadastro(user: "User"):
    st.markdown("<h1 class='page-title'>Cadastro</h1>", unsafe_allow_html=True)
    st.info("Área simples para cadastrar categorias.")
    with SessionLocal() as db:
        with st.expander("Nova categoria"):
            nome = st.text_input("Nome da categoria")
            tipo = st.selectbox("Tipo", [TYPE_IN, TYPE_OUT])
            if st.button("Salvar categoria"):
                if not nome.strip():
                    st.warning("Informe o nome.")
                elif db.scalar(select(Category).where(Category.name == nome)):
                    st.error("Categoria já existe.")
                else:
                    db.add(Category(name=nome.strip(), type=tipo))
                    db.commit()
                    st.success("Categoria cadastrada.")
                    st.rerun()
        st.subheader("Categorias cadastradas")
        cats = db.scalars(select(Category).order_by(Category.type, Category.name)).all()
        df = pd.DataFrame([{"Tipo": c.type, "Nome": c.name} for c in cats])
        st.dataframe(df, use_container_width=True)

def page_visao_geral(user: "User"):
    st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
    ref = month_selector()
    start, end = month_bounds(ref)
    with SessionLocal() as db:
        congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        ranking: List[Tuple[str, float]] = []
        for c in congs:
            tx_in = db.scalars(
                select(func.sum(Transaction.amount)).where(
                    Transaction.congregation_id == c.id,
                    Transaction.type == TYPE_IN,
                    Transaction.date >= start,
                    Transaction.date < end,
                )
            ).first() or 0.0
            tithes = db.scalars(
                select(func.sum(Tithe.amount)).where(
                    Tithe.congregation_id == c.id,
                    Tithe.date >= start,
                    Tithe.date < end,
                )
            ).first() or 0.0
            tx_out = db.scalars(
                select(func.sum(Transaction.amount)).where(
                    Transaction.congregation_id == c.id,
                    Transaction.type == TYPE_OUT,
                    Transaction.date >= start,
                    Transaction.date < end,
                )
            ).first() or 0.0
            total = float(tx_in) + float(tithes) - float(tx_out)
            ranking.append((c.name, total))

        ranking.sort(key=lambda x: x[1], reverse=True)

        st.metric("Saldo consolidado do mês", brl(sum(v for _, v in ranking)))
        st.write("")

        # Top 5 sem duplicações
        for pos, (nome, valor) in enumerate(ranking[:5], start=1):
            st.write(f"**{pos}º lugar** — {nome} — {brl(valor)}")

        st.write("")
        st.dataframe(
            pd.DataFrame([{"Congregação": n, "Total do mês": brl(v)} for n, v in ranking]),
            use_container_width=True,
        )


# ===================== NAV (modelo original: MENU NA SIDEBAR) =====================

NAV_ALL = [
    ("visao", "Visão Geral", page_visao_geral),
    ("lanc", "Lançamentos", page_lancamentos),
    ("ent", "Relatório de Entrada", page_relatorio_entrada),
    ("sai", "Relatório de Saída", page_relatorio_saida),
    ("diz", "Relatório de Dizimistas", page_relatorio_dizimistas),
    ("mis", "Relatório de Missões", page_relatorio_missoes),
    ("cad", "Cadastro", page_cadastro),
]

def allowed_nav_for(user: "User"):
    if user.role == "SEDE":
        return NAV_ALL
    if user.role == "TESOUREIRO":
        return [NAV_ALL[i] for i in [0, 1, 2, 3, 4]]
    if user.role == "TESOUREIRO MISSIONÁRIO":
        return [NAV_ALL[i] for i in [5, 0]]
    return [NAV_ALL[0]]

def nav_sidebar(user: "User") -> Tuple[str, callable]:
    nav = allowed_nav_for(user)
    # ler aba ativa de session/query params
    active = st.session_state.get("page") or _qp_get_first("p") or nav[0][0]

    with st.sidebar:
        st.markdown(f"**{user.username}**")
        # Menu no modelo original (radio)
        labels = [label for _, label, _ in nav]
        slugs = [slug for slug, _, _ in nav]
        try:
            idx = slugs.index(active)
        except ValueError:
            idx = 0
        choice = st.radio("Menu", labels, index=idx, key="menu_radio")
        new_slug = slugs[labels.index(choice)]
        if st.button("Sair"):
            do_logout()
        # salvar escolha
        if new_slug != active:
            active = new_slug

    # persistir a escolha
    st.session_state["page"] = active
    try:
        st.query_params["p"] = active
    except Exception:
        pass

    for slug, _, fn in nav:
        if slug == active:
            return slug, fn
    return nav[0][0], nav[0][2]


# ===================== MAIN =====================

def main():
    try:
        ensure_seed()
        hydrate_from_qp()

        user = current_user()
        if not user:
            login_ui()
            return

        _, page_fn = nav_sidebar(user)

        # Conteúdo principal
        page_fn(user)

    except Exception as e:
        st.error("Ocorreu um erro ao renderizar a aplicação.")
        st.exception(e)

if __name__ == "__main__":
    main()
