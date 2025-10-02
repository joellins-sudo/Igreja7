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

from sqlalchemy import (
    select, func, String, Date, Float, ForeignKey, 
    create_engine, and_, DateTime, Boolean  # <-- NOVOS TIPOS ADICIONADOS AQUI
)
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

# --- COLE AQUI: funções de suporte para "Todas as Congregações" e resumo IA ---

SUB_ALL = "__ALL__"

from sqlalchemy import select, func
from sqlalchemy.orm import joinedload

from sqlalchemy import select

def render_ai_context_selector(user, db, key_prefix: str = "ai"):
    """
    Renderiza o seletor de congregação para a página do Assistente IA.
    Retorna: (congregation_id_or_None_for_all, sub_cong_id_or_None, label_str)
    - Para usuários com role == "SEDE" mostra a opção "Todas as Congregações".
    - Para usuários não-SEDE retorna apenas a congregação do usuário (não permite All).
    Usa as funções já presentes no projeto: cong_options_for(user, db) quando existir.
    """
    # tenta obter lista de congregações usando sua função cong_options_for (se existir)
    try:
        congs = cong_options_for(user, db)  # presume que retorna lista de Congregation objects
    except Exception:
        # fallback simples: busca todas do DB
        try:
            congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        except Exception:
            congs = []

    # se for SEDE, adiciona opção "Todas as Congregações"
    if getattr(user, "role", None) == "SEDE":
        labels = ["Todas as Congregações"] + [c.name for c in congs]
        sel = st.selectbox("Congregação", labels, key=f"{key_prefix}_cong_sel")
        if sel == "Todas as Congregações":
            return (None, None, "Todas as Congregações")
        else:
            # encontra objeto congregação
            chosen = next((c for c in congs if c.name == sel), None)
            if chosen:
                return (chosen.id, None, chosen.name)
            else:
                return (None, None, sel)
    else:
        # não-SEDE: mostrar apenas a congregação do usuário (ou as permitidas)
        try:
            cong_obj = db.get(Congregation, user.congregation_id)
            label = cong_obj.name if cong_obj else "Sem congregação"
            st.markdown(f"**Congregação**: {label}")
            return (user.congregation_id, None, label)
        except Exception:
            # fallback: simples selector com nomes disponíveis (se houver)
            if congs:
                chosen = congs[0]
                st.markdown(f"**Congregação**: {chosen.name}")
                return (chosen.id, None, chosen.name)
            return (None, None, "—")



def _build_common_date_and_congreg_filters(model, start_date, end_date, cong_id=None, sub_cong_id=None):
    """
    Gera a lista de condições (WHERE) para as queries, suportando:
      - cong_id is None => NÃO filtra por congregation (todas)
      - sub_cong_id == SUB_ALL => NÃO filtra por sub (inclui todos)
      - sub_cong_id is None => filtra sub_congregation_id IS NULL (comportamento antigo)
      - sub_cong_id == <id> => filtra por esse sub
    """
    conds = [model.date >= start_date, model.date < end_date]
    if cong_id is not None:
        conds.append(model.congregation_id == cong_id)

    if hasattr(model, "sub_congregation_id"):
        if sub_cong_id == SUB_ALL:
            # não adiciona filtro por sub -> incluir todos
            pass
        elif sub_cong_id is None:
            conds.append(model.sub_congregation_id.is_(None))
        else:
            conds.append(model.sub_congregation_id == sub_cong_id)

    return conds


from sqlalchemy import select, func
from sqlalchemy.orm import joinedload

def summarize_financials_for_ai(db, start_date, end_date, cong_id=None, sub_cong_id=None):
    """
    Retorna dicionário com totais:
      - total_dizimos
      - total_ofertas_culto  (ServiceLog.oferta, EXCETO 'Culto de Missões')
      - total_ofertas_missoes (ServiceLog.oferta, APENAS 'Culto de Missões')
      - total_ofertas_transacoes (Transaction entradas cujo Category.name contém 'oferta')
      - by_payment_method: dict { 'Dinheiro': val, 'PIX': val, ... } para Tithe
    Aplica filtros de congregação/sub-congregação se fornecidos.
    """
    out = {
        "total_dizimos": 0.0,
        "total_ofertas_culto": 0.0,
        "total_ofertas_missoes": 0.0,
        "total_ofertas_transacoes": 0.0,
        "by_payment_method": {}
    }

    # filtros base
    filters_tithe = [Tithe.date >= start_date, Tithe.date < end_date]
    filters_log = [ServiceLog.date >= start_date, ServiceLog.date < end_date]
    filters_tx = [Transaction.date >= start_date, Transaction.date < end_date]

    if cong_id is not None:
        filters_tithe.append(Tithe.congregation_id == cong_id)
        filters_log.append(ServiceLog.congregation_id == cong_id)
        filters_tx.append(Transaction.congregation_id == cong_id)
    if sub_cong_id is not None:
        filters_tithe.append(Tithe.sub_congregation_id == sub_cong_id)
        filters_log.append(ServiceLog.sub_congregation_id == sub_cong_id)
        filters_tx.append(Transaction.sub_congregation_id == sub_cong_id)

    try:
        total_diz = float(db.scalar(
            select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(*filters_tithe)
        ) or 0.0)
        out["total_dizimos"] = round(total_diz, 2)
    except Exception:
        out["total_dizimos"] = 0.0

    # SERVICELOG: ofertas separadas (culto normal x culto de missões)
    try:
        total_ofe_missao = float(db.scalar(
            select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
                *filters_log, ServiceLog.service_type == "Culto de Missões"
            )
        ) or 0.0)
        out["total_ofertas_missoes"] = round(total_ofe_missao, 2)
    except Exception:
        out["total_ofertas_missoes"] = 0.0

    try:
        total_ofe_culto = float(db.scalar(
            select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
                *filters_log, ServiceLog.service_type != "Culto de Missões"
            )
        ) or 0.0)
        out["total_ofertas_culto"] = round(total_ofe_culto, 2)
    except Exception:
        out["total_ofertas_culto"] = 0.0

    # TRANSACTIONS: procurar entradas cuja categoria contenha 'oferta' (ou nome exato)
    try:
        # detecta constante TYPE_IN se existir, senão usa 'ENTRADA' como fallback
        try:
            tx_in_type = TYPE_IN
        except NameError:
            tx_in_type = "ENTRADA"

        tx_q = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
            *filters_tx,
            Transaction.type == tx_in_type,
            func.lower(Category.name).like("%oferta%")
        )
        total_ofe_tx = float(db.scalar(tx_q) or 0.0)
        out["total_ofertas_transacoes"] = round(total_ofe_tx, 2)
    except Exception:
        out["total_ofertas_transacoes"] = 0.0

    # Breakdown por forma de pagamento (para dizimos)
    try:
        pay_q = select(Tithe.payment_method, func.coalesce(func.sum(Tithe.amount), 0.0)).where(
            *filters_tithe
        ).group_by(Tithe.payment_method)
        rows = db.execute(pay_q).all()
        bypm = {}
        for pm, val in rows:
            key = pm or "Não informado"
            bypm[key] = float(val or 0.0)
        out["by_payment_method"] = bypm
    except Exception:
        out["by_payment_method"] = {}

    return out



from sqlalchemy import select, func, and_, or_, not_

def format_currency_br(value):
    """
    Formata número para R$ 1.234,56 (BR).
    Aceita None.
    """
    try:
        v = float(value or 0.0)
    except Exception:
        v = 0.0
    s = f"{v:,.2f}"            # 1,234.56
    # converter para formato brasileiro 1.234,56
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def format_ai_response(summary: dict, question: str = ""):
    """
    Gera um texto limpo e direto (sem listar fontes) para exibir no UI.
    Mantém as quantias separadas (ofertas de culto != ofertas de missões).
    """
    lines = []
    # Pergunta (opcional) — não fazemos explicações extras, apenas resultados
    if question:
        lines.append(f"Pergunta: {question.strip()}")
    # Resultados
    lines.append(f"Total Dízimos: {format_currency_br(summary.get('total_dizimos', 0.0))}")
    lines.append(f"Total Ofertas (Cultos): {format_currency_br(summary.get('total_ofertas_culto', 0.0))}")
    lines.append(f"Total Ofertas (Missões): {format_currency_br(summary.get('total_ofertas_missoes', 0.0))}")
    lines.append(f"Total Ofertas (Transações categoria 'Oferta'): {format_currency_br(summary.get('total_ofertas_transacoes', 0.0))}")
    return "\n".join(lines)


# === CSS do Login (SEI-like) — define se ainda não existir ===
# === CSS do Login (SEI-like) + LOGO IADRF! ===
# === CSS do Login (SEI-like) + LOGO IADRF! ===
# --- UI GLOBAL (botões azuis, inputs maiores) ---
GLOBAL_UI_CSS = """
<style>
  .stButton>button, .stDownloadButton>button,
  [data-testid="stFormSubmitButton"] button,
  button[data-testid="baseButton-primary"], button[data-testid="baseButton-secondary"]{
    background-color:#1d4ed8!important; border-color:#1d4ed8!important; color:#fff!important;
    box-shadow:none!important; border-radius:10px!important; height:48px; font-weight:700;
  }
  .stButton>button:hover, .stDownloadButton>button:hover,
  [data-testid="stFormSubmitButton"] button:hover,
  button[data-testid="baseButton-primary"]:hover, button[data-testid="baseButton-secondary"]:hover{
    background-color:#1e40af!important; border-color:#1e40af!important; color:#fff!important;
  }
  .stTextInput input, .stNumberInput input, .stDateInput input,
  .stSelectbox div[role="combobox"], .stTextArea textarea{
    min-height:44px; font-size:1rem;
  }
</style>
"""
def ui_global_bootstrap():
    try: st.markdown(GLOBAL_UI_CSS, unsafe_allow_html=True)
    except Exception: pass



ADRF_LOGIN_CSS = """
<style>
:root{
  --adrf-card:#ffffff; --adrf-border:#e5e7eb; --adrf-text:#0f172a; --adrf-muted:#64748b;
  --adrf-blue:#1d4ed8; --adrf-blue-dark:#1e40af; --adrf-bg:#f6f7fb; --adrf-green:#16a34a;
}
html, body { background: var(--adrf-bg) !important; }
.block-container { padding-top: 1.25rem; }

/* ---------- LOGO IADRF! ---------- */
.adrf-logo{
  width:100%;
  display:flex; justify-content:center; align-items:flex-end; gap:.1rem;
  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif;
  text-transform: uppercase;
  font-weight: 1000;
  letter-spacing:.02em;
  color: var(--adrf-blue);
  font-size: clamp(34px, 6vw, 56px);
  line-height: 0.95;
  margin: 6vh 0 10px;              /* antes estava 14vh; reduzimos para aparecer logo */
  text-shadow: 0 1px 0 rgba(0,0,0,.02);
}
.adrf-logo .bang{
  color: var(--adrf-green);
  transform: translateY(-2px);
}
.adrf-logo-sep{
  height:1px; width:100%; background:#e8ecf3; margin: 8px 0 18px;
}

/* ---------- Cartão do login ---------- */
.adrf-login-card{
  max-width: 1080px;              /* largura confortável como no seu print */
  margin: 0 auto 6vh auto;
  padding: 16px 18px; border-radius: 14px;
  border: 1px solid var(--adrf-border); background: var(--adrf-card);
  box-shadow: 0 8px 28px rgba(0,0,0,.06);
}
.adrf-login-title{ font-size:1.05rem; font-weight:800; color:var(--adrf-text); margin-bottom:.35rem; }
.adrf-login-sub{ color:var(--adrf-muted); margin-bottom:.6rem; }

/* Inputs ocupando largura e altura agradáveis */
.adrf-login-card [data-baseweb="input"] input,
.adrf-login-card [data-baseweb="select"] div[role="combobox"]{
  min-height:40px; font-size:.95rem;
}
.adrf-login-card .stTextInput,
.adrf-login-card .stSelectbox,
.adrf-login-card .stPassword{ margin-bottom:.65rem; }

/* Botão de entrar azul (coerente com o tema) */
.adrf-login-card [data-testid="stFormSubmitButton"] button{
  background-color: var(--adrf-blue) !important;
  border-color: var(--adrf-blue) !important;
  color:#fff !important; box-shadow:none !important; border-radius:10px !important;
  padding:.55rem .9rem !important; font-weight:700 !important;
}
.adrf-login-card [data-testid="stFormSubmitButton"] button:hover,
.adrf-login-card [data-testid="stFormSubmitButton"] button:focus{
  background-color: var(--adrf-blue-dark) !important;
  border-color: var(--adrf-blue-dark) !important;
  color:#fff !important;
}
</style>
"""


# SUBSTITUA SEU CSS DE BOTÕES ANTIGO POR ESTE
# ==== TODOS OS BOTÕES AZUIS (global) ====
BLUE_BUTTONS_CSS = """
<style>
/* Botões gerais (st.button / st.download_button) */
.stButton > button,
.stDownloadButton > button,
/* Botões de formulário (st.form_submit_button) */
[data-testid="stFormSubmitButton"] button,
/* Variantes internas do Streamlit */
button[data-testid="baseButton-primary"],
button[data-testid="baseButton-secondary"] {
  background-color: #1d4ed8 !important;  /* azul */
  border-color: #1d4ed8 !important;
  color: #ffffff !important;
  box-shadow: none !important;
}
.stButton > button:hover,
.stDownloadButton > button:hover,
[data-testid="stFormSubmitButton"] button:hover,
button[data-testid="baseButton-primary"]:hover,
button[data-testid="baseButton-secondary"]:hover {
  background-color: #1e40af !important;  /* azul (hover) */
  border-color: #1e40af !important;
  color: #ffffff !important;
}
/* Remove heranças de cores antigas por classes específicas */
.adrf-entrada [data-testid="stFormSubmitButton"] button,
.adrf-dizimo  [data-testid="stFormSubmitButton"] button,
.adrf-saida   [data-testid="stFormSubmitButton"] button {
  background-color: #1d4ed8 !important;
  border-color: #1d4ed8 !important;
  color: #ffffff !important;
}
.adrf-entrada [data-testid="stFormSubmitButton"] button:hover,
.adrf-dizimo  [data-testid="stFormSubmitButton"] button:hover,
.adrf-saida   [data-testid="stFormSubmitButton"] button:hover {
  background-color: #1e40af !important;
  border-color: #1e40af !important;
}
</style>
"""
st.markdown(BLUE_BUTTONS_CSS, unsafe_allow_html=True)

FONT_TUNING_CSS = """
<style>
/* ===== Escala base ===== */
html, body { font-size: 18px !important; }               /* aumenta a fonte padrão do app inteiro */
[data-testid="stSidebar"] { font-size: 16.5px !important; } /* aumenta a fonte apenas da Sidebar */

/* ===== Títulos ===== */
h1 { font-size: 2.1rem !important; font-weight: 800; }   /* tamanho do TÍTULO principal (st.title/markdown #) */
h2 { font-size: 1.6rem !important; font-weight: 700; }   /* tamanho de subtítulo (##) */
h3 { font-size: 1.25rem !important; font-weight: 700; }  /* tamanho de cabeçalho menor (###) */

/* ===== Parágrafos / texto de markdown ===== */
[data-testid="stMarkdownContainer"] p { font-size: 1.05rem !important; } /* texto comum em st.markdown */

/* ===== Rótulos (labels) dos campos ===== */
label p,                                                   /* rótulos genéricos */
.stTextInput label, .stSelectbox label,                    /* rótulos de texto e select */
.stDateInput label, .stNumberInput label {                 /* rótulos de data e número */
  font-size: 1rem !important;                              /* tamanho do texto dos rótulos */
}

/* ===== Conteúdo dentro dos inputs/selects ===== */
[data-baseweb="input"] input,                              /* texto digitado nos inputs */
[data-baseweb="select"] div[role="combobox"] {             /* texto exibido no select */
  font-size: 1rem !important;                              /* aumenta o texto interno dos campos */
}

/* ===== Placeholder (dicas dentro de inputs) ===== */
input::placeholder { font-size: 0.98rem !important; }      /* aumenta o placeholder “Usuário”, “Senha”, etc. */

/* ===== Botões ===== */
.stButton > button,                                        /* botões criados com st.button */
[data-testid="stFormSubmitButton"] button,                  /* botões de enviar formulário */
.stDownloadButton > button {                               /* botões de download */
  font-size: 0.98rem !important;                           /* aumenta o texto dos botões (mantém cor do seu tema) */
}

/* ===== Tabelas (dataframe/AgGrid envolvidas por .adrf-table-wrap) ===== */
.adrf-table-wrap td, .adrf-table-wrap th {                  /* células e cabeçalhos da tabela */
  font-size: 0.98rem !important;                            /* aumenta o texto dentro das tabelas */
}

/* ===== Logo do login (IADRF!) ===== */
.adrf-logo {                                               /* título grande do login */
  font-size: clamp(38px, 7vw, 64px) !important;            /* aumenta os limites mínimo/máximo do logo */
}
</style>
"""
st.markdown(FONT_TUNING_CSS, unsafe_allow_html=True)



# ===================== LOCALE (fallback) =====================
def _set_locale_ptbr():
    for loc in ("pt_BR.utf8", "pt_BR.UTF-8", "pt_BR", "Portuguese_Brazil.1252"):
        try:
            _locale.setlocale(_locale.LC_TIME, loc); return
        except Exception:
            continue
_set_locale_ptbr()

# ===================== UTILS =====================
# ===================== COMPONENTE DE ENTRADA DE VOZ =====================

MONTHS = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
MONTHS_SHORT = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

# ===================== FUNÇÃO DE IA PARA ASSISTENTE FINANCEIRO =====================
# ===================== FUNÇÃO DE IA PARA ASSISTENTE FINANCEIRO (MODELO RÁPIDO) =====================
# ===================== FUNÇÃO DE IA PARA ASSISTENTE FINANCEIRO (PERSONALIDADE CORRIGIDA) =====================
# ===================== FUNÇÃO DE IA PARA ASSISTENTE (PROMPT FINAL) =====================
@st.cache_data
def responder_pergunta_financeira(pergunta_usuario: str, dados_df: pd.DataFrame, contexto: str) -> str:
    """
    Mesma lógica anterior — corrigido apenas o formato das strings de saída:
    - evita junções estranhas (ex: "20Totaldesa...")
    - normaliza formatação de valores para "R$ 1.005,00"
    - mantém separação entre Ofertas do Culto / Ofertas (categoria) / Missões
    - não altera funcionalidades nem consultas ao DB
    """
    import os
    import re
    from datetime import date, timedelta, datetime
    try:
        from openai import OpenAI
    except Exception:
        OpenAI = None

    # helpers (assumem disponíveis globalmente: MONTHS, today_bahia, month_bounds, format_currency, _to_float_brl)
    
    
    def _get_colname(df, names):
        if df is None:
            return None
        cols = {c.lower(): c for c in df.columns}
        for n in names:
            if n.lower() in cols:
                return cols[n.lower()]
        return None

    def _col_exists(df, names):
        return _get_colname(df, names) is not None

    def _parse_year_month_from_context(ctx: str):
        if not ctx:
            return None, None
        mY = re.search(r"(20\d{2})", ctx)
        year = int(mY.group(1)) if mY else None
        meses_map = {m.lower(): i+1 for i, m in enumerate(MONTHS)}
        for name, idx in meses_map.items():
            if re.search(r"\b" + re.escape(name.lower()) + r"\b", ctx.lower()):
                return year or today_bahia().year, idx
        mm_yyyy = re.search(r"(\b0?[1-9]|1[0-2])[/\-](20\d{2})", ctx)
        if mm_yyyy:
            m = int(mm_yyyy.group(1)); y = int(mm_yyyy.group(2))
            return y, m
        y_m = re.search(r"(20\d{2})[-/](0?[1-9]|1[0-2])", ctx)
        if y_m:
            return int(y_m.group(1)), int(y_m.group(2))
        return (year, None)

    # formato seguro de moeda (usa format_currency se existir; fallback confiável BRL)
    def _fmt_val(v):
        try:
            # usa função global se existir e funcionar
            if 'format_currency' in globals() and callable(globals()['format_currency']):
                return globals()['format_currency'](v)
        except Exception:
            pass
        try:
            # fallback que garante ponto como milhares e vírgula como decimal: 1.234,56
            s = f"{float(v):,.2f}"  # ex: "1,234.56"
            s = s.replace(",", "X").replace(".", ",").replace("X", ".")
            return f"R$ {s}"
        except Exception:
            return f"R$ {v}"

    # Construir período
    year_ctx, month_ctx = _parse_year_month_from_context(contexto or "")
    if month_ctx is None or year_ctx is None:
        t = today_bahia()
        year_ctx = year_ctx or t.year
        month_ctx = month_ctx or t.month
    start = date(int(year_ctx), int(month_ctx), 1)
    _, end = month_bounds(start)

    # detectar se DataFrame já contém colunas úteis (apenas para compor resumo)
    resumo_items = []
    if dados_df is not None and not dados_df.empty:
        diz_col = _get_colname(dados_df, ["Dízimo", "Dizimo", "dizimo", "tithe", "tithes"])
        if diz_col:
            try:
                total_diz = float(dados_df[diz_col].dropna().map(_to_float_brl).sum() or 0.0)
                resumo_items.append(("dizimos_tab", total_diz))
            except Exception:
                pass
        of_col = _get_colname(dados_df, ["Oferta", "oferta", "ofertas", "offer"])
        if of_col:
            try:
                total_of = float(dados_df[of_col].dropna().map(_to_float_brl).sum() or 0.0)
                resumo_items.append(("ofertas_tab", total_of))
            except Exception:
                pass

    # buscar no DB — CALCULAR SEPARADO (mantive a mesma lógica)
    oferta_sl_total = 0.0        # Ofertas em ServiceLog.oferta (oferta do culto)
    oferta_tx_total = 0.0        # Ofertas em Transactions categoria 'Oferta'
    missao_tx_total = 0.0        # Ofertas em Transactions categoria 'Missões'
    try:
        with SessionLocal() as db:
            # ServiceLog.oferta
            q_sl = select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
                ServiceLog.date >= start, ServiceLog.date < end
            )
            # tenta inferir congregação do contexto (se aplicável)
            try:
                all_congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
                cong_id_candidate = None
                for c in all_congs:
                    if c.name and (c.name.lower() in (contexto or "").lower()):
                        cong_id_candidate = c.id
                        break
                if cong_id_candidate:
                    q_sl = q_sl.where(ServiceLog.congregation_id == cong_id_candidate)
            except Exception:
                cong_id_candidate = None
            oferta_sl_total = float(db.scalar(q_sl) or 0.0)

            # Transações categoria 'Oferta'
            cat_of = db.scalar(select(Category).where(func.lower(Category.name) == "oferta"))
            if not cat_of:
                cat_of = db.scalar(select(Category).where(func.lower(Category.name).in_(["oferta","ofertas"])))
            if cat_of:
                q_tx_of = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.category_id == cat_of.id,
                    Transaction.type.in_((TYPE_IN, "RECEITA"))
                )
                if cong_id_candidate:
                    q_tx_of = q_tx_of.where(Transaction.congregation_id == cong_id_candidate)
                oferta_tx_total = float(db.scalar(q_tx_of) or 0.0)
            else:
                oferta_tx_total = 0.0

            # Transações categoria 'Missões' (SEPARADO)
            cat_missoes = db.scalar(select(Category).where(func.lower(Category.name).in_(["missões","missoes","missões (entrada)","missoes (entrada)"])))
            if cat_missoes:
                q_tx_mis = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.category_id == cat_missoes.id,
                    Transaction.type.in_((TYPE_IN, "RECEITA"))
                )
                if cong_id_candidate:
                    q_tx_mis = q_tx_mis.where(Transaction.congregation_id == cong_id_candidate)
                missao_tx_total = float(db.scalar(q_tx_mis) or 0.0)
            else:
                missao_tx_total = 0.0
    except Exception:
        oferta_sl_total = oferta_sl_total or 0.0
        oferta_tx_total = oferta_tx_total or 0.0
        missao_tx_total = missao_tx_total or 0.0

    # montar instrução curta ao modelo (sem mencionar bases)
    prompt_sistema = (
        "Você é um assistente financeiro. Responda CURTO, PRÁTICO e DIRETO.\n"
        "- Não explique passos nem cite bases consultadas.\n"
        "- Não some ofertas de 'Culto' com ofertas de 'Missões'. Apresente cada uma separadamente.\n"
        "- Responda apenas ao pedido. Use formato R$ 1.234,56.\n"
    )

    # texto-resumo apresentado ao modelo (apenas para contexto interno; não afeta formato final local)
    texto_resumo_para_modelo = []
    for key, val in resumo_items:
        if key == "dizimos_tab":
            texto_resumo_para_modelo.append(f"Dízimos (tabela): {_fmt_val(val)}")
        if key == "ofertas_tab":
            texto_resumo_para_modelo.append(f"Ofertas (tabela): {_fmt_val(val)}")
    texto_resumo_para_modelo.append(f"Ofertas do Culto (ServiceLog): {_fmt_val(oferta_sl_total)}")
    texto_resumo_para_modelo.append(f"Ofertas (transações - categoria 'Oferta'): {_fmt_val(oferta_tx_total)}")
    texto_resumo_para_modelo.append(f"Ofertas Missões (transações - categoria 'Missões'): {_fmt_val(missao_tx_total)}")
    resumo_texto = "\n".join(texto_resumo_para_modelo)

    dados_texto = ""
    try:
        if dados_df is not None and not dados_df.empty:
            dados_texto = dados_df.head(200).to_markdown(index=False)
    except Exception:
        dados_texto = ""

    prompt_usuario_completo = (
        f"Contexto: {contexto}\n\n"
        f"Dados resumidos para o período ({MONTHS[month_ctx-1]} {year_ctx}):\n{resumo_texto}\n\n"
        f"Amostra (se houver):\n```markdown\n{dados_texto}\n```\n\n"
        f"Pergunta: {pergunta_usuario}\n\n"
        "INSTRUÇÃO: responda curto e apenas o que foi pedido. NÃO mencione as bases consultadas."
    )

    # Se OpenAI não disponível: gerar resposta local curta, com itens sempre separados
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        try:
            with SessionLocal() as db:
                # dízimos (mantive sua lógica)
                cat_diz = db.scalar(select(Category).where(func.lower(Category.name).in_(("dízimo","dizimo"))))
                q_diz_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.type.in_((TYPE_IN,"RECEITA"))
                )
                if cat_diz:
                    q_diz_tx = q_diz_tx.where(Transaction.category_id == cat_diz.id)
                total_diz_tx = float(db.scalar(q_diz_tx) or 0.0)
                q_tithe = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start, Tithe.date < end
                )
                total_tithe = float(db.scalar(q_tithe) or 0.0)
                total_diz_final = max(total_tithe, total_diz_tx)

                # saídas (excluindo missões)
                cat_miss_out = db.scalar(select(Category).where(func.lower(Category.name).in_(("missões","missoes")) , Category.type == TYPE_OUT))
                q_saidas = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.type.in_(("SAÍDA","DESPESA"))
                )
                if cat_miss_out:
                    q_saidas = q_saidas.where(Transaction.category_id != cat_miss_out.id)
                total_saidas = float(db.scalar(q_saidas) or 0.0)

                # dizimistas por forma
                q_pix = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start, Tithe.date < end,
                    func.upper(func.coalesce(Tithe.payment_method, "")) == "PIX"
                )
                total_pix = float(db.scalar(q_pix) or 0.0)
                q_cash = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start, Tithe.date < end,
                    func.upper(func.coalesce(Tithe.payment_method, "")) != "PIX"
                )
                total_cash = float(db.scalar(q_cash) or 0.0)

                qlow = pergunta_usuario.strip().lower()
                parts = []

                # quando o usuário pergunta por ofertas, apresentar as 3 linhas separadas
                if "oferta" in qlow or "ofertas" in qlow:
                    parts.append(f"Ofertas do Culto: {_fmt_val(oferta_sl_total)}")
                    parts.append(f"Ofertas (categoria 'Oferta'): {_fmt_val(oferta_tx_total)}")
                    parts.append(f"Ofertas Missões: {_fmt_val(missao_tx_total)}")

                if "dízimo" in qlow or "dizimo" in qlow:
                    parts.append(f"Dízimos (mês): {_fmt_val(total_diz_final)}")

                if "saída" in qlow or "saidas" in qlow or "saídas" in qlow:
                    parts.append(f"Saídas (exceto Missões) (mês): {_fmt_val(total_saidas)}")

                if "dizimistas" in qlow or "pix" in qlow or "dinheiro" in qlow:
                    parts.append(f"Dizimistas por forma: PIX {_fmt_val(total_pix)} • Outros {_fmt_val(total_cash)}")

                # fallback: lista limpa com itens-chave (sempre separados; nunca somados)
                if not parts:
                    parts = [
                        f"Dízimos (mês): {_fmt_val(total_diz_final)}",
                        f"Ofertas do Culto (mês): {_fmt_val(oferta_sl_total)}",
                        f"Ofertas Missões (mês): {_fmt_val(missao_tx_total)}",
                        f"Ofertas (categoria 'Oferta') (mês): {_fmt_val(oferta_tx_total)}",
                        f"Saídas (exceto Missões) (mês): {_fmt_val(total_saidas)}"
                    ]

                # garantir saída limpa e bem espaçada
                return "\n".join(f"- {p}" for p in parts)
        except Exception as e:
            return "Erro ao calcular localmente: " + str(e)

    # Se OpenAI disponível: chamar modelo com instruções estritas (formatação final do modelo será passada diretamente)
    try:
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": prompt_sistema},
                {"role": "user", "content": prompt_usuario_completo}
            ],
            temperature=0.0,
            max_tokens=400
        )
        text = resp.choices[0].message.content
        # limpar linhas em branco extras e garantir espaçamento correto
        lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip() != ""]
        # se o modelo retornar valores sem o formato R$, e quisermos reforçar, não alteramos: o modelo deve seguir instruções.
        return "\n".join(lines)
    except Exception:
        # fallback breve igual ao bloco local acima (garantir formatação)
        try:
            with SessionLocal() as db:
                cat_diz = db.scalar(select(Category).where(func.lower(Category.name).in_(("dízimo","dizimo"))))
                q_diz_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.type.in_((TYPE_IN,"RECEITA"))
                )
                if cat_diz:
                    q_diz_tx = q_diz_tx.where(Transaction.category_id == cat_diz.id)
                total_diz_tx = float(db.scalar(q_diz_tx) or 0.0)
                q_tithe = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start, Tithe.date < end
                )
                total_tithe = float(db.scalar(q_tithe) or 0.0)
                total_diz_final = max(total_tithe, total_diz_tx)

                cat_miss_out = db.scalar(select(Category).where(func.lower(Category.name).in_(("missões","missoes")) , Category.type == TYPE_OUT))
                q_saidas = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.type.in_(("SAÍDA","DESPESA"))
                )
                if cat_miss_out:
                    q_saidas = q_saidas.where(Transaction.category_id != cat_miss_out.id)
                total_saidas = float(db.scalar(q_saidas) or 0.0)

                q_pix = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start, Tithe.date < end,
                    func.upper(func.coalesce(Tithe.payment_method,"")) == "PIX"
                )
                total_pix = float(db.scalar(q_pix) or 0.0)
                q_cash = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start, Tithe.date < end,
                    func.upper(func.coalesce(Tithe.payment_method,"")) != "PIX"
                )
                total_cash = float(db.scalar(q_cash) or 0.0)

                parts = [
                    f"Dízimos (mês): {_fmt_val(total_diz_final)}",
                    f"Ofertas do Culto (mês): {_fmt_val(oferta_sl_total)}",
                    f"Ofertas Missões (mês): {_fmt_val(missao_tx_total)}",
                    f"Ofertas (categoria 'Oferta') (mês): {_fmt_val(oferta_tx_total)}",
                    f"Saídas (exceto Missões) (mês): {_fmt_val(total_saidas)}",
                    f"Dizimistas por forma: PIX {_fmt_val(total_pix)} • Outros {_fmt_val(total_cash)}"
                ]
                return "\n".join(f"- {p}" for p in parts)
        except Exception:
            return "Erro ao gerar resposta (IA/API indisponível e fallback falhou)."
def render_assistente_response(raw_text: str):
    """
    Renderiza a resposta da IA de forma limpa no Streamlit.
    Uso: render_assistente_response(resposta_da_ia)
    NÃO altera qualquer lógica de cálculo/IA — apenas pós-processa e exibe.
    """
    import re
    import html
    import streamlit as st
    if not raw_text:
        st.info("Sem resposta do assistente.")
        return

    # 1) Unescape HTML entities
    text = html.unescape(str(raw_text))

    # 2) Remove tags HTML que possam alterar a formatação (itálico, fontes estranhas, <font>, etc.)
    #    Mantemos apenas quebras de linha e caracteres textuais.
    text = re.sub(r"<\s*(br|br/)\s*>", "\n", text, flags=re.IGNORECASE)  # <br> -> newline
    text = re.sub(r"<\s*/?\s*(p|div|span|strong|b)[^>]*>", "", text, flags=re.IGNORECASE)  # remove wrappers simples
    # Remove tags problemáticas (i, em, font, style, etc.) e todo o resto de tags
    text = re.sub(r"<[^>]+>", "", text)

    # 3) Normalizações de espaço e pontuação
    # Remove espaços múltiplos e tabs
    text = re.sub(r"[ \t]{2,}", " ", text)
    # Remove espaços repetidos em quebras de linha
    text = re.sub(r"\n[ \t]+", "\n", text)
    # Coloca espaço depois de vírgula/ponto/ dois pontos quando faltam
    text = re.sub(r"([,;:])(?=[^\s0-9])", r"\1 ", text)
    # Garante espaço entre número e letra colados (ex: '362Total' -> '362 Total')
    text = re.sub(r"(?<=\d)(?=[A-Za-zÀ-ÿ])", " ", text)
    # Garante espaço entre letra e símbolo de R$ colado (ex: 'ofertasR$50' -> 'ofertas R$50')
    text = re.sub(r"(?<=[A-Za-zÀ-ÿ])(?=R\$)", " ", text)
    # Remove espaços antes de pontuação
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    # Normaliza quebras de linha múltiplas
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()

    # 4) Detecta linhas com marcadores e transforma em <ul><li> para melhor aparência
    lines = text.splitlines()
    html_lines = []
    in_list = False
    for ln in lines:
        stripped = ln.strip()
        if re.match(r"^[-\*\u2022]\s+", stripped):  # - * or bullet
            item = re.sub(r"^[-\*\u2022]\s+", "", stripped)
            if not in_list:
                html_lines.append("<ul style='margin:6px 0 6px 22px;padding:0;'>")
                in_list = True
            html_lines.append(f"<li style='margin:4px 0;line-height:1.5'>{html.escape(item)}</li>")
        else:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            # manter linhas em parágrafo curto
            if stripped == "":
                html_lines.append("<div style='height:8px'></div>")
            else:
                html_lines.append(f"<p style='margin:6px 0;line-height:1.5'>{html.escape(stripped)}</p>")
    if in_list:
        html_lines.append("</ul>")

    content_html = "\n".join(html_lines)

    # 5) Estilo do bloco (ajuste aqui se quiser outro visual)
    container_html = f"""
    <div style="
        background: #eaf6ff;
        border-radius: 10px;
        padding: 16px 20px;
        color: #0b4b71;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial;
        font-size: 16px;
        box-shadow: 0 1px 0 rgba(0,0,0,0.02);
        ">
        {content_html}
    </div>
    """

    # 6) Exibir com unsafe_allow_html (já sanitizamos acima)
    st.markdown(container_html, unsafe_allow_html=True)


def build_monthly_financial_summary_for_ai(year: int, month: int) -> Dict[str, Any]:
    """
    Retorna um dicionário com:
      - 'by_congregation': lista de dicts por congregação com chaves:
          'congregacao', 'tithe_nominal', 'tithe_tx', 'tithe_total',
          'oferta_sl', 'oferta_tx', 'oferta_total',
          'saidas_total_excl_missoes',
          'dizimistas_pix_count','dizimistas_pix_total',
          'dizimistas_other_count','dizimistas_other_total'
      - 'grand_totals': agregados para todo o conjunto
    Usa SessionLocal internamente.
    """
    from datetime import date
    from sqlalchemy import func, and_, or_

    start = date(year, month, 1)
    _, end = month_bounds(start)

    results = []
    grand = defaultdict(float)
    with SessionLocal() as db:
        congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        # Pre-lookup categorias relevantes
        cat_oferta = db.scalar(select(Category).where(func.lower(Category.name) == "oferta"))
        cat_dizimo = db.scalar(select(Category).where(func.lower(Category.name).in_(("dízimo","dizimo"))))
        # Missões de saída podem ter nome 'Missões (Saída)' ou conter 'missões' - vamos detectar pelos nomes
        missao_cat_ids_out = [c.id for c in db.scalars(select(Category).where(func.lower(Category.name).like("%miss%"), Category.type == TYPE_OUT)).all()]

        for c in congs:
            cong_id = c.id

            # TITHES: nominal
            q_tithe_nom = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                Tithe.congregation_id == cong_id, Tithe.date >= start, Tithe.date < end
            )
            tithe_nom = float(db.scalar(q_tithe_nom) or 0.0)

            # TITHES: transações categoria "Dízimo"
            tithe_tx = 0.0
            if cat_dizimo:
                q_tithe_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.congregation_id == cong_id,
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.type.in_((TYPE_IN, "RECEITA")),
                    Transaction.category_id == cat_dizimo.id
                )
                tithe_tx = float(db.scalar(q_tithe_tx) or 0.0)
            tithe_total = max(tithe_nom, tithe_tx)

            # OFERTA: ServiceLog.oferta
            q_of_sl = select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
                ServiceLog.congregation_id == cong_id,
                ServiceLog.date >= start, ServiceLog.date < end
            )
            of_sl = float(db.scalar(q_of_sl) or 0.0)

            # OFERTA: Transaction categoria Oferta
            of_tx = 0.0
            if cat_oferta:
                q_of_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.congregation_id == cong_id,
                    Transaction.date >= start, Transaction.date < end,
                    Transaction.type.in_((TYPE_IN, "RECEITA")),
                    Transaction.category_id == cat_oferta.id
                )
                of_tx = float(db.scalar(q_of_tx) or 0.0)
            of_total = max(of_sl, of_tx)

            # SAÍDAS: todas as saídas, menos as de Missões
            q_out = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                Transaction.congregation_id == cong_id,
                Transaction.date >= start, Transaction.date < end,
                Transaction.type.in_((TYPE_OUT, "DESPESA"))
            )
            # Excluir categorias de missões por id (se existirem)
            if missao_cat_ids_out:
                q_out = q_out.where(~Transaction.category_id.in_(missao_cat_ids_out))
            else:
                # fallback: excluir categorias cujo nome contenha 'miss'
                subq_miss_names = db.scalars(select(Category.id).where(func.lower(Category.name).like("%miss%"))).all()
                if subq_miss_names:
                    q_out = q_out.where(~Transaction.category_id.in_(list(subq_miss_names)))
            saidas_total = float(db.scalar(q_out) or 0.0)

            # DIZIMISTAS: distingue PIX vs Outros
            q_pix_sum = select(func.coalesce(func.sum(Tithe.amount), 0.0), func.count(Tithe.id)).where(
                Tithe.congregation_id == cong_id,
                Tithe.date >= start, Tithe.date < end,
                func.upper(func.coalesce(Tithe.payment_method, "")) == "PIX"
            )
            pix_sum, pix_count = db.execute(q_pix_sum).one()
            pix_sum = float(pix_sum or 0.0); pix_count = int(pix_count or 0)

            q_other_sum = select(func.coalesce(func.sum(Tithe.amount), 0.0), func.count(Tithe.id)).where(
                Tithe.congregation_id == cong_id,
                Tithe.date >= start, Tithe.date < end,
                func.coalesce(func.upper(Tithe.payment_method), "") != "PIX"
            )
            other_sum, other_count = db.execute(q_other_sum).one()
            other_sum = float(other_sum or 0.0); other_count = int(other_count or 0)

            results.append({
                "congregacao": c.name,
                "congregation_id": cong_id,
                "tithe_nominal": tithe_nom,
                "tithe_tx": tithe_tx,
                "tithe_total": tithe_total,
                "oferta_sl": of_sl,
                "oferta_tx": of_tx,
                "oferta_total": of_total,
                "saidas_total_excl_missoes": saidas_total,
                "dizimistas_pix_count": pix_count,
                "dizimistas_pix_total": pix_sum,
                "dizimistas_other_count": other_count,
                "dizimistas_other_total": other_sum
            })

            # acumula grand totals
            grand["tithe_nominal"] += tithe_nom
            grand["tithe_tx"] += tithe_tx
            grand["tithe_total"] += tithe_total
            grand["oferta_sl"] += of_sl
            grand["oferta_tx"] += of_tx
            grand["oferta_total"] += of_total
            grand["saidas_total_excl_missoes"] += saidas_total
            grand["dizimistas_pix_count"] += pix_count
            grand["dizimistas_pix_total"] += pix_sum
            grand["dizimistas_other_count"] += other_count
            grand["dizimistas_other_total"] += other_sum

    # formata saída
    return {"by_congregation": results, "grand_totals": dict(grand), "year": year, "month": month}

# imports necessários - adapte se seus nomes estiverem em outro módulo
from sqlalchemy import select, func
from sqlalchemy.orm import joinedload

# constantes (ajuste se o seu projeto usar valores diferentes)
TX_TYPE_IN = "ENTRADA"
TX_TYPE_OUT = "SAÍDA"

# Helper: formata moeda em pt-BR (R$ 1.234,56)

from sqlalchemy import select, func

# Soma Dízimo/Oferta do resumo (ServiceLog) para UMA unidade (principal ou sub)
# Observação IMPORTANTE: ofertas de 'Culto de Missões' FICAM FORA do fluxo operacional.
def _sum_servicelog_for_unit_operacional(db, cong_id: int, start, end, sub_cong_id: int | None):
    # Dízimo: conta normal
    q_diz = (
        select(func.coalesce(func.sum(ServiceLog.dizimo), 0.0))
        .where(
            ServiceLog.congregation_id == cong_id,
            ServiceLog.date >= start,
            ServiceLog.date < end,
            (ServiceLog.sub_congregation_id.is_(None)
             if sub_cong_id is None else ServiceLog.sub_congregation_id == sub_cong_id)
        )
    )
    diz = float(db.execute(q_diz).scalar_one() or 0.0)

    # Oferta: EXCLUI 'Culto de Missões'
    q_ofe = (
        select(func.coalesce(func.sum(ServiceLog.oferta), 0.0))
        .where(
            ServiceLog.congregation_id == cong_id,
            ServiceLog.date >= start,
            ServiceLog.date < end,
            ServiceLog.service_type != "Culto de Missões",   # <=== filtro crítico
            (ServiceLog.sub_congregation_id.is_(None)
             if sub_cong_id is None else ServiceLog.sub_congregation_id == sub_cong_id)
        )
    )
    ofe = float(db.execute(q_ofe).scalar_one() or 0.0)

    return diz, ofe


# Soma Dízimo/Oferta do resumo para TODO o ESCOPO (principal / sub específica / ALL)
def _sum_servicelog_scope_operacional(db, cong_id: int, start, end, sub_id_or_all):
    total_diz, total_ofe = 0.0, 0.0

    if sub_id_or_all == "ALL":
        # principal
        d, o = _sum_servicelog_for_unit_operacional(db, cong_id, start, end, None)
        total_diz += d; total_ofe += o
        # todas as subs
        subs = db.scalars(
            select(SubCongregation.id).where(SubCongregation.congregation_id == cong_id)
        ).all()
        for sid in subs:
            d, o = _sum_servicelog_for_unit_operacional(db, cong_id, start, end, sid)
            total_diz += d; total_ofe += o
    else:
        d, o = _sum_servicelog_for_unit_operacional(db, cong_id, start, end, sub_id_or_all)
        total_diz += d; total_ofe += o

    return total_diz, total_ofe


# ===== KPI OPERACIONAL (para cards do topo: Entradas / Saídas / Saldo) =====
@st.cache_data(ttl=600)
def get_kpi_operacional(cong_id: int, start, end, scope):
    with SessionLocal() as db:
        # 1) SAÍDAS (fluxo operacional)
        q_out = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type == TYPE_OUT
        )
        total_saida = float(db.execute(q_out).scalar_one() or 0.0)

        # 2) ENTRADAS OPERACIONAIS = Dízimo + Oferta (Oferta SEM Missões)
        #
        # Para evitar duplicidade de fontes, continue usando a sua regra de "maior fonte":
        # - Dízimo: max( Transações[Categoria=Dízimo], Nominal[Tithe] )
        # - Oferta: max( Transações[Categoria=Oferta], ServiceLog.oferta (exceto Missões) )
        #
        # 2a) Dízimos
        q_diz_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            func.lower(func.replace(Category.name, " ", "")) == "dizimo"
        )
        diz_tx = float(db.execute(q_diz_tx).scalar_one() or 0.0)

        q_diz_nom = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
            Tithe.congregation_id == cong_id,
            Tithe.date >= start, Tithe.date < end
        )
        diz_nom = float(db.execute(q_diz_nom).scalar_one() or 0.0)

        total_diz = max(diz_tx, diz_nom)

        # 2b) Ofertas
        q_ofe_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type.in_((TYPE_IN, "RECEITA")),
            func.lower(func.replace(Category.name, " ", "")) == "oferta"
        )
        ofe_tx = float(db.execute(q_ofe_tx).scalar_one() or 0.0)

        # ServiceLog.oferta OPERACIONAL (exclui Missões) no ESCOPO selecionado
        ofe_sl_diz, ofe_sl_ofe = _sum_servicelog_scope_operacional(db, cong_id, start, end, scope)
        # (a função já retorna separadinho: dizimo/oferta do ServiceLog)
        ofe_sl = ofe_sl_ofe

        total_ofe = max(ofe_tx, ofe_sl)

        entradas_total = total_diz + total_ofe
        saldo = entradas_total - total_saida

        return {
            "total_saida": total_saida,
            "total_oferta": total_ofe,
            "total_dizimo": total_diz,
            "total_entradas": entradas_total,
            "saldo": saldo,
        }



def format_currency(amount):
    """
    Recebe float/Decimal e retorna string no formato 'R$ 1.234,56'.
    Garante separadores corretos e 2 casas decimais.
    """
    try:
        a = float(amount or 0.0)
    except Exception:
        a = 0.0
    # arredonda para 2 decimais
    a = round(a + 0.0000001, 2)
    # separa parte inteira e decimal
    inteiro = int(abs(a))
    dec = int(round((abs(a) - inteiro) * 100))
    # formata milhares com "."
    inteiro_str = f"{inteiro:,}".replace(",", ".")
    sinal = "-" if a < 0 else ""
    return f"{sinal}R$ {inteiro_str},{dec:02d}"


def _build_common_date_and_congreg_filters(model, start_date, end_date, cong_id=None, sub_cong_id=None):
    """
    Retorna uma lista de condições reuseable: datas + congreg/subcong.
    Preserva a lógica antiga: se sub_cong_id is None -> filtra sub_congregation_id IS NULL.
    Se cong_id is None -> não filtra por congregation (ou seja, TODAS as congregações).
    """
    conds = [model.date >= start_date, model.date < end_date]
    if cong_id is not None:
        # filtra por congregação específica
        conds.append(model.congregation_id == cong_id)
    # mantém comportamento original: se sub_cong_id for None, requer sub_congregation_id IS NULL
    if hasattr(model, "sub_congregation_id"):
        if sub_cong_id is None:
            conds.append(model.sub_congregation_id.is_(None))
        else:
            conds.append(model.sub_congregation_id == sub_cong_id)
    return conds


def format_ai_response(summary, question=None):
    """
    Recebe o summary gerado por summarize_financials_for_ai e retorna UMA STRING limpa,
    com frases curtas e separadas, sem mencionar 'fontes' nem debug.
    Mantém cada tipo de valor separado (oferta culto vs missões).
    """
    lines = []
    # Se o usuário pediu algo específico, você pode usar question para ajustar, mas aqui
    # apenas formatamos tudo de forma direta e legível.
    lines.append(f"Ofertas do Culto: {format_currency(summary.get('total_ofertas_culto', 0.0))}.")
    lines.append(f"Ofertas de Missões: {format_currency(summary.get('total_ofertas_missoes', 0.0))}.")
    lines.append(f"Dízimos (total): {format_currency(summary.get('total_dizimos', 0.0))}.")
    # Saídas (excluindo Missões)
    lines.append(f"Saídas (excluindo Missões): {format_currency(summary.get('total_saidas_excl_missoes', 0.0))}.")

    # Pagamento — mostra apenas se houver valores
    tpb = summary.get("tithes_by_payment", {})
    if tpb:
        parts = []
        for pay, val in tpb.items():
            parts.append(f"{pay}: {format_currency(val)}")
        lines.append("Dízimos por forma de pagamento — " + "; ".join(parts) + ".")

    # Se quiser mostrar categorias de entrada / saída (opcional, curto)
    # Aqui deixamos opcional e curta: só mostra categorias de entrada importantes (maiores que zero)
    tx_in_cat = summary.get("tx_in_by_category", {})
    if tx_in_cat:
        # seleciona até 5 categorias com maior valor para não ficar longo
        items = sorted(tx_in_cat.items(), key=lambda kv: kv[1], reverse=True)
        shown = [f"{k}: {format_currency(v)}" for k, v in items[:5] if v > 0]
        if shown:
            lines.append("Entradas por categoria (top): " + "; ".join(shown) + ".")

    # Junta as linhas em um parágrafo com quebras de linha simples para exibição
    return "\n".join(lines)

def responder_pergunta_financeira_mes(year: int, month: int) -> str:
    """
    Gera e retorna um relatório textual (em PT-BR) com:
      - Tudo sobre DÍZIMOS (por congregação e total)
      - Tudo sobre OFERTAS (por congregação e total)
      - Tudo sobre SAÍDAS (por congregação e total) EXCLUINDO saídas de Missões
      - Tudo sobre DIZIMISTAS: contagem e soma por forma de pagamento (PIX vs outros)
    Se OPENAI_API_KEY estiver presente o texto será enviado ao modelo com instruções para
    formatar/explicar; senão, será retornado o relatório localmente formatado.
    """
    try:
        payload = build_monthly_financial_summary_for_ai(year, month)
    except Exception as e:
        return f"Erro ao coletar dados: {e}"

    # Monta um relatório textual bem organizado (fallback local)
    header = f"Relatório financeiro - {MONTHS[month-1]} de {year}\n\n"
    sections = []

    # GRAND TOTALS
    g = payload["grand_totals"]
    sec_grand = [
        "### Totais Consolidados (todas as congregações)",
        f"- Dízimos (nominal total): {format_currency(g.get('tithe_nominal',0))}",
        f"- Dízimos (transações): {format_currency(g.get('tithe_tx',0))}",
        f"- Dízimos (usamos MAIOR entre nominal e transações por unidade): {format_currency(g.get('tithe_total',0))}",
        f"- Ofertas (ResumoCulto): {format_currency(g.get('oferta_sl',0))}",
        f"- Ofertas (Transações): {format_currency(g.get('oferta_tx',0))}",
        f"- Ofertas (usamos MAIOR entre ResumoCulto e Transações por unidade): {format_currency(g.get('oferta_total',0))}",
        f"- Saídas totais (excl. Missões): {format_currency(g.get('saidas_total_excl_missoes',0))}",
        f"- Dizimistas por PIX: {int(g.get('dizimistas_pix_count',0))} registros — total {format_currency(g.get('dizimistas_pix_total',0))}",
        f"- Dizimistas por Dinheiro/Outros: {int(g.get('dizimistas_other_count',0))} registros — total {format_currency(g.get('dizimistas_other_total',0))}",
    ]
    sections.append("\n".join(sec_grand))

    # POR CONGREGAÇÃO (lista com detalhes)
    lines = ["### Detalhamento por congregação"]
    for r in sorted(payload["by_congregation"], key=lambda x: _norm(x["congregacao"])):
        lines.append(f"\n**{r['congregacao']}**:")
        lines.append(f"  - Dízimo (nominal): {format_currency(r['tithe_nominal'])}")
        lines.append(f"  - Dízimo (transações): {format_currency(r['tithe_tx'])}")
        lines.append(f"  - Dízimo final (MAIOR): {format_currency(r['tithe_total'])}")
        lines.append(f"  - Oferta (ResumoCulto): {format_currency(r['oferta_sl'])}")
        lines.append(f"  - Oferta (Transações): {format_currency(r['oferta_tx'])}")
        lines.append(f"  - Oferta final (MAIOR): {format_currency(r['oferta_total'])}")
        lines.append(f"  - Saídas (excl. Missões): {format_currency(r['saidas_total_excl_missoes'])}")
        lines.append(f"  - Dizimistas PIX: {int(r['dizimistas_pix_count'])} → {format_currency(r['dizimistas_pix_total'])}")
        lines.append(f"  - Dizimistas Outros: {int(r['dizimistas_other_count'])} → {format_currency(r['dizimistas_other_total'])}")

    sections.append("\n".join(lines))

    report_text = header + "\n\n".join(sections)

    # Se houver OPENAI_API_KEY, envie para o modelo pedindo formatação/resumo executivo
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        # Fallback: retornar relatório local
        return report_text

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        system = (
            "Você é um assistente financeiro que fornece relatórios concisos e bem estruturados.\n"
            "Tarefa: usando os dados fornecidos, gere uma resposta com 4 seções claramente marcadas:\n"
            "  1) DÍZIMOS — explicando a regra de equivalência e mostrando totals por congregação e consolidado.\n"
            "  2) OFERTAS — explicar as duas fontes (ResumoCulto vs Transações) e mostrar totals (usar MAIOR por unidade antes do agregado).\n"
            "  3) SAÍDAS (EXCETO MISSÕES) — fornecer total por congregação e consolidado; enfatizar que saídas de MISSÕES foram excluídas.\n"
            "  4) DIZIMISTAS — separar PIX vs outros (contagem + total) e listar comportamentos relevantes.\n"
            "Formate com bullets e tabelas simples em markdown; sempre explique que fontes foram usadas (ServiceLog vs Transaction vs Tithe).\n"
            "Se algum valor for zero ou inexistente, indique claramente 'não consta'.\n"
        )
        user_prompt = f"Dados (resumo já agregado por congregação):\n\n{report_text}\n\nPor favor, produza a saída pedida."
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role":"system","content":system},{"role":"user","content":user_prompt}],
            temperature=0.0,
            max_tokens=800
        )
        return resp.choices[0].message.content
    except Exception as e:
        # Em caso de erro com a API, retornar o relatório bruto
        return report_text + f"\n\n(Erro ao chamar OpenAI: {e})"
        



def now_bahia():
    """
    Retorna datetime.now() seguro — usa datetime.datetime.now() para evitar
    o AttributeError quando 'datetime' foi importado como módulo.
    Se quiser validar timezone mais tarde, podemos ajustar aqui.
    """
    # se você quiser usar timezone fixa, podemos alterar aqui; por enquanto
    # retornamos a hora local do servidor.
    return datetime.now()

def today_bahia():
    """
    Retorna a data (date) atual baseada em now_bahia().
    """
    return now_bahia().date()

# NOVO HELPER: Função genérica para limpar campos
# NOVO HELPER:

def _process_dizimos_lote_callback(
    dizimos_texto: str, 
    default_payment: str, 
    rap_data: date, 
    target_cong_obj: Any, 
    target_sub_cong_id: Any
):
    """
    Callback para processar o lote de dizimos.
    A lógica é a mesma de antes, mas agora é chamada APENAS no final.
    """
    
    # A lógica de parsing é a mesma, mas agora usa _parse_lote_dizimos para garantir consistência
    registros, erros_parse = _parse_lote_dizimos(dizimos_texto)

    erros_db, sucessos = list(erros_parse), 0
    
    # Se houver erros de parsing, pare aqui
    if erros_parse:
        st.session_state.status_message = ("error", "❌ Erros de formato: " + " | ".join(erros_parse))
        return

    for i, reg in enumerate(registros):
        valor_float = reg["Valor"]
        nome_dizimista = reg["Nome"]
        
        with SessionLocal() as db_batch:
            try:
                db_batch.add(Tithe(
                    date=rap_data, tither_name=nome_dizimista, amount=valor_float,
                    congregation_id=target_cong_obj.id, sub_congregation_id=target_sub_cong_id,
                    payment_method=default_payment 
                ))
                db_batch.commit(); sucessos += 1
                
            except Exception as e:
                db_batch.rollback(); erros_db.append(f"Erro inesperado no registro '{nome_dizimista}': {str(e)}")

    # Feedback
    if sucessos > 0: 
        st.session_state.status_message = ("success", f"✅ {sucessos} dízimos registrados com sucesso.")
    if erros_db: 
        st.session_state.status_message = ("error", "❌ Erros encontrados: " + " | ".join(erros_db))

    # Importante: NÃO chame st.rerun() ou st.cache_data.clear() aqui, pois o chamador já o fará.

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

# === AVISO VISUAL PARA CULTO DE MISSÕES (apenas UI, sem alterar dados) ===
import re

def _has_culto_missoes_in_df(df: pd.DataFrame) -> bool:
    """True se existir 'Culto de Missões' na coluna 'Tipo de Culto'."""
    try:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return False
        cols_lc = {c.lower(): c for c in df.columns}
        key = cols_lc.get("tipo de culto") or cols_lc.get("tipo")
        if not key:
            return False
        rx = re.compile(r'\bmiss(ões|oes)\b', flags=re.IGNORECASE)
        return df[key].astype(str).str.contains(rx, na=False).any()
    except Exception:
        return False

def _render_aviso_missoes_inline():
    """Aviso amarelo em UMA LINHA (acima da tabela)."""
    st.markdown("""
    <style>
      .inline-missoes-alert{
        background:#fff3cd;           /* amarelo suave */
        border:1px solid #ffeeba;     /* borda amarela */
        color:#856404;                /* texto amarelo-escuro */
        padding:6px 10px; border-radius:8px;
        margin:8px 0 10px;
        white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
        font-size:.95rem;
      }
    </style>
    """, unsafe_allow_html=True)
    st.markdown(
        "<div class='inline-missoes-alert'>⚠️ "
        "Atenção : As ofertas do culto de missões são lançadas automaticamente no "
        "Menu Relatório de Missões ao lado.</div>",
        unsafe_allow_html=True
    )


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
    
    # Adicione esta classe junto com User, Congregation, Transaction, etc.
class InternalMessage(Base):
    __tablename__ = "internal_messages"
    id: Mapped[int] = mapped_column(primary_key=True)
    sender_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    target_congregation_id: Mapped[int] = mapped_column(ForeignKey("congregations.id"))
    date_sent: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    message_text: Mapped[str] = mapped_column(String(500))
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    
    target_congregation: Mapped["Congregation"] = relationship(foreign_keys=[target_congregation_id])
    sender: Mapped["User"] = relationship(foreign_keys=[sender_user_id])

# ===================== ENGINE / SESSION =====================
@st.cache_resource
def get_engine():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        # ALTERE ESTA LINHA:
        db_url = "sqlite:///database.db"
        
        # PARA ESTA LINHA: (usando um caminho absoluto simples)
        # Note: 'sqlite:///' + caminho absoluto/database.db
        
        # Você não precisa mudar nada, pois "sqlite:///database.db" já se refere ao diretório
        # de trabalho se o caminho não for absoluto.

        # Se o problema persistir, TENTE FORÇAR um caminho no diretório de trabalho:
        # Substitua a linha 1 pela linha 2, garantindo que o arquivo será criado.
        # Linha 1: db_url = "sqlite:///database.db"
        
        # db_url = f"sqlite:///{os.path.abspath('database.db')}" # Com essa linha, ele força
                                                                # o caminho absoluto

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
APP_SECRET = os.environ.get("APP_SECRET") or "troque-esta-chave"
INACTIVITY_MINUTES = int(os.environ.get("INACTIVITY_MINUTES", 20))

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

# === Função COMPLETA do login com o logo IADRF! ===
# --- HELPER de verificação de senha (hash bcrypt ou texto puro para ambiente de dev) ---
# --- helper para verificar a senha (bcrypt se houver hash, senão comparação simples) ---
# --- substituir sua verify_password por esta versão tolerante ---
def verify_password(password: str, stored_hash: str) -> bool:
    """
    Aceita:
      - PBKDF2 no formato 'salt_hex:hash_hex'
      - bcrypt ($2a$/$2b$/$2y$) se a lib passlib estiver instalada
      - texto puro (fallback; útil em bases antigas de dev)
    """
    import hashlib, hmac

    if not stored_hash:
        return False

    s = str(stored_hash)

    # 1) bcrypt
    if s.startswith("$2a$") or s.startswith("$2b$") or s.startswith("$2y$"):
        try:
            from passlib.hash import bcrypt
            return bcrypt.verify(password, s)
        except Exception:
            # se passlib não estiver instalada, não trava o login;
            # cai para os demais métodos (vai falhar de propósito aqui).
            pass

    # 2) PBKDF2: "salt_hex:hash_hex"
    if ":" in s:
        try:
            salt_hex, pwdhash_hex = s.split(":", 1)
            salt = bytes.fromhex(salt_hex)
            expected = bytes.fromhex(pwdhash_hex)
            calc = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
            return hmac.compare_digest(calc, expected)
        except Exception:
            return False

    # 3) Fallback: texto puro (para dados legados de dev)
    return password == s



# =============== LOGIN UI (compatível com seu main que usa st.session_state.uid) ===============
def login_ui():
    import streamlit as st
    from sqlalchemy import or_

    # injeta CSS/Logo se existirem
    try:
        st.markdown(ADRF_LOGIN_CSS, unsafe_allow_html=True)
    except NameError:
        pass
    st.markdown('<div class="adrf-logo">IADRF<span class="bang">!</span></div>', unsafe_allow_html=True)
    st.markdown('<div class="adrf-logo-sep"></div>', unsafe_allow_html=True)

    # Evita colisão de chaves com outros inputs da página
    USER_KEY = "login_user_main_fix"
    PWD_KEY  = "login_pwd_main_fix"

    # Mensagem persistente
    st.session_state.setdefault("auth_msg", "")

    # ---------- Cartão ----------
    st.markdown('<div class="adrf-login-card">', unsafe_allow_html=True)

    with st.form("form_login"):
        user_in = st.text_input("Usuário", key=USER_KEY)
        pwd_in  = st.text_input("Senha", type="password", key=PWD_KEY)
        ok      = st.form_submit_button("Acessar")

        if ok:
            u = (st.session_state.get(USER_KEY) or "").strip()
            p = (st.session_state.get(PWD_KEY)  or "").strip()

            if not u or not p:
                st.session_state.auth_msg = "Informe usuário e senha."
            else:
                try:
                    # ===== Busca do usuário sem usar 'email' =====
                    with SessionLocal() as db:
                        # Aceita 'username' e/ou 'login', conforme existir no modelo
                        filters = []
                        if hasattr(User, "username"): filters.append(User.username == u)
                        if hasattr(User, "login"):    filters.append(User.login    == u)

                        if not filters:
                            st.session_state.auth_msg = (
                                "Modelo User sem campos de login reconhecidos (esperado: 'username' ou 'login')."
                            )
                        else:
                            q = db.query(User)
                            q = q.filter(filters[0] if len(filters) == 1 else or_(*filters))
                            user = q.first()

                            if not user:
                                st.session_state.auth_msg = "Usuário não encontrado."
                            else:
                                # ===== Descobre o campo de senha disponível =====
                                pwd_fields_try = ("password_hash", "senha_hash", "password", "senha")
                                stored = None
                                for f in pwd_fields_try:
                                    if hasattr(user, f):
                                        stored = getattr(user, f)
                                        if stored is not None:
                                            break

                                if stored is None:
                                    st.session_state.auth_msg = "Campo de senha não encontrado no modelo User."
                                elif not verify_password(p, stored):
                                    st.session_state.auth_msg = "Senha inválida."
                                else:
                                    # ===== SUCESSO: seta UID (é isso que teu main() verifica) =====
                                    st.session_state.uid = user.id

                                    # Nome exibido (opcional)
                                    name_try = ("name", "full_name", "username", "login")
                                    shown = None
                                    for f in name_try:
                                        if hasattr(user, f):
                                            shown = getattr(user, f) or shown
                                    st.session_state.current_user = shown or u

                                    st.session_state.auth_msg = ""
                                    st.rerun()

                except Exception as e:
                    st.session_state.auth_msg = f"Erro ao autenticar: {e}"

    if st.session_state.auth_msg:
        st.warning(st.session_state.auth_msg)

    st.markdown("</div>", unsafe_allow_html=True)





# ===================== HELPERS =====================

# --------- Parser BR para valores e nomes (tolerante a "R$ 1.234,56") ----------
import re
_CURRENCY_RE = re.compile(r"(?<!\d)(?:R\$\s*)?([0-9]{1,3}(?:[.\s][0-9]{3})*|[0-9]+)(?:,([0-9]{1,2}))?(?!\d)")

import pandas as pd
import re

# ====== helpers (mantêm os seus; incluo aqui para ficar autocontido) ======
_CURRENCY_RE = re.compile(r"(?<!\d)(?:R\$\s*)?([0-9]{1,3}(?:[.\s][0-9]{3})*|[0-9]+)(?:,([0-9]{1,2}))?(?!\d)")

import pandas as pd
import re

_CURRENCY_RE = re.compile(r"(?<!\d)(?:R\$\s*)?([0-9]{1,3}(?:[.\s][0-9]{3})*|[0-9]+)(?:,([0-9]{1,2}))?(?!\d)")

import re
import pandas as pd

# ----------------- helpers de parsing/format -----------------
_CURRENCY_RE = re.compile(r"(?<!\d)(?:R\$\s*)?([0-9]{1,3}(?:[.\s][0-9]{3})*|[0-9]+)(?:,([0-9]{1,2}))?(?!\d)")

import re
import pandas as pd

# -------- helpers de parsing/format (podem estar em outro lugar; deixei aqui autocontido) --------
_CURRENCY_RE = re.compile(r"(?<!\d)(?:R\$\s*)?([0-9]{1,3}(?:[.\s][0-9]{3})*|[0-9]+)(?:,([0-9]{1,2}))?(?!\d)")



# ----------------- seção com confirmação em 2 etapas + TABELA EDITÁVEL -----------------
# ----------------- seção com confirmação em 2 etapas + TABELA EDITÁVEL -----------------

# ===================== HELPERS: LOTE DE DÍZIMOS =====================
# Estes helpers são essenciais para o parsing e conversão do lote
# e devem estar definidos antes de serem usados.

def _fmt_brl(v: float) -> str:
    """
    Formata float para string com separador de decimal (,) e milhar (.)
    (apenas números, sem o "R$") para remontar o texto de lote.
    """
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s

def _rows_to_text_br(rows: List[Dict[str, Any]]) -> str:
    """
    Transforma uma lista de dicionários [{"Nome":..., "Valor": 12.34}]
    em uma string formatada como lote: "Nome 12,34" por linha.
    """
    linhas = []
    for r in rows:
        nome = str(r.get("Nome", "")).strip()
        valor = float(r.get("Valor", 0.0) or 0.0)
        # Usa _fmt_brl para garantir o separador de milhar/decimal correto.
        linhas.append(f"{nome} {_fmt_brl(valor)}")
    return "\n".join(linhas)

def _br_to_float(s: str) -> float:
    """
    Converte string de moeda BR (1.234,56) ou simples (500) para float.
    Usa a regex _CURRENCY_RE.
    """
    s = (s or "").strip()
    if not s:
        return 0.0
    # Presume que _CURRENCY_RE está definido em outro lugar, se não estiver, esta função
    # pode quebrar, mas é a versão que você estava usando para análise.
    m = _CURRENCY_RE.search(s.replace("\u00A0", " "))
    if not m:
        return 0.0
    inteiro = (m.group(1) or "").replace(".", "").replace(" ", "")
    frac = (m.group(2) or "0")
    try:
        # Tenta a conversão limpa da regex
        return float(f"{int(inteiro)}.{int(frac):02d}")
    except Exception:
        # Fallback para tentar uma conversão mais suja se a regex falhar
        try:
            return float(s.replace("R$", "").replace(".", "").replace(",", ".").strip() or 0.0)
        except Exception:
            return 0.0

def _parse_lote_dizimos(texto: str):
    """
    Converte o texto bruto do lote em lista de dicionários [{Nome: str, Valor: float}].
    """
    registros, erros = [], []
    if not (texto and texto.strip()):
        return registros, erros
    linhas = [ln.strip() for ln in texto.splitlines() if ln.strip()]
    for i, ln in enumerate(linhas, start=1):
        try:
            valor = _br_to_float(ln)
            if valor <= 0:
                erros.append(f"Linha {i}: valor não encontrado (> 0).")
                continue
            # Note: _CURRENCY_RE deve estar definido fora dessa função.
            m = _CURRENCY_RE.search(ln.replace("\u00A0", " "))
            
            # Tenta isolar o nome removendo a parte do valor
            if m:
                nome = (ln[:m.start()] + " " + ln[m.end():]) 
            else: 
                nome = ln
                
            nome = re.sub(r"[,\|;]+", " ", nome).strip()
            nome = re.sub(r"\s{2,}", " ", nome) or "Dizimista"
            
            registros.append({"Nome": nome, "Valor": float(valor)})
        except Exception:
            erros.append(f"Linha {i}: formato inválido.")
    return registros, erros
# ============================================================


# ----------------- seção com confirmação em 2 etapas + TABELA EDITÁVEL -----------------
# ----------------- seção com confirmação em 2 etapas + TABELA EDITÁVEL -----------------
def render_dizimos_lote_section(default_payment: str, rap_data, target_cong_obj, target_sub_cong_id):
    """
    Etapa 1: Digita -> 'Avançar para confirmação' (limpa texto e cria _lote_pending).
    Etapa 2: Mostra st.data_editor editável; 'Confirmar Lote' salva com dados EDITADOS; 'Cancelar' descarta.
    """

    st.markdown("##### 2. Lançar Dízimos Nominais em Lote (Entrada Livre)")

    # Estado controlado do textarea (evita conflito de key)
    if "lote_text" not in st.session_state:
        st.session_state["lote_text"] = ""

    def _sync_lote_text():
        # AQUI GARANTIMOS QUE O ESTADO REAL RECEBE O VALOR DO WIDGET
        st.session_state["lote_text"] = st.session_state.get("rap_dizimo_lote_input", "")

    # Se já existe um pendente, mostramos a TABELA EDITÁVEL (Etapa 2)
    pending = st.session_state.get("_lote_pending")
    if pending:
        st.info("Confira e edite os lançamentos antes de confirmar.")
        df_pending = pd.DataFrame(pending["rows"])
        if df_pending.empty:
            st.warning("Nenhum item no lote.")
            st.session_state.pop("_lote_pending", None)
            st.rerun()

        # Data editor EDITÁVEL (adicionar/remover/alterar)
        edited_df = st.data_editor(
            df_pending,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="lote_pending_editor",
            column_config={
                "Nome": st.column_config.TextColumn("Nome", required=True),
                # CORREÇÃO FINAL: Configura a coluna para aceitar vírgula (padrão BRL)
                "Valor": st.column_config.NumberColumn(
                    "Valor (R$)", 
                    format="R$ %.2f",           # Formato BRL para exibição
                    required=True, 
                    step=0.01,                  # Permite editar centavos
                    min_value=0.0
                )
            },
            column_order=["Nome", "Valor"]
        )

        # Métricas após edição
        try:
            # st.data_editor retorna o valor como float (já convertido do input com vírgula)
            total = float(edited_df["Valor"].sum())
            qtd = int(len(edited_df))
        except Exception:
            total, qtd = 0.0, 0
        c1, c2 = st.columns(2)
        c1.metric("Registros", str(qtd))
        # CORREÇÃO: Usa format_currency (função global)
        c2.metric("Total (editado)", format_currency(total))

        colC, colD = st.columns(2)
        if colC.button("✅ Confirmar Lote", type="primary", use_container_width=True, key="btn_confirmar_lote"):
            try:
                # 1) Converte linhas EDITADAS para texto BR e chama o callback
                rows = edited_df.to_dict(orient="records")
                # CORREÇÃO: _rows_to_text_br deve ser usado para formatar o lote
                texto_confirmado = _rows_to_text_br(rows) 
                
                # --- AQUI VAI O CALL BACK FINAL (SALVAR) ---
                _process_dizimos_lote_callback(
                    texto_confirmado,
                    default_payment,
                    rap_data,
                    target_cong_obj,
                    target_sub_cong_id
                )
                # --- FIM DO CALL BACK FINAL ---

                # 2) Guarda o último lote salvo (já editado)
                st.session_state["_ultimo_lote_salvo"] = rows
                # 3) Limpa o pendente e reroda
                st.session_state.pop("_lote_pending", None)
                st.cache_data.clear() 
                st.success("Lote salvo com sucesso!")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar o lote: {e}")

        if colD.button("❌ Cancelar", use_container_width=True, key="btn_cancelar_lote"):
            st.session_state.pop("_lote_pending", None)
            st.info("Lote cancelado.")
            st.rerun()

        st.divider()
        return  # não mostra a etapa 1 quando há pendente

    # --- ETAPA 1: Entrada de texto ---
    st.text_area(
        "Insira um dízimo por linha (Ex: João Silva 500,00 | Adriana Pereira 50 | Pedro Souza 25,30)",
        height=200,
        key="rap_dizimo_lote_input",
        value=st.session_state["lote_text"],
        on_change=_sync_lote_text
    )

    # ------- ETAPA 1: ações quando NÃO há pendente -------
    col1, col2 = st.columns(2)
    avancar = col1.button("Avançar para confirmação", type="primary", use_container_width=True, key="btn_avancar_lote")
    limpar  = col2.button("Limpar texto", use_container_width=True, key="btn_limpar_lote")

    if limpar:
        # Altera apenas a chave 'lote_text' (que é usada como value)
        st.session_state["lote_text"] = ""
        st.rerun()

    if avancar:
        texto = st.session_state["lote_text"]
        regs, erros = _parse_lote_dizimos(texto)
        if not regs:
            st.error("Nenhum dízimo válido encontrado. Verifique o texto e tente novamente.")
        else:
            # Guarda lote pendente
            st.session_state["_lote_pending"] = {"rows": regs}
            # Limpa o estado 'lote_text' para que a área de texto fique vazia após o rerun
            st.session_state["lote_text"] = ""
            st.rerun()

    # Exibe o último lote efetivamente salvo (apenas referência)
    if st.session_state.get("_ultimo_lote_salvo"):
        st.caption("Último lote salvo:")
        df_conf = pd.DataFrame(st.session_state["_ultimo_lote_salvo"])
        if not df_conf.empty:
            df_show = df_conf.copy()
            df_show["Valor (R$)"] = df_show["Valor"].map(lambda v: format_currency(float(v or 0.0)))
            df_show = df_show[["Nome", "Valor (R$)"]]
            st.dataframe(df_show, use_container_width=True, hide_index=True)
            
            
            
# =============== GUARDA PARA EVITAR SALVAR DIRETO (Lote de Dízimos) ===============
def install_lote_guard():
    """
    Re-encapa _process_dizimos_lote_callback para só salvar quando
    st.session_state['_allow_save_lote'] == True.
    Bloqueia qualquer chamada direta (ex.: botões antigos).
    """
    import streamlit as st
    g = globals()
    if g.get("_LoteGuardInstalled"):
        return  # já instalado

    orig = g.get("_process_dizimos_lote_callback")
    if not callable(orig):
        # não existe o callback ainda; apenas marca que tentou
        g["_LoteGuardInstalled"] = True
        return

    g["_orig_process_dizimos_cb"] = orig

    def _guarded_process_dizimos_lote_callback(*args, **kwargs):
        # Só permite quando a confirmação explícita habilitou
        if not st.session_state.get("_allow_save_lote", False):
            # Opcional: aviso suave para rastrear chamadas indevidas
            # st.info("Aguardando confirmação do lote.")
            return
        return g["_orig_process_dizimos_cb"](*args, **kwargs)

    g["_process_dizimos_lote_callback"] = _guarded_process_dizimos_lote_callback
    g["_LoteGuardInstalled"] = True







import re
import pandas as pd
import streamlit as st

_CURRENCY_RE = re.compile(r"(?<!\d)(?:R\$\s*)?([0-9]{1,3}(?:[.\s][0-9]{3})*|[0-9]+)(?:,([0-9]{1,2}))?(?!\d)")


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


def check_unread_messages(user: "User", db: Session) -> Optional[InternalMessage]:
    """Busca a mensagem não lida mais recente para a congregação do usuário."""
    if not user.congregation_id:
        return None
    
    # Exclui Sede (quem envia)
    if user.role == "SEDE": 
        return None
        
    q = select(InternalMessage).where(
        InternalMessage.target_congregation_id == user.congregation_id,
        InternalMessage.is_read == False
    ).order_by(InternalMessage.date_sent.desc()).limit(1)
    
    return db.scalar(q)

def mark_message_as_read(msg_id: int):
    """Marca uma mensagem específica como lida."""
    with SessionLocal() as db:
        msg = db.get(InternalMessage, msg_id)
        if msg:
            msg.is_read = True
            db.commit()

SIDEBAR_PILLS_CSS = """
<style>
/* ====== Vars (edite pra ajustar a paleta) ====== */
:root {
  --pill-bg: #f8fafc;           /* fundo padrão */
  --pill-text: #0f172a;         /* texto padrão */
  --pill-border: #e2e8f0;       /* borda padrão */
  --pill-hover-bg: #eff6ff;     /* hover fundo */
  --pill-hover-border: #bfdbfe; /* hover borda */
  --pill-active-bg: #1e40af;    /* ativo fundo (azul) */
  --pill-active-text: #ffffff;  /* ativo texto */
  --pill-active-border: #1e3a8a;/* ativo borda */
  --pill-shadow: 0 1px 2px rgba(0,0,0,.04);
  --pill-shadow-active: 0 2px 6px rgba(30,64,175,.25);
}

/* Espaçamento entre itens do grupo */
div[data-testid="stSidebar"] [role="radiogroup"]{
  display: grid !important;
  gap: 10px !important;
}

/*** VERSÃO A — cada item é <div role="radio"> ***/
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"]{
  display: flex; align-items:center; width:100%;
  border:1px solid var(--pill-border);
  background:var(--pill-bg); color:var(--pill-text);
  padding:12px 14px; border-radius:12px;
  box-shadow:var(--pill-shadow);
  transition:background .12s, border-color .12s, color .12s, box-shadow .12s, transform .02s;
  cursor:pointer;
}
/* Esconde a bolinha/ícone do rádio */
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"] svg,
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"] > div:first-child{
  display:none !important;
}
/* Texto dentro (garante largura total) */
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"] > div{
  width:100%;
}
/* Hover */
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"]:hover{
  background:var(--pill-hover-bg);
  border-color:var(--pill-hover-border);
}
/* Ativo */
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"][aria-checked="true"]{
  background:var(--pill-active-bg)!important;
  border-color:var(--pill-active-border)!important;
  color:var(--pill-active-text)!important;
  box-shadow:var(--pill-shadow-active);
  transform: translateY(-0.5px);
}

/*** VERSÃO B — cada item é <label><input type=radio></label> ***/
div[data-testid="stSidebar"] [role="radiogroup"] > label{
  display:flex; align-items:center; width:100%;
  border:1px solid var(--pill-border);
  background:var(--pill-bg); color:var(--pill-text);
  padding:12px 14px; border-radius:12px;
  box-shadow:var(--pill-shadow);
  transition:background .12s, border-color .12s, color .12s, box-shadow .12s, transform .02s;
  cursor:pointer; user-select:none; -webkit-user-select:none;
  gap:10px;
}
/* Esconde input bolinha */
div[data-testid="stSidebar"] [role="radiogroup"] > label input[type="radio"],
div[data-testid="stSidebar"] [role="radiogroup"] > label > div:first-child{
  display:none !important;
}
/* Hover */
div[data-testid="stSidebar"] [role="radiogroup"] > label:hover{
  background:var(--pill-hover-bg);
  border-color:var(--pill-hover-border);
}
/* Ativo (usa :has para capturar o checked) */
div[data-testid="stSidebar"] [role="radiogroup"] > label:has(input[type="radio"]:checked){
  background:var(--pill-active-bg)!important;
  border-color:var(--pill-active-border)!important;
  color:var(--pill-active-text)!important;
  box-shadow:var(--pill-shadow-active);
  transform: translateY(-0.5px);
}

/* Tipografia consistente e sem “bolinhas” de lista */
div[data-testid="stSidebar"] [role="radiogroup"] > *{
  font-size: 1rem;
  font-weight: 600;
  list-style: none !important;
}

/* Remove marcadores/sangrias acidentais */
div[data-testid="stSidebar"] li{
  list-style: none !important;
  margin: 0 !important; padding: 0 !important;
}
</style>
"""
SIDEBAR_MODERN_MENU_CSS = """
<style>
:root{
  --pill-bg:#f8fafc; --pill-text:#0f172a; --pill-border:#e2e8f0;
  --pill-hover-bg:#eff6ff; --pill-hover-border:#bfdbfe;
  --pill-active-bg:#1d4ed8; --pill-active-text:#ffffff; --pill-active-border:#1e40af;
  --pill-shadow:0 1px 2px rgba(0,0,0,.05);
  --pill-shadow-active:0 4px 12px rgba(29,78,216,.25);
  --focus-ring:0 0 0 3px rgba(59,130,246,.25);
}
div[data-testid="stSidebar"] [role="radiogroup"]{ display:grid!important; gap:10px!important; margin-top:6px; }

/* Estrutura A: <div role="radio"> */
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"]{
  display:flex; align-items:center; width:100%;
  border:1px solid var(--pill-border); background:var(--pill-bg); color:var(--pill-text);
  padding:12px 14px; border-radius:14px; box-shadow:var(--pill-shadow);
  transition:background .15s, border-color .15s, color .15s, transform .02s, box-shadow .15s; cursor:pointer;
}
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"] svg,
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"] > div:first-child{ display:none!important; }
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"] > div{ width:100%; }
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"]:hover{ background:var(--pill-hover-bg); border-color:var(--pill-hover-border); }
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"][aria-checked="true"]{
  background:var(--pill-active-bg)!important; color:var(--pill-active-text)!important;
  border-color:var(--pill-active-border)!important; box-shadow:var(--pill-shadow-active); transform:translateY(-1px);
}
div[data-testid="stSidebar"] [role="radiogroup"] > div[role="radio"]:focus-visible{ outline:none; box-shadow:var(--focus-ring); }

/* Estrutura B: <label><input type=radio> */
div[data-testid="stSidebar"] [role="radiogroup"] > label{
  display:flex; align-items:center; width:100%;
  border:1px solid var(--pill-border); background:var(--pill-bg); color:var(--pill-text);
  padding:12px 14px; border-radius:14px; box-shadow:var(--pill-shadow);
  transition:background .15s, border-color .15s, color .15s, transform .02s, box-shadow .15s;
  cursor:pointer; user-select:none; -webkit-user-select:none; gap:10px;
}
div[data-testid="stSidebar"] [role="radiogroup"] > label input[type="radio"],
div[data-testid="stSidebar"] [role="radiogroup"] > label > div:first-child{ display:none!important; }
div[data-testid="stSidebar"] [role="radiogroup"] > label:hover{ background:var(--pill-hover-bg); border-color:var(--pill-hover-border); }
div[data-testid="stSidebar"] [role="radiogroup"] > label:has(input[type="radio"]:checked){
  background:var(--pill-active-bg)!important; color:var(--pill-active-text)!important;
  border-color:var(--pill-active-border)!important; box-shadow:var(--pill-shadow-active); transform:translateY(-1px);
}
div[data-testid="stSidebar"] [role="radiogroup"] > label:focus-within{ box-shadow:var(--focus-ring); }

/* Estrutura C: BaseWeb wrapper */
div[data-testid="stSidebar"] [role="radiogroup"] > div[data-baseweb="radio"]{
  display:flex; align-items:center; width:100%;
  border:1px solid var(--pill-border); background:var(--pill-bg); color:var(--pill-text);
  padding:12px 14px; border-radius:14px; box-shadow:var(--pill-shadow);
  transition:background .15s, border-color .15s, color .15s, transform .02s, box-shadow .15s; cursor:pointer;
}
div[data-testid="stSidebar"] [role="radiogroup"] > div[data-baseweb="radio"] input[type="radio"],
div[data-testid="stSidebar"] [role="radiogroup"] > div[data-baseweb="radio"] svg{ display:none!important; }
div[data-testid="stSidebar"] [role="radiogroup"] > div[data-baseweb="radio"]:hover{ background:var(--pill-hover-bg); border-color:var(--pill-hover-border); }
div[data-testid="stSidebar"] [role="radiogroup"] > div[data-baseweb="radio"]:has(input[type="radio"]:checked){
  background:var(--pill-active-bg)!important; color:var(--pill-active-text)!important;
  border-color:var(--pill-active-border)!important; box-shadow:var(--pill-shadow-active); transform:translateY(-1px);
}

/* Tipografia */
div[data-testid="stSidebar"] [role="radiogroup"] > *{ font-size:1rem; font-weight:600; letter-spacing:.1px; }

/* Dark-mode */
@media (prefers-color-scheme: dark){
  :root{
    --pill-bg:#0b1220; --pill-text:#e5e7eb; --pill-border:#253045;
    --pill-hover-bg:#111a2b; --pill-hover-border:#334155;
    --pill-active-bg:#1d4ed8; --pill-active-text:#ffffff; --pill-active-border:#1e40af;
    --pill-shadow:0 1px 2px rgba(0,0,0,.4); --pill-shadow-active:0 4px 12px rgba(0,0,0,.55);
    --focus-ring:0 0 0 3px rgba(59,130,246,.35);
  }
}
</style>
"""





def sidebar_common(user: "User") -> str:
    """Sidebar com botões 'pill' modernos.
       Simplifica rotas para 5 opções principais para usuários leigos.
    """
    import streamlit as st  # <=== CORREÇÃO DO UnboundLocalError
    
    role = getattr(user, "role", "")
    
    # 🚨 Lista de menus ATUALIZADA (COM Assistente IA para SEDE)
    if role == "SEDE":
        menu_options_plain = [
            "Painel Principal",
            "Lançamentos",
            "Relatórios Financeiros",  # <--- A IA será integrada AQUI
            "Gestão Missões",
            "Assistente IA",         # <=== ADICIONADO AQUI
            "Configurações",
        ]
    elif role == "TESOUREIRO":
        menu_options_plain = [
            "Painel Principal",
            "Lançamentos",
            "Relatórios Financeiros",
            "Gestão Missões",
        ]
    elif role == "TESOUREIRO MISSIONÁRIO":
        menu_options_plain = [
            "Gestão Missões",
            # IA removida daqui
        ]
    else:
        menu_options_plain = ["Painel Principal"]

    session_key = "nav"
    current = st.session_state.get(session_key, menu_options_plain[0])
    try:
        default_index = menu_options_plain.index(current)
    except ValueError:
        default_index = 0

    with st.sidebar:
        # injeta o CSS moderno (mantido)
        st.markdown(SIDEBAR_MODERN_MENU_CSS, unsafe_allow_html=True)

        st.markdown("### Menu")

        page = st.radio(
            label="",
            options=menu_options_plain,
            index=default_index,
            key=session_key,
        )

        # Bloco de ambiente (mantido)
        if not os.environ.get("DATABASE_URL"):
            st.markdown(
                """
                <div style="
                    background:#eef2ff;
                    border:1px solid #c7d2ff;
                    color:#1f2937;
                    padding:14px 16px;
                    border-radius:10px;
                    margin-top:12px;">
                  <strong>Ambiente: DESENVOLVIMENTO</strong><br/>
                  <span style="opacity:.85;">(SQLite Local)</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Botão Sair (mantido)
        if st.button("Sair", use_container_width=False):
            try:
                logout()
                st.rerun()
            except Exception:
                pass

    return page


def page_inicio(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        
        # === DESTAQUE DO LOGO NO CORPO DA PÁGINA ===
        LOGO_PATH = "images/logo_igreja.png" 
        try:
            st.image(LOGO_PATH, width=150)
        except Exception:
            pass
        # === FIM DESTAQUE ===
        
        st.markdown("<h1 class='page-title'>Painel Principal</h1>", unsafe_allow_html=True)

        # 1. BLOCO DE FILTROS E ESCOPO
        
        congs_all = order_congs_sede_first(cong_options_for(user, db))
        parent_cong_obj = None
        
        col_cong, col_sub, col_data = st.columns([1.5, 1.5, 1])

        with col_cong:
            if user.role == "SEDE":
                cong_names = [c.name for c in congs_all] or ["—"]
                cong_sel = st.selectbox("Congregação (Escopo)", cong_names + ["Toda a Rede"])
                is_all_network = (cong_sel == "Toda a Rede")
                if not is_all_network:
                    parent_cong_obj = next((c for c in congs_all if c.name == cong_sel), None)
            else:
                parent_cong_obj = db.get(Congregation, user.congregation_id)
                st.text_input("Congregação (Escopo)", parent_cong_obj.name, disabled=True)
                is_all_network = False
                
        # 1.2. Seleção de Sub-unidade
        sub_id = None
        if parent_cong_obj:
            subs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id)).all()
            if subs:
                sub_opts = [f"{parent_cong_obj.name} (Principal)"] + [s.name for s in subs] + ["Todas as Unidades"]
                with col_sub:
                    opt = st.selectbox("Unidade (Filtro)", sub_opts, key="inicio_sub_filtro")
                    if opt == "Todas as Unidades":
                        sub_id = "ALL"
                    elif opt.endswith("(Principal)"):
                        sub_id = None
                    else:
                        sub_id = next(s.id for s in subs if s.name == opt)
            else:
                 with col_sub:
                     st.text_input("Unidade (Filtro)", f"{parent_cong_obj.name} (Principal)", disabled=True)


        # 1.3. Seleção de Data
        with col_data:
            ref = get_month_selector(label="Mês", key_prefix="inicio_ref")
        start, end = month_bounds(ref)

        st.divider()

        # --- FIM BLOCO DE FILTROS ---

        # 2) KPIs (entradas/saídas/saldo)
        if is_all_network:
            st.info("KPIs desabilitados para 'Toda a Rede'. Use a tabela abaixo.")
            entradas = saidas = saldo = 0.0
        else:
            # === LÓGICA CORRIGIDA: FILTRANDO MISSÕES PARA FLUXO DE CAIXA OPERACIONAL ===
            
            # --- Configuração dos Filtros de Exclusão ---
            # Assume que sub_id 'None' ou 'int' significa a unidade selecionada.
            sub_filter = [ServiceLog.sub_congregation_id == sub_id] if isinstance(sub_id, int) else [ServiceLog.sub_congregation_id.is_(None)]
            tx_sub_filter = [Transaction.sub_congregation_id == sub_id] if isinstance(sub_id, int) else [Transaction.sub_congregation_id.is_(None)]
            tithe_sub_filter = [Tithe.sub_congregation_id == sub_id] if isinstance(sub_id, int) else [Tithe.sub_congregation_id.is_(None)]
            
            # Condição para ignorar Missões no ServiceLog (Culto)
            missao_sl_filter = ServiceLog.service_type != "Culto de Missões"
            
            # Condição para ignorar Missões/Dízimos/Ofertas em Transações para o cálculo de 'Outras Entradas'
            cat_miss = db.scalar(select(Category).where(func.lower(Category.name).in_(("missões","missoes")), Category.type == TYPE_IN))
            cat_diz = db.scalar(select(Category).where(func.lower(Category.name).in_(("dízimo","dizimo")), Category.type == TYPE_IN))
            cat_ofe = db.scalar(select(Category).where(func.lower(Category.name) == "oferta", Category.type == TYPE_IN))
            exclude_cat_ids = [c.id for c in [cat_miss, cat_diz, cat_ofe] if c]
            
            # --- CÁLCULO DAS FONTES (Dízimo e Oferta) SEM MISSÕES ---
            
            # Dízimo do Resumo (ServiceLog) - APENAS não-Missões (CORREÇÃO APLICADA)
            # ESTE VALOR VEM DA FUNÇÃO AUXILIAR CORRIGIDA (_sum_dizimo_resumo)
            total_dizimo_resumo = _sum_dizimo_resumo(parent_cong_obj, sub_id, start, end, db)

            # Dízimo Nominal (Tithe) - Assume que a função _sum_dizimo_nominal está corrigida para filtrar
            total_dizimo_nominal = _sum_dizimo_nominal(parent_cong_obj, sub_id, start, end, db)
            
            # Dízimo FINAL (para o KPI): Regra de equivalência (max)
            dizimo_final = max(total_dizimo_resumo, total_dizimo_nominal)
            
            # Oferta do Resumo (ServiceLog) - APENAS não-Missões (CORREÇÃO APLICADA)
            # ESTE VALOR VEM DA FUNÇÃO AUXILIAR CORRIGIDA (_sum_oferta_resumo)
            total_oferta_resumo = _sum_oferta_resumo(parent_cong_obj, sub_id, start, end, db)
            
            # Oferta de Transações (Categoria 'Oferta')
            total_oferta_tx = float(db.scalar(select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                Transaction.congregation_id == parent_cong_obj.id, Transaction.date >= start, Transaction.date < end,
                *tx_sub_filter, Transaction.type == TYPE_IN, Transaction.category_id == cat_ofe.id if cat_ofe else Transaction.id < 0
            )) or 0.0)
            
            # Oferta FINAL (para o KPI): Regra de equivalência (max)
            oferta_final = max(total_oferta_resumo, total_oferta_tx)
            
            # Outras Entradas (Transactions que não são Dizimo, Oferta ou Missões)
            total_outras_entradas = float(db.scalar(select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                Transaction.congregation_id == parent_cong_obj.id, Transaction.date >= start, Transaction.date < end,
                *tx_sub_filter, Transaction.type == TYPE_IN, 
                Transaction.category_id.notin_(exclude_cat_ids) if exclude_cat_ids else Transaction.id > 0
            )) or 0.0)
            
            # Total de Saídas (KPI)
            saidas = _sum_saidas(parent_cong_obj, sub_id, start, end, db) 
            
            # Entrada Total do KPI
            entradas = dizimo_final + oferta_final + total_outras_entradas
            saldo    = entradas - saidas

            # --- EXIBIÇÃO DOS KPIS CORRIGIDOS ---
            c1,c2,c3 = st.columns(3)
            c1.metric("Entradas (Dízimo/Oferta)", format_currency(entradas))
            c2.metric("Saídas (Despesas)", format_currency(saidas))
            c3.metric("Saldo Final", format_currency(saldo), delta=(saldo))

            # 3) Avisos (mantidos)
            _render_missoes_notice()
            
            # Chamada das funções originais (agora corrigidas no back-end)
            total_resumo_for_banner = total_dizimo_resumo
            total_nominal_for_banner = _sum_dizimo_nominal(parent_cong_obj, sub_id, start, end, db)
            
            _render_divergence_banner(total_resumo_for_banner, total_nominal_for_banner)


        # 4) ATALHOS RÁPIDOS (Garantindo estética uniforme)
        st.markdown("#### Ações Rápidas")
        a1, a2, a3 = st.columns(3)

        # Botões de Lançamento (usando use_container_width=True)
        a1.button("Lançar Ofertas/Culto ➕", 
                  on_click=lambda: _goto("Lançamentos"), 
                  use_container_width=True, 
                  key="btn_atalho_oferta")
                  
        a2.button("Lançar Dízimo Rápido 🤲", 
                  on_click=lambda: _goto("Lançamentos"), 
                  use_container_width=True, 
                  key="btn_atalho_dizimo")
                  
        a3.button("Lançar Despesa Rápida 💳",
                  on_click=lambda: _goto("Lançamentos"), 
                  use_container_width=True, 
                  key="btn_atalho_despesa")
        
        # Botão de Configurações (Se for SEDE, fica em uma linha separada para melhor layout)
        if user.role == "SEDE":
            st.markdown("---")
            st.button("Configurações", 
                      on_click=lambda: _goto("Configurações"),
                      use_container_width=True,
                      key="btn_atalho_config")
        
        st.markdown("---")


        # 5) TABELAS DE RESUMO
        st.markdown("#### Resumo Mensal")
        
        # df é calculado com os valores arredondados (round(..., 2)) na função _build_resumo_por_unidade
        df = _build_resumo_por_unidade(parent_cong_obj, sub_id, start, end, db)
        
        # --- TABELA DE RESUMO CORRIGIDA ---
        st.markdown("##### Resumo de Entradas por Unidade")
        
        # Aplicação da formatação BRL (format_currency)
        st.dataframe(df.style.format({
            "Dízimos": format_currency, 
            "Ofertas": format_currency,
            "Total Entradas": format_currency
        }), use_container_width=True, hide_index=True)
        
        # --- MOVIMENTO RECENTE (Antiga aba 2) ---
        st.markdown("---")
        st.markdown("##### Últimas Ações (Movimento Recente)")
        df_recent = _ultimos_movimentos(parent_cong_obj, sub_id, start, end, db)
        st.dataframe(df_recent.style.format({"Valor": format_currency}), use_container_width=True, hide_index=True)


        # --- EXPORTAÇÃO (Antiga aba 3) ---
        st.markdown("---")
        st.markdown("##### ⬇️ Downloads")
        b1, b2, b3 = st.columns(3)
        
        # Lógica de download
        b1.caption("Exportar Mês (CSV) [Em Breve]")
        b2.caption("Exportar Mês (Excel) [Em Breve]")
        
        # Rodapé
        ambiente_status = "Ambiente: DESENVOLVIMENTO" if not os.environ.get("DATABASE_URL") else "Ambiente: Produção"
        
        b3.caption(f"Período: {ref.strftime('%B de %Y')}")
        b3.caption(f"Versão do sistema: 1.0 • {ambiente_status}")
def display_finance_hierarchy_aggregated(congs_all: list, start: date, end: date, db: Session):
    """
    Gera as duas tabelas (Entrada Total e Saída Total) de toda a rede
    agregadas por Congregação (Visão Nível 1 Agregada).
    """
    import pandas as pd
    from sqlalchemy import select, func
    
    st.markdown("## Visão Agregada Total da Rede")
    st.caption(f"Dados agregados de todas as congregações no período: {start.strftime('%d/%m/%Y')} a {end.strftime('%d/%m/%Y')}.")

    # --- 1. TABELA DE ENTRADAS (Entrada Total) ---
    st.markdown("### 📈 Entradas (Total por Congregação)")
    
    rows_entrada = []
    for c in congs_all:
        totals = _collect_month_data(c.id, start, end, sub_cong_id="ALL")["totals"] 
        rows_entrada.append({
            "Congregação": c.name,
            "Total_Entrada": totals["entradas_total_sem_missoes"]
        })
    
    df_entrada = pd.DataFrame(rows_entrada)
    
    if not df_entrada.empty:
        total_geral_entrada = df_entrada["Total_Entrada"].sum()
        df_entrada["Entrada (R$)"] = df_entrada["Total_Entrada"].apply(format_currency)
        
        # === CORREÇÃO: Ordena o DF completo ANTES de projetar a visualização ===
        df_sorted_entrada = df_entrada.sort_values("Total_Entrada", ascending=False)
        
        st.dataframe(
            df_sorted_entrada[["Congregação", "Entrada (R$)"]],
            use_container_width=True,
            hide_index=True
        )
        st.metric("Total Geral de Entradas da Rede", format_currency(total_geral_entrada))
    else:
        st.info("Nenhuma entrada encontrada para o período em toda a rede.")


    # --- 2. TABELA DE SAÍDAS (Saída Total) ---
    st.markdown("### 📉 Saídas (Total por Congregação)")
    
    rows_saida = []
    for c in congs_all:
        totals = _collect_month_data(c.id, start, end, sub_cong_id="ALL")["totals"] 
        rows_saida.append({
            "Congregação": c.name,
            "Total_Saida": totals["saidas_total"]
        })
        
    df_saida = pd.DataFrame(rows_saida)
    
    if not df_saida.empty:
        total_geral_saida = df_saida["Total_Saida"].sum()
        df_saida["Saída (R$)"] = df_saida["Total_Saida"].apply(format_currency)
        
        # === CORREÇÃO: Ordena o DF completo ANTES de projetar a visualização ===
        df_sorted_saida = df_saida.sort_values("Total_Saida", ascending=False)

        st.dataframe(
            df_sorted_saida[["Congregação", "Saída (R$)"]],
            use_container_width=True,
            hide_index=True
        )
        st.metric("Total Geral de Saídas da Rede", format_currency(total_geral_saida))
    else:
        st.info("Nenhuma saída encontrada para o período em toda a rede.")

def page_relatorios_unificados(user: "User"):
    ensure_seed()
    
    # ... (Seu bloco de CSS deve vir aqui) ...

    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Relatórios Financeiros</h1>", unsafe_allow_html=True)
        
        # OBTENÇÃO DO ESCOPO DE DADOS E FILTROS GLOBAIS
        congs_all = order_congs_sede_first(cong_options_for(user, db))
        
        col_cong, col_data = st.columns([2, 1])
        
        # --- FILTROS DE ESCOPO ---
        parent_cong_obj = None
        is_aggregated_view = False
        
        with col_cong:
            if user.role == "SEDE":
                # === LÓGICA DE ESCOPO SEDE COM VISÃO HIERÁRQUICA ===
                HIERARCHY_OPTION = "-- RELATÓRIO HIERÁRQUICO (TODAS AS CONGREGAÇÕES) --"
                cong_names = [c.name for c in congs_all]
                escopo_opts = [HIERARCHY_OPTION] + cong_names
                
                escopo_selecionado = st.selectbox("Congregação (Escopo)", escopo_opts, key="rel_cong_filtro_sede")
                
                if escopo_selecionado == HIERARCHY_OPTION:
                    is_aggregated_view = True
                    st.caption("Exibindo Entradas e Saídas de TODA a rede.")
                else:
                    parent_cong_obj = next((c for c in congs_all if c.name == escopo_selecionado), None)
            else:
                # Perfil não-SEDE: Trava na congregação do usuário (lógica original)
                parent_cong_obj = db.get(Congregation, user.congregation_id)
                st.text_input("Congregação (Escopo)", parent_cong_obj.name if parent_cong_obj else "N/A", disabled=True)
                
        with col_data:
            ref = get_month_selector(label="Mês de Referência", key_prefix="rel_ref")
        start, end = month_bounds(ref)
        
        st.divider()

        # =========================================================================
        # LÓGICA DE EXIBIÇÃO: VISÃO AGREGADA TOTAL (SEDE)
        # =========================================================================
        if is_aggregated_view:
            # CHAMADA CORRETA APÓS DEFINIÇÃO DA FUNÇÃO AUXILIAR
            display_finance_hierarchy_aggregated(congs_all, start, end, db)
            return

        # Verifica se há congregação selecionada para continuar com as ABAS
        if not parent_cong_obj:
            st.warning("Selecione uma congregação válida no filtro acima.")
            return

        # --- Variáveis de suporte para as abas (lógica original) ---
        target_cong_id = parent_cong_obj.id
        
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == target_cong_id)).all()
        opcoes_unidade = {"-- Todas (Principal + Subs) --": "ALL", f"{parent_cong_obj.name} (Principal)": None}
        for sub in sub_congs:
             opcoes_unidade[sub.name] = sub.id

        # ================== ABAS CORRIGIDAS NA ORDEM SOLICITADA (Lógica original) ==================
        tab1, tab2, tab3, tab4 = st.tabs([
            "📖 Entradas (Culto/Resumo)",
            "🤲 Dízimos (Nominal)",
            "💳 Saídas (Despesas)",
            "🏆 Painel (Visão Geral)"
        ])

        # --- O CÓDIGO DE CADA ABA ORIGINAL DO SEU RELATÓRIO CONTINUA A PARTIR DAQUI ---
        
        with tab1:
            st.subheader("📖 Resumo Diário de Entradas (Cultos)")
            # ... (seu código da TAB 1 continua aqui) ...
            if target_cong_id:
                # LÓGICA DE FILTRO DE SUB-UNIDADE
                if sub_congs:
                    target_sub_name = st.selectbox("Filtrar Unidade (Entradas):", list(opcoes_unidade.keys()), key="unif_entradas_sub")
                    target_sub_id_unif = opcoes_unidade[target_sub_name]
                else:
                    target_sub_name = f"{parent_cong_obj.name} (Principal)"
                    target_sub_id_unif = None
                # FIM LÓGICA
                
                st.info(f"Exibindo resumos de culto para: **{target_sub_name}**")
                
                report_df = _load_service_logs(target_cong_id, start, end, sub_cong_id=target_sub_id_unif)
                
                if not report_df.empty:
                    st.dataframe(
                        report_df.style.format({"Data do Culto": "{:%d/%m/%Y}", "Dízimo": format_currency, "Oferta": format_currency, "Total": format_currency}),
                        use_container_width=True, hide_index=True, column_order=["Data do Culto", "Tipo de Culto", "Dízimo", "Oferta", "Total"]
                    )
                    st.metric("Total Geral de Entradas no Culto", format_currency(report_df["Total"].sum()))
                else:
                    st.caption("Nenhum resumo de culto encontrado para este período.")


        with tab2:
            st.subheader("🤲 Dízimos Nominais (Por Pessoa)")
            if target_cong_id:
                # LÓGICA DE FILTRO DE SUB-UNIDADE
                if sub_congs:
                    target_sub_name = st.selectbox("Filtrar Unidade (Dízimos):", list(opcoes_unidade.keys()), key="unif_dizimo_sub")
                    target_sub_id_unif = opcoes_unidade[target_sub_name]
                else:
                    target_sub_name = f"{parent_cong_obj.name} (Principal)"
                    target_sub_id_unif = None

                st.info(f"Exibindo dízimos nominais para: **{target_sub_name}**")
                
                tithes_q = select(Tithe).where(
                    Tithe.congregation_id == target_cong_id,
                    Tithe.date >= start, Tithe.date < end,
                    Tithe.sub_congregation_id == target_sub_id_unif
                )
                tithes = db.scalars(tithes_q.order_by(Tithe.tither_name)).all()
                
                if tithes:
                    rows = [{"Data": t.date, "Dizimista": t.tither_name, "Valor": float(t.amount), "Forma": t.payment_method or "—"} for t in tithes]
                    df = pd.DataFrame(rows)
                    st.dataframe(df.style.format({"Data": "{:%d/%m/%Y}", "Valor": format_currency}), use_container_width=True, hide_index=True)
                    st.metric("Total Dízimos Nominais", format_currency(df["Valor"].sum()))
                else:
                    st.caption("Nenhum dízimo nominal encontrado.")


        with tab3:
            st.subheader("💳 Saídas (Transações de Despesas)")
            if target_cong_id:
                # LÓGICA DE FILTRO DE SUB-UNIDADE
                if sub_congs:
                    target_sub_name = st.selectbox("Filtrar Unidade (Saídas):", list(opcoes_unidade.keys()), key="unif_saidas_sub")
                    target_sub_id_unif = opcoes_unidade[target_sub_name]
                else:
                    target_sub_name = f"{parent_cong_obj.name} (Principal)"
                    target_sub_id_unif = None

                st.info(f"Exibindo saídas para: **{target_sub_name}**")
                
                txs_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
                    Transaction.congregation_id == target_cong_id, 
                    Transaction.date >= start, Transaction.date < end, 
                    Transaction.type == "SAÍDA", 
                    Transaction.sub_congregation_id == target_sub_id_unif
                )
                txs_out = db.scalars(txs_out_query.order_by(Transaction.date)).all()
                
                if txs_out:
                    rows_out = [{"Data": t.date, "Categoria": t.category.name if t.category else "", "Valor": t.amount, "Descrição": t.description or ""} for t in txs_out]
                    df_saidas = pd.DataFrame(rows_out)
                    st.dataframe(df_saidas.style.format({"Data":"{:%d/%m/%Y}", "Valor": format_currency}), use_container_width=True, hide_index=True)
                    st.metric("Total de Saídas no Período", format_currency(df_saidas["Valor"].sum()))
                else:
                    st.caption("Nenhuma saída registrada neste período.")


        with tab4:
            st.markdown('<div class="panel-destaque-fundo">', unsafe_allow_html=True)
            st.subheader("🏆 Painel de Indicadores e Saldo")
            if not parent_cong_obj:
                st.warning("Selecione uma congregação válida no filtro acima.")
            else:
                
                # --- Cálculo dos KPIS (Lógica de Missões já corrigida nas funções auxiliares) ---
                sub_id = None 
                
                # Configuração dos Filtros de Exclusão (Para Outras Entradas)
                cat_miss = db.scalar(select(Category).where(func.lower(Category.name).in_(("missões","missoes")), Category.type == TYPE_IN))
                cat_diz = db.scalar(select(Category).where(func.lower(Category.name).in_(("dízimo","dizimo")), Category.type == TYPE_IN))
                cat_ofe = db.scalar(select(Category).where(func.lower(Category.name) == "oferta", Category.type == TYPE_IN))
                exclude_cat_ids = [c.id for c in [cat_miss, cat_diz, cat_ofe] if c]

                # Dízimo Final (Usando as funções auxiliares corrigidas)
                total_dizimo_resumo = _sum_dizimo_resumo(parent_cong_obj, sub_id, start, end, db)
                total_dizimo_nominal = _sum_dizimo_nominal(parent_cong_obj, sub_id, start, end, db)
                dizimo_final = max(total_dizimo_resumo, total_dizimo_nominal)

                # Oferta Final (Usando as funções auxiliares corrigidas)
                total_oferta_resumo = _sum_oferta_resumo(parent_cong_obj, sub_id, start, end, db)
                total_oferta_tx = float(db.scalar(select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.congregation_id == parent_cong_obj.id, Transaction.date >= start, Transaction.date < end,
                    Transaction.type == TYPE_IN, Transaction.category_id == cat_ofe.id if cat_ofe else Transaction.id < 0
                )) or 0.0)
                oferta_final = max(total_oferta_resumo, total_oferta_tx)
                
                # Outras Entradas
                total_outras_entradas = float(db.scalar(select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.congregation_id == parent_cong_obj.id, Transaction.date >= start, Transaction.date < end,
                    Transaction.type == TYPE_IN, 
                    Transaction.category_id.notin_(exclude_cat_ids) if exclude_cat_ids else Transaction.id > 0
                )) or 0.0)
                
                # Saídas
                saidas = _sum_saidas(parent_cong_obj, sub_id, start, end, db)
                
                # Totais
                entradas = dizimo_final + oferta_final + total_outras_entradas
                saldo = entradas - saidas

                # EXIBIÇÃO DOS KPIS
                c1,c2,c3 = st.columns(3)
                c1.metric("Entradas (Dízimo/Oferta)", format_currency(entradas))
                c2.metric("Saídas (Despesas)", format_currency(saidas))
                c3.metric("Saldo Final", format_currency(saldo), delta=(saldo))

                st.divider()

                st.markdown("##### 🖨️ Download de Relatórios")
                
                # Download (Consolidado + Individual)
                if user.role == "SEDE":
                    st.download_button(
                        "⬇️ Baixar Relatório Geral Consolidado (PDF)",
                        data=build_consolidated_pdf(congs_all, ref, db), 
                        file_name=f"relatorio_geral_consolidado_{ref.strftime('%Y-%m')}.pdf",
                        mime="application/pdf",
                        key="dl_pdf_geral_consolidado"
                    )
                
                st.markdown("---")
                st.markdown("##### Relatórios Individuais por Congregação")
                
                congs_download_list = order_congs_sede_first(cong_options_for(user, db))
                congs_download_names = [c.name for c in congs_download_list]
                
                sel_cong_pdf_name = st.selectbox(
                    "Selecione a Congregação para download:",
                    congs_download_names,
                    key="sel_cong_pdf_download"
                )
                
                selected_cong_obj = next((c for c in congs_download_list if c.name == sel_cong_pdf_name), None)

                if selected_cong_obj:
                    # Funções de construção de PDF (assumidas no escopo)
                    pdf_data_unit = build_single_unit_report_pdf(
                        selected_cong_obj.id, None, selected_cong_obj.name, ref, db
                    )
                    
                    def _norm(name):
                        return name.lower().replace(" ", "_").replace("ã", "a").replace("ç", "c")
                        
                    st.download_button(
                        f"⬇️ Baixar PDF de {selected_cong_obj.name}",
                        data=pdf_data_unit,
                        file_name=f"prestacao_{_norm(selected_cong_obj.name)}_{ref.strftime('%Y-%m')}.pdf",
                        mime="application/pdf",
                        key=f"dl_pdf_unit_{selected_cong_obj.id}"
                    )
            
            st.markdown('</div>', unsafe_allow_html=True) # Fechamento do div de destaque # Fechamento do div de destaque


# Helpers mínimos (esboços)
def _sum_entradas(parent, sub_id, start, end, db):
    cond = [Transaction.type == TYPE_IN, Transaction.date >= start, Transaction.date < end]
    if parent: cond.append(Transaction.congregation_id == parent.id)
    if sub_id == "ALL": pass
    elif sub_id is None: cond.append(Transaction.sub_congregation_id.is_(None))
    else: cond.append(Transaction.sub_congregation_id == sub_id)
    return float(db.scalar(select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(*cond)) or 0.0)

def _sum_saidas(parent, sub_id, start, end, db):
    """
    Soma o total de saídas (Transaction type=SAÍDA), excluindo a categoria 'Missões (Saída)'.
    """
    cat_miss_saida = db.scalar(select(Category).where(func.lower(Category.name).in_(("missões (saída)", "missoes (saida)")), Category.type == TYPE_OUT))
    
    cond = [Transaction.type == TYPE_OUT, Transaction.date >= start, Transaction.date < end]
    if parent: cond.append(Transaction.congregation_id == parent.id)
    if sub_id == "ALL": pass
    elif sub_id is None: cond.append(Transaction.sub_congregation_id.is_(None))
    else: cond.append(Transaction.sub_congregation_id == sub_id)
        
    # FILTRO CRÍTICO: Excluir Saídas de Missões
    if cat_miss_saida:
        cond.append(Transaction.category_id != cat_miss_saida.id)
        
    return float(db.scalar(select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(*cond)) or 0.0)

def _sum_dizimo_resumo(parent, sub_id, start, end, db):
    """
    Soma o total de dízimos declarados no resumo de culto (ServiceLog),
    excluindo o tipo 'Culto de Missões' (caixa segregado).
    """
    cond = [ServiceLog.date >= start, ServiceLog.date < end]
    if parent: cond.append(ServiceLog.congregation_id == parent.id)
    if sub_id == "ALL": pass
    elif sub_id is None: cond.append(ServiceLog.sub_congregation_id.is_(None))
    else: cond.append(ServiceLog.sub_congregation_id == sub_id)
    
    # FILTRO CRÍTICO: Excluir logs de 'Culto de Missões'
    cond.append(ServiceLog.service_type != "Culto de Missões") 
    
    return float(db.scalar(select(func.coalesce(func.sum(ServiceLog.dizimo), 0.0)).where(*cond)) or 0.0)

def _sum_dizimo_nominal(parent, sub_id, start, end, db):
    """
    Soma os dízimos nominais (Tithe), excluindo registros de Missões.
    """
    cond = [Tithe.date >= start, Tithe.date < end]
    if parent: cond.append(Tithe.congregation_id == parent.id)
    if sub_id == "ALL": pass
    elif sub_id is None: cond.append(Tithe.sub_congregation_id.is_(None))
    else: cond.append(Tithe.sub_congregation_id == sub_id)
        
    # FILTRO CRÍTICO: Excluir dízimos lançados para Missões ou com nome de Missão
    cond.append(func.lower(Tithe.tither_name).notin_(["missoes", "missões", "oferta de missoes", "oferta de missões"]))
    
    return float(db.scalar(select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(*cond)) or 0.0)

def _sum_oferta_resumo(parent, sub_id, start, end, db):
    """
    Soma o total de ofertas declaradas no resumo de culto (ServiceLog),
    excluindo o tipo 'Culto de Missões' (caixa segregado).
    """
    cond = [ServiceLog.date >= start, ServiceLog.date < end]
    if parent: cond.append(ServiceLog.congregation_id == parent.id)
    if sub_id == "ALL": pass
    elif sub_id is None: cond.append(ServiceLog.sub_congregation_id.is_(None))
    else: cond.append(ServiceLog.sub_congregation_id == sub_id)
    
    # FILTRO CRÍTICO: Excluir Cultos de Missões
    cond.append(ServiceLog.service_type != "Culto de Missões") 
    
    return float(db.scalar(select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(*cond)) or 0.0)





# ======= NOVO: helper padrão para botões 'Salvar alterações' =======
# ====== CORES P/ BOTÕES ======
# ====== CORES P/ BOTÕES — MUDAR TUDO PARA AZUL (HEX #2563eb) ======
BTN_COLORS = {
    "entrada":  "#2563eb",  # AZUL (era verde)
    "dizimista":"#2563eb",  # AZUL (manter)
    "saida":    "#2563eb",  # AZUL (era vermelho)
    "neutral":  "#2563eb",  # AZUL (era neutro)
}

# ====== NOVO: helper padrão para botões 'Salvar alterações' =======
# Altera a lógica de _save_btn e _submit_btn para simplificar o CSS
# O novo CSS de cor virá do seu bloco BTN_COLORS.

def _save_btn(on_click, key_suffix: str, theme: str = "neutral", label: str = "Salvar alterações"):
    """
    Botão 'Salvar alterações' com cor personalizada por tema.
    """
    # Esta função agora confia que o BTN_COLORS vai dar a cor certa
    color = BTN_COLORS.get(theme, BTN_COLORS["neutral"])
    
    # Marcador de ID que o CSS usará
    st.markdown(f'<div id="mark-{key_suffix}"></div>', unsafe_allow_html=True)
    
    # Criamos o botão como st.button (tipo secondary para não brigar com primary)
    st.button(label, key=f"btn_save_{key_suffix}", type="secondary", on_click=on_click)
    
    # Injetamos o CSS direcionado para o ID, forçando a cor
    st.markdown(
        f"""
        <style>
          /* Força a cor definida por BTN_COLORS para este botão específico */
          #mark-{key_suffix} ~ div[data-testid="stButton"] > button {{
            background-color: {color} !important;
            border-color: {color} !important;
            color: white !important; /* Garante texto branco em fundos escuros */
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
    Versão colorida para st.form_submit_button (forms). Retorna True quando o usuário clica.
    """
    color = BTN_COLORS.get(theme, BTN_COLORS["neutral"])
    
    # Marcador de ID que o CSS usará
    st.markdown(f'<div id="mark-{key_suffix}"></div>', unsafe_allow_html=True)
    
    # Criamos o botão de formulário (tipo secondary para não brigar com primary)
    clicked = st.form_submit_button(label, type="secondary")
    
    # Injetamos o CSS direcionado para o ID, forçando a cor
    st.markdown(
        f"""
        <style>
          /* Força a cor definida por BTN_COLORS para este botão de formulário específico */
          #mark-{key_suffix} ~ div[data-testid="stFormSubmitButton"] > button,
          #mark-{key_suffix} ~ div[data-testid="stButton"] > button {{ /* fallback */
            background-color: {color} !important;
            border-color: {color} !important;
            color: white !important; 
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

    n = n_bruto[
        (n_bruto["Valor"].abs() > 0.01) & 
        (n_bruto["Dizimista"] != "")
    ].copy()

    old_ids = set(int(x) for x in o["ID"].tolist() if pd.notna(x) and x > 0)
    new_ids = set(int(x) for x in n["ID"].tolist() if pd.notna(x) and x > 0)
    to_delete = list(old_ids - new_ids)

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
            
            new_valor_float = float(new["Valor"])
            if t.amount != new_valor_float: t.amount = new_valor_float; changed = True
            
            if (t.payment_method or "") != (new["Forma de Pagamento"] or ""): t.payment_method = new["Forma de Pagamento"] or None; changed = True
            if changed: db.add(t)

        for _, row in n.iterrows():
            rid = row.get("ID", None)
            if pd.notna(rid) and int(rid) > 0: continue

            if default_cong_id is None: continue
            db.add(Tithe(
                date=row["Data"], 
                tither_name=row["Dizimista"], 
                amount=float(row["Valor"]),
                congregation_id=int(default_cong_id), 
                # --- A CORREÇÃO ESTÁ AQUI ---
                sub_congregation_id=default_sub_cong_id, # Corrigido de 'sub_cong_regation_id'
                # --- FIM DA CORREÇÃO ---
                payment_method=(row.get("Forma de Pagamento") or None)
            ))
        db.commit()
        # ================================================================

# ===================== RELATÓRIO DE ENTRADA — TABELA ÚNICA (EDIT SUMÁRIO) =====================
@st.cache_data
def _entrada_summary_df(
    _db: Session,
    cong_id: int,
    start: date,
    end: date,
    sub_cong_id: Optional[int] = None
) -> pd.DataFrame:
    """
    Retorna um DataFrame com colunas:
      - Data do Culto
      - Dízimo   (aplica a equivalência: maior entre Tithe e Transaction 'Dízimo')
      - Oferta   (MAIOR entre ServiceLog.oferta e Transações categoria 'Oferta')
      - Total    (= Dízimo + Oferta)
    Filtro por congregação principal ou por sub_congregação (se informada).
    """
    # Base queries
    # Dízimos (nominal)
    tithes_q = select(Tithe.date, func.coalesce(func.sum(Tithe.amount), 0.0)).where(
        Tithe.congregation_id == cong_id,
        Tithe.date >= start,
        Tithe.date < end,
    )

    # Dízimos (transações)
    diz_trans_q = select(Transaction.date, func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
        Transaction.congregation_id == cong_id,
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type.in_((TYPE_IN, "RECEITA")),
        func.lower(func.replace(Category.name, " ", "" )).in_(("dizimo","dízimo")),
    )

    # Ofertas (transações)
    oferta_trans_q = select(Transaction.date, func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
        Transaction.congregation_id == cong_id,
        Transaction.date >= start,
        Transaction.date < end,
        Transaction.type.in_((TYPE_IN, "RECEITA")),
        func.lower(func.replace(Category.name, " ", "" )) == "oferta",
    )

    # Ofertas (ServiceLog) — Resumo do Culto por data
    sl_oferta_q = select(ServiceLog.date, func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
        ServiceLog.congregation_id == cong_id,
        ServiceLog.date >= start,
        ServiceLog.date < end
    )

    # Filtro de sub-congregação
    if sub_cong_id is not None:
        tithes_q      = tithes_q.where(Tithe.sub_congregation_id == sub_cong_id)
        diz_trans_q   = diz_trans_q.where(Transaction.sub_congregation_id == sub_cong_id)
        oferta_trans_q= oferta_trans_q.where(Transaction.sub_congregation_id == sub_cong_id)
        sl_oferta_q   = sl_oferta_q.where(ServiceLog.sub_congregation_id == sub_cong_id)
    else:
        tithes_q      = tithes_q.where(Tithe.sub_congregation_id.is_(None))
        diz_trans_q   = diz_trans_q.where(Transaction.sub_congregation_id.is_(None))
        oferta_trans_q= oferta_trans_q.where(Transaction.sub_congregation_id.is_(None))
        sl_oferta_q   = sl_oferta_q.where(ServiceLog.sub_congregation_id.is_(None))

    # Executa queries (usa _db recebido)
    tithes      = _db.execute(tithes_q.group_by(Tithe.date)).all()
    diz_trans   = _db.execute(diz_trans_q.group_by(Transaction.date)).all()
    oferta_trans= _db.execute(oferta_trans_q.group_by(Transaction.date)).all()
    sl_ofertas  = _db.execute(sl_oferta_q.group_by(ServiceLog.date)).all()

    # Agrega por data
    by_date_diz_tit = defaultdict(float)
    for d, s in tithes:
        by_date_diz_tit[d] += float(s or 0.0)

    by_date_diz_tx = defaultdict(float)
    for d, s in diz_trans:
        by_date_diz_tx[d] += float(s or 0.0)

    by_date_ofe_tx = defaultdict(float)
    for d, s in oferta_trans:
        by_date_ofe_tx[d] += float(s or 0.0)

    by_date_ofe_sl = defaultdict(float)
    for d, s in sl_ofertas:
        by_date_ofe_sl[d] += float(s or 0.0)

    all_dates = sorted(set(list(by_date_diz_tit.keys()) + list(by_date_diz_tx.keys()) + list(by_date_ofe_tx.keys()) + list(by_date_ofe_sl.keys())))

    rows = []
    for d in all_dates:
        dz  = max(float(by_date_diz_tit.get(d, 0.0)), float(by_date_diz_tx.get(d, 0.0)))
        # Oferta por dia = MAIOR entre ServiceLog.oferta (se houver) e Transação(categoria 'Oferta')
        ofe = max(float(by_date_ofe_sl.get(d, 0.0)), float(by_date_ofe_tx.get(d, 0.0)))
        rows.append({
            "Data do Culto": d,
            "Dízimo": dz,
            "Oferta": ofe,
            "Total": dz + ofe,
        })
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
            principal_totals = _collect_month_data(c.id, start, end, sub_cong_id=None)["totals"]
            rows_data.append({
                "unidade_display": f"{c.name} (Principal)",
                "valor": float(principal_totals["entradas_total_sem_missoes"]),
                "cong_id": c.id,
                "cong_name": c.name,
                "sub_id": None,
                "is_sub": False
            })
            
            # Dados das sub-congregações
            sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == c.id)).all()
            for sub in sub_congs:
                sub_totals = _collect_month_data(c.id, start, end, sub_cong_id=sub.id)["totals"]
                rows_data.append({
                    "unidade_display": f"↳ {sub.name}",
                    "valor": float(sub_totals["entradas_total_sem_missoes"]),
                    "cong_id": c.id,
                    "cong_name": c.name,
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
            # df_view["Total (R$)"] está formatado como string pelo style; compute pelo df_full
            total_geral = df_full["valor"].sum()
        st.metric("Total Geral de Entradas (todas as unidades)", format_currency(total_geral))

        # REMOVIDO: Botão de salvar e sua lógica

def _editor_saidas_agg_all(congs_all: List[Congregation], start: date, end: date):
    with SessionLocal() as db:
        rows = []
        for c in congs_all:
            totals = _collect_month_data(c.id, start, end)["totals"]
            rows.append({"Congregação": c.name, "Total Saídas (R$)": float(totals["saidas_total"])})
        df_view = pd.DataFrame(rows).sort_values("Total Saídas (R$)", ascending=False).reset_index(drop=True)

    # ALTERADO: st.data_editor virou st.dataframe
    st.dataframe(
        df_view.style.format({"Total Saídas (R$)": format_currency}),
        use_container_width=True, 
        hide_index=True
    )
    # REMOVIDO: Botão de salvar e toda sua lógica

# ===================== FUNÇÃO DE COLETA GERAL PARA IA DA SEDE =====================
# ===================== FUNÇÃO DE COLETA DETALHADA PARA IA DA SEDE =====================
@st.cache_data(ttl=3600)  # Cache 1h
def get_all_aggregated_data_for_ia():
    """
    Retorna DataFrame com linhas agregadas por congregação/ano/mês,
    incluindo as fontes:
      - Transactions (categorias)
      - Tithes (dízimos nominais)
      - ServiceLog.oferta (resumo do culto)
    Além disso, cria uma coluna 'Categoria' padronizada e 'Valor'.
    """
    with SessionLocal() as db:
        # 1) Transactions (Entradas e Saídas) por cong/ano/mes/categoria
        q_tx = select(
            Congregation.name.label("Congregacao"),
            func.extract('year', Transaction.date).label("Ano"),
            func.extract('month', Transaction.date).label("Mes"),
            Transaction.type.label("Tipo"),
            Category.name.label("Categoria"),
            func.coalesce(func.sum(Transaction.amount), 0.0).label("Valor")
        ).join(Congregation).join(Category).group_by(
            Congregation.name,
            func.extract('year', Transaction.date),
            func.extract('month', Transaction.date),
            Transaction.type,
            Category.name
        )
        df_tx = pd.read_sql(q_tx, db.bind)

        # 2) Tithes (dízimos nominais) por cong/ano/mes
        q_tithe = select(
            Congregation.name.label("Congregacao"),
            func.extract('year', Tithe.date).label("Ano"),
            func.extract('month', Tithe.date).label("Mes"),
            func.literal("DOAÇÃO").label("Tipo"),
            func.literal("Dízimo Nominal").label("Categoria"),
            func.coalesce(func.sum(Tithe.amount), 0.0).label("Valor")
        ).join(Congregation).group_by(
            Congregation.name,
            func.extract('year', Tithe.date),
            func.extract('month', Tithe.date)
        )
        df_tithe = pd.read_sql(q_tithe, db.bind)

        # 3) ServiceLog.oferta por cong/ano/mes (Resumo do Culto — fonte importante para Ofertas)
        q_sl_ofe = select(
            Congregation.name.label("Congregacao"),
            func.extract('year', ServiceLog.date).label("Ano"),
            func.extract('month', ServiceLog.date).label("Mes"),
            func.literal("DOAÇÃO").label("Tipo"),
            func.literal("Oferta (ResumoCulto)").label("Categoria"),
            func.coalesce(func.sum(ServiceLog.oferta), 0.0).label("Valor")
        ).join(Congregation).group_by(
            Congregation.name,
            func.extract('year', ServiceLog.date),
            func.extract('month', ServiceLog.date)
        )
        df_sl_ofe = pd.read_sql(q_sl_ofe, db.bind)

        # 4) Combina tudo
        df_final = pd.concat([df_tx, df_tithe, df_sl_ofe], ignore_index=True, sort=False)

        # Normaliza colunas (string safe)
        if not df_final.empty:
            df_final["Categoria"] = df_final["Categoria"].astype(str)
            df_final["Valor"] = df_final["Valor"].astype(float)
            df_final["Ano"] = df_final["Ano"].astype(int)
            df_final["Mes"] = df_final["Mes"].astype(int)

        return df_final


# ===================== FUNÇÃO DE RESUMO RÁPIDO PARA DASHBOARD =====================
# ===================== FUNÇÃO DE RESUMO RÁPIDO PARA DASHBOARD =====================
@st.cache_data(ttl=600)
def get_dashboard_summary(cong_id: int, start: date, end: date):
    """
    Busca e calcula os 5 totais financeiros essenciais para uma congregação e período.
    Corrigida para *excluir* ofertas do 'Culto de Missões' do fluxo operacional.
    """
    with SessionLocal() as db:
        # 1) Total de Saídas (fluxo operacional)
        q_saidas = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type == TYPE_OUT  # 'SAÍDA'
        )
        total_saida = float(db.scalar(q_saidas) or 0.0)

        # 2) Total de Ofertas (usa MAIOR entre Transações[Oferta] e ServiceLog.oferta)
        # 2a. Transações categorizadas como "Oferta"
        q_ofertas_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type.in_((TYPE_IN, "RECEITA")),
            func.lower(func.replace(Category.name, " ", "")) == "oferta"
        )
        total_oferta_tx = float(db.scalar(q_ofertas_tx) or 0.0)

        # 2b. Ofertas no ServiceLog (EXCLUINDO Culto de Missões)
        q_ofertas_sl = select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
            ServiceLog.congregation_id == cong_id,
            ServiceLog.date >= start, ServiceLog.date < end,
            ServiceLog.service_type != "Culto de Missões"  # <<< filtro CRÍTICO
        )
        total_oferta_sl = float(db.scalar(q_ofertas_sl) or 0.0)

        # Usa a fonte mais representativa (equivalência de fontes)
        total_oferta = max(total_oferta_tx, total_oferta_sl)

        # 3) Total de Dízimos: usa MAIOR entre Transações[Dízimo] e Dízimos Nominais (Tithe)
        q_dizimos_trans = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            func.lower(func.replace(Category.name, " ", "")) == "dizimo"
        )
        total_dizimo_transacao = float(db.scalar(q_dizimos_trans) or 0.0)

        q_dizimos_nominal = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
            Tithe.congregation_id == cong_id,
            Tithe.date >= start, Tithe.date < end
        )
        total_dizimo_nominal = float(db.scalar(q_dizimos_nominal) or 0.0)

        total_dizimo = max(total_dizimo_transacao, total_dizimo_nominal)

        # 4) Agregados para o painel
        total_dizimo_mais_oferta = total_dizimo + total_oferta
        saldo = total_dizimo_mais_oferta - total_saida

        return {
            "total_saida": total_saida,
            "total_oferta": total_oferta,
            "total_dizimo": total_dizimo,
            "total_dizimo_mais_oferta": total_dizimo_mais_oferta,
            "saldo": saldo,
        }

 

@st.cache_data
# 1. O parâmetro 'db' foi REMOVIDO daqui
def _collect_month_data(cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None):
    # 2. Adicionamos esta linha para criar a conexão DENTRO da função
    with SessionLocal() as db:
        # 3. Todo o código original foi recuado para ficar dentro do 'with'
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

        # ServiceLog (para somar ofertas do resumo do culto)
        sl_query = select(ServiceLog).where(
            ServiceLog.date >= start, ServiceLog.date < end,
            ServiceLog.congregation_id == cong_id
        )

        if sub_cong_id is not None:
            tx_in_query = tx_in_query.where(Transaction.sub_congregation_id == sub_cong_id)
            tithes_query = tithes_query.where(Tithe.sub_congregation_id == sub_cong_id)
            tx_out_query = tx_out_query.where(Transaction.sub_congregation_id == sub_cong_id)
            sl_query = sl_query.where(ServiceLog.sub_congregation_id == sub_cong_id)
        else:
            tx_in_query = tx_in_query.where(Transaction.sub_congregation_id.is_(None))
            tithes_query = tithes_query.where(Tithe.sub_congregation_id.is_(None))
            tx_out_query = tx_out_query.where(Transaction.sub_congregation_id.is_(None))
            sl_query = sl_query.where(ServiceLog.sub_congregation_id.is_(None))
        
        tx_in = db.scalars(tx_in_query.order_by(Transaction.date)).all()
        tithes = db.scalars(tithes_query.order_by(Tithe.date)).all()
        tx_out = db.scalars(tx_out_query.order_by(Transaction.date)).all()
        sls = db.scalars(sl_query.order_by(ServiceLog.date)).all()

        def _is_dizimo_tx(t: Transaction) -> bool:
            return t.category and _norm(t.category.name) in ("dizimo", "dízimo")
        def _is_oferta_tx(t: Transaction) -> bool:
            return t.category and _norm(t.category.name) == "oferta"
        def _is_mission_entry(t: Transaction) -> bool:
            return t.category and _norm(t.category.name) in ("missoes","missões")

        total_dizimos_tithe = sum(float(t.amount) for t in tithes)
        total_dizimos_trans = sum(float(t.amount) for t in tx_in if _is_dizimo_tx(t))
        total_dizimos_final = max(total_dizimos_tithe, total_dizimos_trans)

        # Soma das ofertas por transação no mês
        total_ofertas_tx = sum(float(t.amount) for t in tx_in if _is_oferta_tx(t))
        # Soma das ofertas registradas no ServiceLog (Resumo do Culto) no mês
        total_ofertas_sl = sum(float(s.oferta or 0.0) for s in sls)
        # Regra: total_ofertas = MAIOR entre as fontes (soma mensal)
        total_ofertas = max(total_ofertas_sl, total_ofertas_tx)

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

    
 
@st.cache_data(ttl=600)
def build_ai_month_df(cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None) -> pd.DataFrame:
    """
    Monta a tabela diária do mês para a IA com:
      - Data do Culto
      - Dízimo  (MAIOR entre dízimo nominal e dízimo em transações)
      - Oferta  (MAIOR entre Oferta do Resumo do Culto e Oferta em transações)
      - Total   (Dízimo + Oferta)
    Observação: Missões ficam de fora (sua categoria separada).
    """
    with SessionLocal() as db:
        # Filtros de sub/ principal
        if sub_cong_id is None:
            sl_sub = ServiceLog.sub_congregation_id.is_(None)
            tx_sub = Transaction.sub_congregation_id.is_(None)
            tt_sub = Tithe.sub_congregation_id.is_(None)
        else:
            sl_sub = (ServiceLog.sub_congregation_id == sub_cong_id)
            tx_sub = (Transaction.sub_congregation_id == sub_cong_id)
            tt_sub = (Tithe.sub_congregation_id == sub_cong_id)

        # ---- OFERTA (duas fontes equivalentes) ----
        # 1) Ofertas lançadas no Resumo do Culto (ServiceLog.oferta)
        sl_oferta_q = (
            select(ServiceLog.date, func.coalesce(func.sum(ServiceLog.oferta), 0.0))
            .where(
                ServiceLog.congregation_id == cong_id,
                ServiceLog.date >= start, ServiceLog.date < end,
                sl_sub
            )
            .group_by(ServiceLog.date)
        )
        sl_oferta = {d: float(v or 0.0) for d, v in db.execute(sl_oferta_q).all()}

        # 2) Ofertas lançadas como Transação (Categoria = "Oferta")
        tx_oferta_q = (
            select(Transaction.date, func.coalesce(func.sum(Transaction.amount), 0.0))
            .join(Category)
            .where(
                Transaction.congregation_id == cong_id,
                Transaction.date >= start, Transaction.date < end,
                Transaction.type.in_((TYPE_IN, "RECEITA")),
                func.lower(Category.name) == "oferta",
                tx_sub
            )
            .group_by(Transaction.date)
        )
        tx_oferta = {d: float(v or 0.0) for d, v in db.execute(tx_oferta_q).all()}

        # ---- DÍZIMO (sua regra de equivalência) ----
        # 1) Dízimo nominal (Tithe)
        tt_diz_q = (
            select(Tithe.date, func.coalesce(func.sum(Tithe.amount), 0.0))
            .where(
                Tithe.congregation_id == cong_id,
                Tithe.date >= start, Tithe.date < end,
                tt_sub
            )
            .group_by(Tithe.date)
        )
        tt_diz = {d: float(v or 0.0) for d, v in db.execute(tt_diz_q).all()}

        # 2) Dízimo em Transações (Categoria = "Dízimo")
        tx_diz_q = (
            select(Transaction.date, func.coalesce(func.sum(Transaction.amount), 0.0))
            .join(Category)
            .where(
                Transaction.congregation_id == cong_id,
                Transaction.date >= start, Transaction.date < end,
                Transaction.type.in_((TYPE_IN, "RECEITA")),
                func.lower(Category.name).in_(("dízimo", "dizimo")),
                tx_sub
            )
            .group_by(Transaction.date)
        )
        tx_diz = {d: float(v or 0.0) for d, v in db.execute(tx_diz_q).all()}

        # ---- Montagem por dia (evita duplicidade) ----
        # Oferta: MAIOR entre (ServiceLog) x (Transação) — são o MESMO conceito por caminhos diferentes
        # Dízimo: MAIOR entre nominal x transação (sua regra original)
        all_dates = sorted(set(sl_oferta) | set(tx_oferta) | set(tt_diz) | set(tx_diz))

        rows = []
        for d in all_dates:
            diz = max(float(tt_diz.get(d, 0.0)), float(tx_diz.get(d, 0.0)))
            ofe = max(float(sl_oferta.get(d, 0.0)), float(tx_oferta.get(d, 0.0)))
            rows.append({
                "Data do Culto": d,
                "Dízimo": diz,
                "Oferta": ofe,
                "Total": diz + ofe
            })

        return pd.DataFrame(rows, columns=["Data do Culto", "Dízimo", "Oferta", "Total"])



# COLE ESTAS DUAS FUNÇÕES NO SEU CÓDIGO, ANTES DA "page_lancamentos"

# APAGUE AS FUNÇÕES _load_multi_service_data e _apply_multi_service_changes E SUBSTITUA POR ESTAS

# SUBSTITUA SUA FUNÇÃO _load_service_logs INTEIRA POR ESTA VERSÃO CORRIGIDA
@st.cache_data
def _load_service_logs(cong_id: int, start: date, end: date, sub_cong_id: Optional[int] = None) -> pd.DataFrame:
    """Carrega os resumos de culto para a tabela de edição, com ordenação customizada."""
    with SessionLocal() as db:
        # filtro base (tratando None explicitamente para evitar 'col = NULL' em SQL)
        filters = [
            ServiceLog.congregation_id == cong_id,
            ServiceLog.date >= start,
            ServiceLog.date < end,
        ]
        if sub_cong_id is None:
            filters.append(ServiceLog.sub_congregation_id.is_(None))
        else:
            filters.append(ServiceLog.sub_congregation_id == sub_cong_id)

        # ordenação customizada por tipo de culto (mantém data como principal)
        from sqlalchemy import case
        custom_sort_order = case(
            (ServiceLog.service_type == "Trabalhos pela Manhã (EBD, CO, FESTIVIDADES)", 1),
            (ServiceLog.service_type == "Culto da Noite (Padrão)", 2),
            (ServiceLog.service_type == "Evento Especial", 3),
            else_=4
        )

        query = select(ServiceLog).where(and_(*filters)).order_by(ServiceLog.date, custom_sort_order)
        logs = db.scalars(query).all()

        if not logs:
            return pd.DataFrame()

        data = []
        for log in logs:
            data.append({
                "ID": log.id,
                "Data do Culto": log.date,
                "Tipo de Culto": log.service_type,
                "Dízimo": float(log.dizimo or 0.0),
                "Oferta": float(log.oferta or 0.0),
                "Total": float((log.dizimo or 0.0) + (log.oferta or 0.0))
            })
        return pd.DataFrame(data)


# Substitua esta função inteira
# Substitua sua função _apply_service_log_changes inteira por esta
# Substitua sua função _apply_service_log_changes inteira por esta
# Substitua sua função _apply_service_log_changes inteira por esta
def _apply_service_log_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, cong_id: int, sub_cong_id: Optional[int] = None) -> str:
    """
    Aplica as mudanças na tabela service_logs e retorna um status da operação.
    Status possíveis: "missao_ok", "geral_ok", "erro_integridade", "erro_categoria", "erro_geral"
    """
    oferta_de_missao_processada = False
    df_para_salvar = edited_df.copy()

    with SessionLocal() as db:
        cat_missoes = db.scalar(select(Category).where(func.lower(Category.name) == 'missões', Category.type == TYPE_IN))
        if not cat_missoes:
            return "erro_categoria"

        for index, row in df_para_salvar.iterrows():
            tipo_culto = str(row.get("Tipo de Culto", ""))
            oferta_valor = _to_float_brl(row.get("Oferta", 0.0))

            if tipo_culto == "Culto de Missões" and oferta_valor > 0:
                db.add(Transaction(
                    date=_to_date(row["Data do Culto"]), type=TYPE_IN,
                    category_id=cat_missoes.id, amount=oferta_valor,
                    description="Oferta do Culto de Missões (lançada via tabela)",
                    congregation_id=cong_id, sub_congregation_id=sub_cong_id
                ))
                df_para_salvar.loc[index, 'Oferta'] = 0.0
                oferta_de_missao_processada = True

        orig_ids = set(orig_df['ID'].dropna())
        edited_ids = set(df_para_salvar['ID'].dropna())
        to_delete = orig_ids - edited_ids
        to_update = orig_ids.intersection(edited_ids)

        if to_delete:
            db.query(ServiceLog).filter(ServiceLog.id.in_(to_delete)).delete(synchronize_session=False)

        for log_id in to_update:
            log = db.get(ServiceLog, int(log_id))
            if log:
                row = df_para_salvar[df_para_salvar['ID'] == log_id].iloc[0]
                log.date = _to_date(row["Data do Culto"])
                log.service_type = str(row["Tipo de Culto"])
                log.dizimo = _to_float_brl(row["Dízimo"])
                log.oferta = _to_float_brl(row["Oferta"])

        new_rows = df_para_salvar[df_para_salvar['ID'].isna()]
        for _, row in new_rows.iterrows():
            if _to_float_brl(row["Dízimo"]) > 0 or _to_float_brl(row["Oferta"]) > 0:
                db.add(ServiceLog(
                    date=_to_date(row["Data do Culto"]), service_type=str(row["Tipo de Culto"]),
                    dizimo=_to_float_brl(row["Dízimo"]), oferta=_to_float_brl(row["Oferta"]),
                    congregation_id=cong_id, sub_congregation_id=sub_cong_id
                ))
        
        try:
            db.commit()
            return "missao_ok" if oferta_de_missao_processada else "geral_ok"
        except IntegrityError:
            db.rollback()
            return "erro_integridade"
        except Exception:
            db.rollback()
            return "erro_geral"

# ===================== PAGE: LANÇAMENTOS (com modo Tabela fora do form) =====================
# APAGUE SUA FUNÇÃO page_lancamentos ANTIGA E SUBSTITUA POR ESTA VERSÃO FINAL


# ===================== PAGE: LANÇAMENTOS (com modo Tabela fora do form) =====================
# APAGUE SUA FUNÇÃO page_lancamentos ANTIGA E SUBSTITUA POR ESTA VERSÃO FINAL

def page_lancamentos(user: "User"):
    ensure_seed()

    # Variáveis de Estado
    if "rap_dizimo_lote" not in st.session_state:
        st.session_state.rap_dizimo_lote = ""
    # Variável para controlar se o modo avançado está aberto/ativo
    if "is_advanced_mode_active" not in st.session_state:
        st.session_state.is_advanced_mode_active = False

    # === CORREÇÃO DO ATTRIBUTE ERROR: Inicializa a chave do toggle ===
    if "advanced_mode_toggle" not in st.session_state:
        st.session_state.advanced_mode_toggle = False
    
    # Reset do modo avançado no carregamento da página
    st.session_state.is_advanced_mode_active = False

    # Mensagens persistidas entre reruns
    if 'status_message' in st.session_state:
        msg_type, msg_text = st.session_state.status_message
        if msg_type == "success":
            st.success(msg_text)
        elif msg_type == "error":
            st.error(msg_text)
        elif msg_type == "warning":
            st.warning(msg_text)
        del st.session_state.status_message
        
    # === CALLBACK PARA FORÇAR O RERUN EM 1 CLIQUE ===
    def set_advanced_mode_and_rerun_direct():
        # A chave 'advanced_mode_toggle' já foi atualizada pelo st.toggle no clique.
        # Define nossa variável de controle com o valor atual do widget e força o rerun.
        st.session_state.is_advanced_mode_active = st.session_state.advanced_mode_toggle
        st.rerun()

    with SessionLocal() as db:
        # === CONFIRMAÇÃO DE AMBIENTE (PARA TESTE LOCAL) ===
        if not os.environ.get("DATABASE_URL"):
            st.sidebar.info("Ambiente: DESENVOLVIMENTO (SQLite Local)")
        # =================================================

        st.markdown(f"<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)

        # ================== SELEÇÃO DA CONGREGAÇÃO (SEDE pode operar todas) ==================
        parent_cong_obj = None

        if getattr(user, "role", "") == "SEDE":
            # Lista completa das congregações que o usuário SEDE pode operar (mantendo sua ordenação especial)
            congs_all = order_congs_sede_first(cong_options_for(user, db))
            cong_names = [c.name for c in congs_all] or ["—"]

            # Index default reaproveitando seleção anterior, se existir
            default_index = 0
            prev_name = st.session_state.get("lan_cong_sel_sede_name")
            if prev_name and prev_name in cong_names:
                default_index = cong_names.index(prev_name)

            cong_sel_name = st.selectbox(
                "Selecione a Congregação para lançar/editar:",
                cong_names,
                index=default_index,
                key="lan_cong_sel_sede",
                help="Conta SEDE: escolha a congregação que deseja operar."
            )
            # Persiste a escolha para os próximos reruns
            st.session_state.lan_cong_sel_sede_name = cong_sel_name

            parent_cong_obj = next((c for c in congs_all if c.name == cong_sel_name), None)
        else:
            # Perfil não-SEDE: trava na congregação do usuário
            parent_cong_obj = db.get(Congregation, getattr(user, "congregation_id", None))

        if not parent_cong_obj:
            st.error("Nenhuma congregação selecionada ou encontrada.")
            return

        st.markdown(f"### CONGREGAÇÃO: {parent_cong_obj.name.upper()}")

        # --- Configuração de Contexto ---
        sub_congs = db.scalars(
            select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id)
        ).all()
        
        # LISTA DE TIPOS DE CULTO ATUALIZADA
        tipos_de_culto = [
            "Culto de Oração (Ensino)",
            "Culto Público",
            "Trabalhos pela Manhã (EBD, CO, FESTIVIDADES)",
            "Culto de Missões",
            "Evento Especial",
            "Outro"
        ]
        
        # Variáveis de contexto padrão
        target_cong_obj = parent_cong_obj
        target_sub_cong_id = None
        contexto_selecionado = f"{parent_cong_obj.name} (Principal)"
        contexto_tabela = f"{parent_cong_obj.name} (Principal)"

        if sub_congs:
            opcoes = {f"{parent_cong_obj.name} (Principal)": None}
            for sub in sub_congs:
                opcoes[sub.name] = sub.id

            # Seletor da unidade (só para o modo rápido)
            if not st.session_state.advanced_mode_toggle:
                contexto_selecionado = st.selectbox(
                    "Lançar em:", list(opcoes.keys()), key="lan_sub_sel_rapido_contexto"
                )
                target_sub_cong_id = opcoes[contexto_selecionado]
        
        st.divider()

        # ====================== SELETORES DE CONTEXTO (MOVIDOS PARA O TOPO) ======================
        # Estes seletores definem o contexto para as Seções 1, 2 e 3
        col_data, col_tipo = st.columns(2)
        with col_data:
            rap_data = st.date_input(
                "Data do Culto:",
                value=today_bahia(),
                key="rap_data_unica_sel",
                format="DD/MM/YYYY"
            )
        with col_tipo:
            ent_tipo = st.selectbox(
                "Tipo de Culto", options=tipos_de_culto, key="rap_ent_tipo"
            )
        st.caption(f"Contexto de Lançamento: **{contexto_selecionado}** | Data: **{format_date(rap_data)}**")
        st.divider()
        # =========================================================================================

        # ====================== 1. LANÇAMENTO RÁPIDO (MÓVEL) - FLUXO PADRÃO =======================
        
        # SÓ MOSTRA O BLOCO RÁPIDO SE O MODO AVANÇADO NÃO ESTIVER ATIVO
        if not st.session_state.advanced_mode_toggle:
            
            # --- 1. Lançar Ofertas e Resumo do Culto ---
            st.markdown("##### 1. Lançar Ofertas e Resumo do Culto")
            with st.form("form_oferta_rapida"):

                # Segunda linha: Dízimo e Oferta
                c1, c2 = st.columns(2)
                ent_dizimo = c1.number_input(
                    "Total Dízimo (Culto)", min_value=0.0, value=0.0, format="%.2f",
                    key="rap_ent_diz"
                )
                ent_oferta = c2.number_input(
                    "Total Oferta (Culto)", min_value=0.0, value=0.0, format="%.2f",
                    key="rap_ent_ofe"
                )

                # Botão Salvar (AZUL)
                if _submit_btn("Salvar Ofertas e Resumo do Culto",
                               key_suffix="salvar_ofe_rapida", theme="entrada"):
                    if ent_dizimo <= 0 and ent_oferta <= 0:
                        st.session_state.status_message = ("warning", "Nenhum valor foi inserido.")
                    else:
                        try:
                            # Lógica reaproveitada do Formulário Único para o ServiceLog
                            log_existente = db.scalar(
                                select(ServiceLog).where(
                                    ServiceLog.date == rap_data,
                                    ServiceLog.service_type == ent_tipo,
                                    ServiceLog.congregation_id == target_cong_obj.id,
                                    ServiceLog.sub_congregation_id.is_(None)
                                    if target_sub_cong_id is None
                                    else (ServiceLog.sub_congregation_id == target_sub_cong_id)
                                )
                            )

                            if ent_tipo == "Culto de Missões":
                                if ent_oferta > 0:
                                    cat_missoes = db.scalar(
                                        select(Category).where(
                                            func.lower(Category.name) == 'missões',
                                            Category.type == TYPE_IN
                                        )
                                    )
                                    if cat_missoes:
                                        db.add(Transaction(
                                            date=rap_data, type=TYPE_IN,
                                            category_id=cat_missoes.id,
                                            amount=float(ent_oferta),
                                            description="Oferta do Culto de Missões",
                                            congregation_id=target_cong_obj.id,
                                            sub_congregation_id=target_sub_cong_id
                                        ))
                                    else:
                                        st.session_state.status_message = (
                                            "error",
                                            "ERRO: Categoria 'Missões' não encontrada."
                                        )
                                        db.rollback()
                                        st.rerun()

                                if log_existente:
                                    log_existente.dizimo = (log_existente.dizimo or 0.0) + float(ent_dizimo)
                                else:
                                    db.add(ServiceLog(
                                        date=rap_data, service_type=ent_tipo,
                                        dizimo=float(ent_dizimo), oferta=0.0,
                                        congregation_id=target_cong_obj.id,
                                        sub_congregation_id=target_sub_cong_id
                                    ))
                                st.session_state.status_message = (
                                    "success",
                                    "Atenção: Ofertas de Missões lançadas em transação."
                                )
                            else:
                                if log_existente:
                                    log_existente.dizimo = (log_existente.dizimo or 0.0) + float(ent_dizimo)
                                    log_existente.oferta = (log_existente.oferta or 0.0) + float(ent_oferta)
                                else:
                                    db.add(ServiceLog(
                                        date=rap_data, service_type=ent_tipo,
                                        dizimo=float(ent_dizimo), oferta=float(ent_oferta),
                                        congregation_id=target_cong_obj.id,
                                        sub_congregation_id=target_sub_cong_id
                                    ))
                                st.session_state.status_message = ("success", "Registro de culto salvo com sucesso!")

                            try:
                                db.commit()
                                st.cache_data.clear()
                            except IntegrityError as ie:
                                db.rollback()
                                st.session_state.status_message = ("error", "Erro de integridade: lançamento duplicado.")
                            except Exception as e:
                                db.rollback()
                                st.session_state.status_message = ("error", f"Erro inesperado: {str(e)}")
                        except Exception as e:
                            try:
                                db.rollback()
                            except Exception:
                                pass
                            st.session_state.status_message = ("error", f"Erro ao processar entrada: {e}")

                    st.rerun()

            st.divider()
            
            # --- 2. Lançamento Rápido de Dízimos ---
            
            # Chama a função que gerencia as 2 etapas (Entrada de Texto -> Tabela Editável -> Salvar)
            render_dizimos_lote_section(
                default_payment=st.session_state.get("rap_diz_default_pay", "Dinheiro"), # Passa o valor atual
                rap_data=rap_data,
                target_cong_obj=target_cong_obj,
                target_sub_cong_id=target_sub_cong_id
            )

            # O SELETOR DE PAGAMENTO VEM ABAIXO DA CAIXA DE TEXTO LIVRE
            default_payment = st.selectbox(
                "Forma de Pagamento (Padrão para Lote):",
                ["PIX", "Dinheiro"], 
                key="rap_diz_default_pay", 
                index=1, # Default para Dinheiro (index 1)
                help="Selecione se os dízimos do lote foram pagos primariamente via PIX ou Dinheiro."
            )
            
            st.divider()
            
            # --- 3. Lançar Pagamento / Despesa (Saída Rápida) ---
            st.markdown("##### 3. Lançar Pagamento / Despesa")
            with st.form("form_saida_rapida", clear_on_submit=True):

                with SessionLocal() as db_form:
                    # Busca TODAS as categorias de SAÍDA e as mais comuns
                    cats_out_all = categories_for_type(db_form, "SAÍDA")
                    # Filtra para mostrar apenas categorias mais comuns no topo (TOP 5)
                    cats_frequentes_nomes = [c.name for c in cats_out_all][:5] or ["—"]
                    cats_map = {c.name: c.id for c in cats_out_all}
                    
                    # Cria a lista completa (incluindo o "Outra categoria...")
                    cats_out_others = [c.name for c in cats_out_all if c.name not in cats_frequentes_nomes]
                    select_options = ["Selecione outra categoria..."] + cats_out_others

                
                st.markdown("**1. Selecione a Categoria (Despesa)**")
                
                col_radio, col_select = st.columns([1.5, 1.5])

                # 1. Opções Rápidas (Radio) - MODO MAIS FÁCIL PARA CELULAR
                with col_radio:
                    selected_radio = st.radio(
                        "Categoria Comum:", 
                        options=cats_frequentes_nomes, 
                        key="sai_rap_cat_radio"
                    )

                # 2. Opção Lenta (Selectbox)
                with col_select:
                    selected_select = st.selectbox(
                        "Ou, selecione Outra:", 
                        options=select_options, 
                        key="sai_rap_cat_select"
                    )

                # Define a categoria final
                if selected_radio and selected_radio != "Selecione...":
                    sai_cat_name = selected_radio
                elif selected_select and selected_select != "Selecione outra categoria...":
                    sai_cat_name = selected_select
                else:
                    sai_cat_name = None
                    
                st.markdown("---")
                st.markdown("**2. Insira Valor e Descrição**")

                c1, c2 = st.columns(2)
                with c1:
                    # CORRIGIDO: Usando st.text_input para permitir vírgula (,)
                    sai_valor_str = st.text_input(
                        "Valor (R$)", 
                        value="0,00",
                        key="sai_rap_valor_str",
                        help="Use a vírgula (,) para separar os centavos. Ex: 100,50"
                    )
                with c2:
                    sai_desc = st.text_input("Descrição (Opcional, mas recomendado)", key="sai_rap_desc")

                # Botão Salvar (AZUL)
                if _submit_btn("Salvar Pagamento", key_suffix="salvar_saida_rapida", theme="saida"):
                    cat_id = cats_map.get(sai_cat_name)
                    # CONVERSÃO: Converte a string (com vírgula) para float
                    valor_valido = _to_float_brl(sai_valor_str)

                    if valor_valido <= 0.0:
                        st.error("O valor deve ser maior que zero.")
                    elif cat_id is None:
                        st.error("Selecione uma categoria válida para a despesa.")
                    else:
                        try:
                            with SessionLocal() as db_tx:
                                db_tx.add(Transaction(
                                    date=rap_data, type="SAÍDA", category_id=cat_id,
                                    amount=valor_valido, description=(sai_desc or None),
                                    congregation_id=target_cong_obj.id,
                                    sub_congregation_id=target_sub_cong_id
                                ))
                                db_tx.commit()
                                st.session_state.status_message = (
                                    "success",
                                    f"Pagamento '{sai_cat_name}' de {format_currency(valor_valido)} registrado com sucesso!"
                                )
                                st.cache_data.clear()
                                st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao salvar pagamento: {e}")

        
        # ====================== 4. EDIÇÃO AVANÇADA / TABELA (No final da página) =======================
        
        st.markdown("---")
        st.subheader("🛠️ Tabela de Edição Avançada")
        st.caption("Use esta seção apenas para corrigir ou ajustar dados diretamente nas tabelas.")
        
        # 1. Toggle/Opção de escolha
        col_toggle, col_empty = st.columns([1, 3])
        with col_toggle:
            # st.toggle para ativar/desativar o modo avançado
            st.toggle(
                "Ativar Edição Avançada",
                value=st.session_state.advanced_mode_toggle,
                key="advanced_mode_toggle",
            )
            
        
        if st.session_state.advanced_mode_toggle: # Usa o valor da key do toggle para controle
            
            st.markdown("---")
            
            # --- Configura o contexto da tabela ---
            if sub_congs:
                opcoes_tabela = {f"{parent_cong_obj.name} (Principal)": None}
                for sub in sub_congs:
                    opcoes_tabela[sub.name] = sub.id
                contexto_tabela = st.selectbox(
                    "Selecione a unidade para editar:",
                    list(opcoes_tabela.keys()),
                    key="lan_tabela_contexto_avancado"
                )
                target_sub_cong_id_expander = opcoes_tabela[contexto_tabela]
            else:
                # No modo avançado, se não houver sub, usa a congregação principal
                target_sub_cong_id_expander = None
            
            st.info(f"Editando lançamentos de: **{contexto_tabela}**")

            ref_tab = get_month_selector("Mês de referência da tabela")
            start_tab, end_tab = month_bounds(ref_tab)

            # --- SEPARADOR CLARO ---
            st.markdown("---")
            st.subheader("1. Edição de Entradas por Culto (Resumo)")

            # _load_service_logs deve usar o target_sub_cong_id correto, que já foi definido
            df_logs = _load_service_logs(
                parent_cong_obj.id, start_tab, end_tab, sub_cong_id=target_sub_cong_id_expander
            )

            # --- Placeholder do aviso (fica ACIMA visualmente da tabela) ---
            _aviso_top = st.empty()

            edited_df = st.data_editor(
                df_logs, use_container_width=True, hide_index=True, num_rows="dynamic",
                key=f"editor_service_logs_adv_{parent_cong_obj.id}_{target_sub_cong_id_expander}",
                column_config={
                    "ID": None,
                    "Data do Culto": st.column_config.DateColumn("Data do Culto", required=True, format="DD/MM/YYYY"),
                    "Tipo de Culto": st.column_config.SelectboxColumn("Tipo de Culto", options=tipos_de_culto, required=True),
                    "Dízimo": st.column_config.NumberColumn("Dízimo", format="R$ %.2f", required=True),
                    "Oferta": st.column_config.NumberColumn("Oferta", format="R$ %.2f", required=True),
                    "Total": st.column_config.NumberColumn(
                        "Total", help="Soma do Dízimo e Oferta. Atualiza após salvar.",
                        format="R$ %.2f", disabled=True
                    )
                },
                column_order=["Data do Culto", "Tipo de Culto", "Dízimo", "Oferta", "Total"]
            )

            # AVISO AMARELO: aparece se existir "Culto de Missões" na tabela
            try:
                if _has_culto_missoes_in_df(edited_df):
                    with _aviso_top:
                        _render_aviso_missoes_inline()
            except Exception:
                pass

            st.divider()
            # Totais rápidos da tabela
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

            # Botão salvar mudanças do resumo (AZUL)
            def on_save_click():
                result = _apply_service_log_changes(
                    df_logs, edited_df, parent_cong_obj.id, sub_cong_id=target_sub_cong_id_expander
                )
                try:
                    st.cache_data.clear()
                except Exception:
                        pass
                if result == "missao_ok":
                    st.session_state.status_message = (
                        "success",
                        "Atenção: As ofertas do Culto de Missões são lançadas automaticamente no menu 'Relatório de Missões'."
                    )
                elif result == "geral_ok":
                    st.session_state.status_message = ("success", "Alterações salvas com sucesso!")
                elif result == "erro_integridade":
                    st.session_state.status_message = ("error", "Erro: Tentativa de criar um lançamento duplicado. Verifique os dados.")
                elif result == "erro_categoria":
                    st.session_state.status_message = ("error", "ERRO CRÍTICO: Categoria 'Missões' (Entrada) não encontrada.")
                elif result == "erro_geral":
                    st.session_state.status_message = ("error", "Ocorreu um erro inesperado ao salvar.")
                st.rerun()

            # Utiliza _save_btn para aplicar a cor AZUL (tema "neutral")
            _save_btn(on_save_click, f"save_table_adv_resumo_{parent_cong_obj.id}", theme="neutral",
                         label="Salvar alterações no Resumo de Entradas")

            # --- SEPARADOR CLARO ---
            st.markdown("---")
            st.subheader("2. Edição de Dízimos Nominais")
            
            tithes_query = select(Tithe).where(
                Tithe.congregation_id == parent_cong_obj.id,
                Tithe.date >= start_tab, Tithe.date < end_tab,
                (Tithe.sub_congregation_id.is_(None)
                     if target_sub_cong_id_expander is None
                     else (Tithe.sub_congregation_id == target_sub_cong_id_expander))
            )
            tithes = db.scalars(tithes_query.order_by(Tithe.date)).all()
            _editor_dizimos(
                tithes, f"Dizimistas - {contexto_tabela}",
                force_cong_id=parent_cong_obj.id, force_sub_cong_id=target_sub_cong_id_expander
            )

            # --- SEPARADOR CLARO ---
            st.markdown("---")
            st.subheader("3. Edição de Saídas")
            
            txs_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
                Transaction.congregation_id == parent_cong_obj.id,
                Transaction.date >= start_tab, Transaction.date < end_tab,
                Transaction.type == "SAÍDA",
                (Transaction.sub_congregation_id.is_(None)
                     if target_sub_cong_id_expander is None
                     else (Transaction.sub_congregation_id == target_sub_cong_id_expander))
            )
            txs_out = db.scalars(txs_out_query.order_by(Transaction.date)).all()
            _editor_lancamentos(
                txs_out, f"Saídas - {contexto_tabela}", tx_type_hint="SAÍDA",
                force_cong_id=parent_cong_obj.id, force_sub_cong_id=target_sub_cong_id_expander
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
                totals = _collect_month_data(parent_cong_obj.id, start, end, sub_cong_id=sub_id)["totals"]
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
    data_geral = _collect_month_data(cong_id, start, end, sub_cong_id=sub_cong_id)
    totals_gerais = data_geral["totals"]
    
    # ===== Tabela de Entradas (CORRIGIDA) =====
    story.append(Paragraph("1. Entradas (Resumo por Culto)", heading_style))
    
    # Usa a função correta para buscar os logs de serviço
    df_entradas = _load_service_logs(cong_id, start, end, sub_cong_id=sub_cong_id)
    
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
        
        data = _collect_month_data(parent_cong_obj.id, start, end, sub_cong_id=sub_id)
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
        
        df_principal = _load_service_logs(cong.id, start, end, None)
        principal_entradas = df_principal['Total'].sum() if not df_principal.empty else 0.0
        
        total_subs = 0.0
        subs_data = []
        for sub in sub_congs:
            df_sub = _load_service_logs(cong.id, start, end, sub.id)
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

# ===================== PAGE: RESUMO RÁPIDO =====================
def page_resumo_financeiro(user: "User"):
    st.markdown("<h1 class='page-title'>⚡ Resumo Financeiro Rápido</h1>", unsafe_allow_html=True)

    with SessionLocal() as db:
        congs_all = order_congs_sede_first(cong_options_for(user, db))
        
        # --- Filtros de Congregação e Data ---
        col_cong, col_filtros = st.columns([2, 3])
        with col_cong:
            cong_selecionada_obj = None
            if user.role in ["SEDE", "TESOUREIRO MISSIONÁRIO"]:
                cong_sel_name = st.selectbox("Congregação", [c.name for c in congs_all], key="resumo_cong_sel")
                cong_selecionada_obj = next((c for c in congs_all if c.name == cong_sel_name), None)
            else:
                cong_selecionada_obj = db.get(Congregation, user.congregation_id)
                if cong_selecionada_obj:
                    st.text_input("Congregação", cong_selecionada_obj.name, disabled=True)

        with col_filtros:
            ref = get_month_selector("Mês de Referência", key_prefix="resumo_ref")
        
        start, end = month_bounds(ref)

        if not cong_selecionada_obj:
            st.warning("Nenhuma congregação selecionada.")
            return
            
        st.divider()

        # --- Busca os dados e exibe as métricas ---
        with st.spinner(f"Calculando resumo para {cong_selecionada_obj.name}..."):
            summary = get_dashboard_summary(cong_selecionada_obj.id, start, end)

            st.markdown(f"### Resumo para **{cong_selecionada_obj.name}** em **{ref.strftime('%B de %Y')}**")

            # Layout das métricas
            col1, col2, col3 = st.columns(3)
            col1.metric("Total de Dízimo", format_currency(summary["total_dizimo"]))
            col2.metric("Total de Oferta", format_currency(summary["total_oferta"]))
            col3.metric("Dízimo + Oferta", format_currency(summary["total_dizimo_mais_oferta"]))
            
            st.markdown("<br>", unsafe_allow_html=True) # Espaçamento

            col4, col5 = st.columns(2)
            col4.metric("Total de Saídas", format_currency(summary["total_saida"]), delta_color="inverse")
            col5.metric("Saldo do Mês", format_currency(summary["saldo"]))

# ===================== PAGE: VISÃO GERAL =====================
# ===================== PAGE: VISÃO GERAL =====================
def page_visao_geral(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
        
        # --------------------- NOVO: SEÇÃO DE MENSAGENS INTERNAS (apenas para SEDE) ---------------------
        if user.role == "SEDE":
            st.markdown("### 📩 Enviar Comunicado Interno (SEDE)")
            congs_all = order_congs_sede_first(cong_options_for(user, db))
            congs_sem_sede = [c for c in congs_all if _norm(c.name) != "sede"]
            
            with st.form("form_send_message", clear_on_submit=True):
                # Permite enviar para todas ou escolher uma
                target_options = ["TODAS AS CONGREGAÇÕES"] + [c.name for c in congs_sem_sede]
                target_cong_name = st.selectbox("Destinatário:", target_options, key="msg_target_cong")
                msg_text = st.text_area("Mensagem:", height=100, max_chars=500, key="msg_text")
                
                if st.form_submit_button("✉️ Enviar Mensagem"):
                    if not msg_text.strip():
                        st.error("A mensagem não pode ser vazia.")
                    else:
                        congs_to_send = congs_sem_sede if target_cong_name == "TODAS AS CONGREGAÇÕES" else [next((c for c in congs_sem_sede if c.name == target_cong_name), None)]
                        congs_to_send = [c for c in congs_to_send if c] # Remove None se houver

                        if not congs_to_send:
                            st.error("Nenhum destinatário encontrado.")
                        else:
                            for c in congs_to_send:
                                db.add(InternalMessage(
                                    sender_user_id=user.id,
                                    target_congregation_id=c.id,
                                    message_text=msg_text.strip(),
                                    date_sent=now_bahia()
                                ))
                            db.commit()
                            st.success(f"Mensagem enviada para {len(congs_to_send)} congregação(ões)!")
                            st.rerun()

            st.divider()

        # --------------------- NOVO: VISUALIZAÇÃO DAS MENSAGENS (para Tesoureiros) ---------------------
        if user.role != "SEDE":
            st.markdown("### 🔔 Avisos Recebidos")
            if user.congregation_id:
                # 1. Checa a mensagem mais recente NÃO LIDA
                unread_msg = check_unread_messages(user, db)

                if unread_msg:
                    with st.container(border=True):
                        st.warning(f"📩 **MENSAGEM NÃO LIDA** — Recebida em: {unread_msg.date_sent.strftime('%d/%m/%Y %H:%M')}")
                        st.markdown(f"**De:** SEDE")
                        st.markdown(f"**Mensagem:** {unread_msg.message_text}")
                        
                        if st.button("Marcar como Lida e Arquivar", key=f"mark_read_{unread_msg.id}"):
                            mark_message_as_read(unread_msg.id)
                            st.toast("Mensagem arquivada.", icon="✅")
                            st.rerun()
                    st.divider()

                # 2. Histórico de Mensagens Arquivadas (as que foram lidas ou são antigas)
                q_history = select(InternalMessage).where(
                    InternalMessage.target_congregation_id == user.congregation_id
                ).order_by(InternalMessage.date_sent.desc()).limit(10)

                messages = db.scalars(q_history).all()

                if not messages:
                    st.info("Nenhum aviso no seu histórico.")
                else:
                    st.markdown("##### Histórico Recente (Máximo 10)")
                    
                    df_msg = pd.DataFrame([{
                        "Data": m.date_sent.strftime('%d/%m/%Y'),
                        "Lida": "✅" if m.is_read else "❌",
                        "Mensagem": m.message_text
                    } for m in messages])
                    st.dataframe(df_msg, use_container_width=True, hide_index=True)
            
            st.divider()
        # --------------------- FIM SEÇÃO DE MENSAGENS INTERNAS ---------------------

        # --------------------- CÓDIGO ORIGINAL (continua abaixo) ---------------------
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
                df_entradas = _load_service_logs(cong.id, start, end, sub_id)
                total_dizimos = df_entradas['Dízimo'].sum() if not df_entradas.empty else 0.0
                total_ofertas = df_entradas['Oferta'].sum() if not df_entradas.empty else 0.0
                total_geral_entradas = total_dizimos + total_ofertas

                dados_saidas = _collect_month_data(cong.id, start, end, sub_id)
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
@st.cache_data
# ===================== FUNÇÃO _build_missions_search_df CORRIGIDA =====================
@st.cache_data
# 1. O parâmetro 'db' foi REMOVIDO daqui
def _build_missions_search_df(year: int, month_name: str):
    """
    Busca e agrega as contribuições de missões, identificando o Top 5 de contribuintes.
    """
    # 2. Adicionamos esta linha para criar a conexão DENTRO da função
    with SessionLocal() as db:
        # 3. Todo o código original foi recuado para ficar dentro do 'with'
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
                "Total no Período (R$)": float(period_data.get(cong_name, 0.0) or 0.0),
                "Total no Ano (R$)": float(year_data.get(cong_name, 0.0) or 0.0)
            })

        if not report_rows:
            return pd.DataFrame(), 0.0, 0, pd.DataFrame(), pd.DataFrame()

        df = pd.DataFrame(report_rows)
        total_periodo = df["Total no Período (R$)"].sum()
        num_congs_periodo = len(df[df["Total no Período (R$)"] > 0])

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

            # --- CORREÇÃO DE ORDEM ---
            # PRIMEIRO, a linha que cria a variável df_search
            df_search, total_periodo, num_congs, df_top_period, df_top_year = _build_missions_search_df(ano_pesq, mes_sel)

            # SÓ DEPOIS, o bloco de código que USA a variável df_search
            st.divider()
            
            st.markdown("###### Tabela Geral de Contribuições")
            if not df_search.empty:
                st.dataframe(
                    df_search.style.format({"Total no Período (R$)": format_currency, "Total no Ano (R$)": format_currency}),
                    use_container_width=True, hide_index=True
                )
            else:
                st.caption("Nenhuma contribuição de missões encontrada para os filtros selecionados.")

            st.markdown("##### Destaques do Período Selecionado")
            c1, c2, c3 = st.columns(3)

            # --- CORREÇÃO DO KEYERROR ---
            # Calcula o total do ano de forma segura, verificando se a tabela não está vazia
            total_ano = 0.0
            if not df_search.empty:
                total_ano = df_search["Total no Ano (R$)"].sum()

            c1.metric("Total de Entradas no Mês", format_currency(total_periodo))
            c2.metric("Nº de Congregações Contribuintes (mês)", f"{num_congs}")
            c3.metric("Total de Entradas no Ano", format_currency(total_ano))
            
            st.divider()
            
            st.markdown("##### Maiores Contribuintes")
            col_top1, col_top2 = st.columns(2)
            with col_top1:
                st.markdown(f"**Top 5 ({mes_sel if mes_sel != 'Todos' else 'Período'})**")
                if not df_top_period.empty:
                    st.dataframe(
                        df_top_period[['Congregação', 'Total no Período (R$)']].style.format({"Total no Período (R$)": format_currency}),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.caption("Nenhum contribuinte no período.")
            
            with col_top2:
                st.markdown(f"**Top 5 (Ano de {ano_pesq})**")
                if not df_top_year.empty:
                    st.dataframe(
                        df_top_year[['Congregação', 'Total no Ano (R$)']].style.format({"Total no Ano (R$)": format_currency}),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.caption("Nenhum contribuinte no ano.")


def page_relatorio_missoes_congregacao(user: "User"):
    """
    Relatório de Missões para login de congregação (TESOUREIRO):
    - Adiciona tabela EDITÁVEL de ENTRADAS de Missões por culto.
    - Adiciona VISUALIZAÇÃO de SAÍDAS e SALDO de Missões da sua unidade.
    """
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Gestão Missões</h1>", unsafe_allow_html=True)

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
        
        target_sub_cong_id = None
        contexto = f"{parent_cong_obj.name} (Principal)"

        if sub_congs:
            opcoes = {f"{parent_cong_obj.name} (Principal)": None}
            for sub in sub_congs:
                opcoes[sub.name] = sub.id
            
            # Se houver mais de uma opção, exibe o seletor
            contexto = st.selectbox("Unidade para Lançamento:", list(opcoes.keys()), key="missoes_unidade_cong")
            target_sub_cong_id = opcoes[contexto]
        
        # Se não houver sub-congregações, exibe apenas a unidade principal
        st.info(f"Unidade selecionada: **{contexto}**")

        # ==================== 1. EDIÇÃO DE ENTRADAS ====================
        st.markdown("---")
        st.subheader("1. Lançar/Editar Entradas de Missões")
        
        _editor_missions_entries_unit(
            cong_id=parent_cong_obj.id,
            sub_cong_id=target_sub_cong_id,
            start=start, end=end,
            titulo=f"Entradas de Missões — {ref.strftime('%B/%Y')}"
        )
        
        # ==================== 2. VISUALIZAÇÃO DOS FLUXOS (Para Dirigente) ====================
        st.markdown("---")
        st.subheader("2. Histórico de Saídas e Saldo (Visualização)")
        
        # Coleta de dados (ENTRADAS e SAÍDAS de Missões da unidade)
        entradas_missoes, saidas_missoes = _collect_missions_data(db, start, end, only_cong_id=parent_cong_obj.id)
        
        # --- FILTRO POR SUB-UNIDADE (apenas para Saídas e Saldo) ---
        # Filtra as transações de Saída para mostrar apenas as da sub-unidade selecionada (se não for a principal)
        if target_sub_cong_id is not None:
             saidas_missoes = [t for t in saidas_missoes if t.sub_congregation_id == target_sub_cong_id]
             entradas_missoes = [t for t in entradas_missoes if t.sub_congregation_id == target_sub_cong_id]
        elif sub_congs:
             # Se for a Principal, remove os lançamentos das Subs (se existirem)
             saidas_missoes = [t for t in saidas_missoes if t.sub_congregation_id is None]
             entradas_missoes = [t for t in entradas_missoes if t.sub_congregation_id is None]
        # Se sub_congs não existir, os dados já são apenas da unidade principal.


        total_entradas_missions = sum(float(t.amount) for t in entradas_missoes)
        total_saidas_missions = sum(float(t.amount) for t in saidas_missoes)
        saldo_missions = total_entradas_missions - total_saidas_missions

        # --- Tabela de Saídas ---
        st.markdown("##### Saídas de Missões (Despesas)")
        if saidas_missoes:
            saidas_rows = [
                {"Data": t.date.strftime("%d/%m/%Y"), "Descrição": t.description or "—", "Valor": float(t.amount)}
                for t in saidas_missoes
            ]
            df_saidas = pd.DataFrame(saidas_rows)
            st.dataframe(df_saidas.style.format({"Valor": format_currency}), use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhuma saída de missões registrada para esta unidade no período.")

        st.divider()
        
        # --- Resumo Saldo ---
        st.markdown("##### Saldo de Missões no Mês")
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Entradas", format_currency(total_entradas_missions))
        c2.metric("Total Saídas", format_currency(total_saidas_missions), delta_color="inverse")
        c3.metric("Saldo do Mês", format_currency(saldo_missions))

        # (Adicione aqui quaisquer outras visualizações ou exportações que o Tesoureiro precise)

        # (Se você tinha outras seções específicas aqui, mantenha abaixo sem alterações.)
        # Ex.: visualizações, exportações, etc.

@st.cache_data(ttl=600)
def get_missions_data_for_ia(cong_id: int, start: date, end: date):
    """
    Busca todas as transações de ENTRADA (Missões) e SAÍDA (Missões)
    de uma congregação e período específicos para análise da IA.
    """
    with SessionLocal() as db:
        q_missions = select(
            Transaction.date.label("Data"),
            Transaction.type.label("Tipo"),
            Category.name.label("Categoria"),
            Transaction.description.label("Descricao"),
            Transaction.amount.label("Valor")
        ).join(Category).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start,
            Transaction.date < end,
            func.lower(Category.name).like('%missões%') # Pega 'Missões' e 'Missões (Saída)'
        ).order_by(Transaction.date)

        df_missions = pd.read_sql(q_missions, db.bind)
        return df_missions
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
        
        principal_totals = _collect_month_data(cong.id, start, end, sub_cong_id=None)["totals"]
        principal_entradas = principal_totals["entradas_total_sem_missoes"]
        
        # Adiciona a linha da congregação principal
        report_data.append({
            "Unidade": f"{cong.name} (Principal)", "Entradas": principal_entradas,
            "cong_id": cong.id, "sub_id": None
        })
        
        for sub in sub_congs:
            sub_totals = _collect_month_data(cong.id, start, end, sub_cong_id=sub.id)["totals"]
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
        
        principal_totals = _collect_month_data(cong.id, start, end, sub_cong_id=None)["totals"]
        principal_saidas = principal_totals["saidas_total"]
        
        report_data.append({
            "Unidade": f"{cong.name} (Principal)", "Saídas": principal_saidas,
            "cong_id": cong.id, "sub_id": None
        })
        
        for sub in sub_congs:
            sub_totals = _collect_month_data(cong.id, start, end, sub_cong_id=sub.id)["totals"]
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
        principal_df = _load_service_logs(cong.id, start, end, sub_cong_id=None)
        principal_entradas = principal_df['Total'].sum() if not principal_df.empty else 0.0
        report_data.append({
            "Unidade": f"{cong.name} (Principal)", "Entradas": principal_entradas,
            "cong_id": cong.id, "sub_id": None
        })
        
        # Busca dados das sub-congregações
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == cong.id).order_by(SubCongregation.name)).all()
        for sub in sub_congs:
            sub_df = _load_service_logs(cong.id, start, end, sub_cong_id=sub.id)
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
    """
    Relatório de Entrada — versão fiel ao seu fluxo original
    + Aviso fixo de Missões
    + Banner de divergência (Resumo x Nominal) abaixo dos filtros.
    """
    ensure_seed()
    with SessionLocal() as db:
        from sqlalchemy import select, func

        st.markdown("<h1 class='page-title'>Relatório de Entrada</h1>", unsafe_allow_html=True)

        # Período
        ref = get_month_selector()
        start, end = month_bounds(ref)

        # Escopo (SEDE: escolha de congregação/hierarquia; demais: congregação do usuário)
        parent_cong_obj = None
        if user.role == "SEDE":
            congs_all = order_congs_sede_first(cong_options_for(user, db))
            escopo_opts = ["-- Relatório Hierárquico (Edição) --"] + [c.name for c in congs_all]

            escopo_selecionado = st.selectbox(
                "Selecione o escopo do relatório:",
                escopo_opts,
                key="re_sede_escopo"
            )

            if escopo_selecionado == "-- Relatório Hierárquico (Edição) --":
                # Mantém a sua visão hierárquica original
                display_entry_hierarchy(user, congs_all, start, end, db)
                return
            else:
                parent_cong_obj = next((c for c in congs_all if c.name == escopo_selecionado), None)
        else:  # TESOREIRO
            parent_cong_obj = db.get(Congregation, user.congregation_id)

        if not parent_cong_obj:
            st.info("Nenhuma congregação para analisar.")
            return

        st.divider()
        st.markdown(f"### Detalhes de: {parent_cong_obj.name.upper()}")

        # Seleção de unidade (Principal / Subs / Todas)
        sub_congs = db.scalars(
            select(SubCongregation)
            .where(SubCongregation.congregation_id == parent_cong_obj.id)
            .order_by(SubCongregation.name)
        ).all()

        target_sub_cong_id_or_all = None  # None = Principal; id = Sub; "ALL" = Todas
        contexto_selecionado = parent_cong_obj.name

        if sub_congs:
            opcoes = {"-- Todas (Principal + Subs) --": "ALL", f"{parent_cong_obj.name} (Principal)": None}
            for sub in sub_congs:
                opcoes[sub.name] = sub.id

            contexto_selecionado = st.selectbox(
                "Filtrar por unidade:",
                list(opcoes.keys()),
                key="re_sub_sel_unified"
            )
            target_sub_cong_id_or_all = opcoes[contexto_selecionado]

        st.info(f"Exibindo dados para: **{contexto_selecionado}**")

        # ---------------- AVISOS ABAIXO DOS FILTROS ----------------

        # Aviso fixo de Missões
        def _render_missoes_notice():
            st.markdown(
                """
                <div style="
                    background:#fff7e6;             /* amarelo suave */
                    border:1px solid #ffd59e;       /* borda âmbar */
                    color:#7a4b00;                  /* texto âmbar escuro */
                    border-radius:12px;
                    padding:10px 14px;
                    margin: 6px 0 10px 0;
                    font-size:0.95rem;">
                  <strong>Atenção:</strong> As ofertas do <strong>Culto de Missões</strong> são lançadas
                  automaticamente no menu <strong>Relatório de Missões</strong> ao lado.
                </div>
                """,
                unsafe_allow_html=True,
            )

        _render_missoes_notice()

        # Banner de divergência (Resumo x Nominal) — respeita período e unidade
        def _render_divergence_banner(total_resumo: float, total_nominal: float):
            if round(total_resumo, 2) == round(total_nominal, 2):
                return
            diff = total_nominal - total_resumo
            st.markdown(
                f"""
                <div style="
                    background:#fdecec; border:1px solid #f3b4b6; color:#7a1c1c;
                    border-radius:12px; padding:10px 14px; font-weight:600; margin-bottom:8px;">
                  <strong>Divergência de Dízimos no período</strong>
                  — Declarado no resumo: <span style="font-weight:800;">{format_currency(total_resumo)}</span>
                  • Nominal (dizimistas): <span style="font-weight:800;">{format_currency(total_nominal)}</span>
                  • Diferença: <span style="font-weight:800;">{format_currency(diff)}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Calcula os totais para o banner (sem interferir no restante do relatório)
        sl_conditions = [
            ServiceLog.congregation_id == parent_cong_obj.id,
            ServiceLog.date >= start,
            ServiceLog.date < end,
        ]
        tt_conditions = [
            Tithe.congregation_id == parent_cong_obj.id,
            Tithe.date >= start,
            Tithe.date < end,
        ]

        if target_sub_cong_id_or_all == "ALL":
            # todas as unidades (sem filtro adicional)
            pass
        elif target_sub_cong_id_or_all is None:
            # somente Principal
            sl_conditions.append(ServiceLog.sub_congregation_id.is_(None))
            tt_conditions.append(Tithe.sub_congregation_id.is_(None))
        else:
            # uma Sub específica
            sl_conditions.append(ServiceLog.sub_congregation_id == target_sub_cong_id_or_all)
            tt_conditions.append(Tithe.sub_congregation_id == target_sub_cong_id_or_all)

        total_resumo = float(
            db.scalar(select(func.coalesce(func.sum(ServiceLog.dizimo), 0.0)).where(*sl_conditions)) or 0.0
        )
        total_nominal = float(
            db.scalar(select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(*tt_conditions)) or 0.0
        )

        _render_divergence_banner(total_resumo, total_nominal)

        # ---------------- RESTO DO RELATÓRIO (INALTERADO) ----------------

        if target_sub_cong_id_or_all == "ALL":
            # Visão agregada por unidade (Principal + cada Sub)
            all_units_data = []

            df_principal = _load_service_logs(parent_cong_obj.id, start, end, sub_cong_id=None)
            total_principal = float(df_principal['Total'].sum()) if not df_principal.empty else 0.0
            all_units_data.append({
                "Unidade": f"{parent_cong_obj.name} (Principal)",
                "Total Entradas": total_principal
            })

            for sub in sub_congs:
                df_sub = _load_service_logs(parent_cong_obj.id, start, end, sub_cong_id=sub.id)
                total_sub = float(df_sub['Total'].sum()) if not df_sub.empty else 0.0
                all_units_data.append({"Unidade": f"↳ {sub.name}", "Total Entradas": total_sub})

            df_agg = pd.DataFrame(all_units_data)
            st.dataframe(
                df_agg.style.format({"Total Entradas": format_currency}),
                use_container_width=True,
                hide_index=True
            )
            total_geral = float(df_agg["Total Entradas"].sum()) if not df_agg.empty else 0.0
            st.metric("Total Geral da Congregação", format_currency(total_geral))

        else:
            # Relatório detalhado da unidade selecionada
            report_df = _load_service_logs(parent_cong_obj.id, start, end, sub_cong_id=target_sub_cong_id_or_all)

            st.dataframe(
                report_df.style.format({
                    "Data do Culto": "{:%d/%m/%Y}",
                    "Dízimo": format_currency,
                    "Oferta": format_currency,
                    "Total": format_currency,
                }),
                use_container_width=True,
                hide_index=True,
                column_order=["Data do Culto", "Tipo de Culto", "Dízimo", "Oferta", "Total"]
            )

            # Métricas rápidas
            st.divider()
            try:
                if report_df.empty:
                    total_dizimo = total_oferta = total_geral = 0.0
                else:
                    total_dizimo = float(report_df["Dízimo"].sum())
                    total_oferta = float(report_df["Oferta"].sum())
                    total_geral = float(report_df["Total"].sum())

                col1, col2, col3 = st.columns(3)
                col1.metric("Total de Dízimos", format_currency(total_dizimo))
                col2.metric("Total de Ofertas", format_currency(total_oferta))
                col3.metric("Total Geral Entradas", format_currency(total_geral))
            except Exception:
                st.caption("Calculando totais...")


def _render_missoes_notice():
    """Banner informativo: ofertas de Culto de Missões são lançadas no Relatório de Missões."""
    st.markdown(
        """
        <div style="
            margin: 8px 0 16px 0;
            padding: 10px 12px;
            border-radius: 10px;
            background: #fff7ed;               /* laranja bem claro */
            border: 1px solid #fdba74;         /* laranja */
            color: #7c2d12;                    /* marrom/laranja escuro */
            font-weight: 600;">
            ⚠️ Atenção: Ao lançar as ofertas do <strong>Culto de Missões</strong>, ela estará visivel no menu
            <strong>Gestão Missões, ao lado</strong>.
        </div>
        """,
        unsafe_allow_html=True,
    )
def _render_divergence_banner(total_resumo: float, total_nominal: float):
    """Banner vermelho de divergência de dízimos (resumo vs nominal)."""
    diff = round(total_resumo - total_nominal, 2)
    if abs(diff) < 0.01:
        return
    st.markdown(
        f"""
        <div style="
            margin: 8px 0 16px 0;
            padding: 10px 12px;
            border-radius: 10px;
            background: #fdecec;               /* vermelho bem claro */
            border: 1px solid #f5a6a6;         /* vermelho */
            color: #7f1d1d;                    /* vinho */
            font-weight: 700;">
            Divergência de Dízimos no período — Declarado no resumo: <strong>R$ {total_resumo:,.2f}</strong>
            • Nominal (dizimistas): <strong>R$ {total_nominal:,.2f}</strong>
            • Diferença: <strong>R$ {diff:,.2f}</strong>
        </div>
        """.replace(",", "X").replace(".", ",").replace("X", "."),
        unsafe_allow_html=True,
    )
def _build_resumo_por_unidade(parent: Optional[Congregation], sub_id: Optional[Union[int, str]], start: date, end: date, db: Session) -> pd.DataFrame:
    """
    Monta um DataFrame com resumo de entradas (Dízimo, Oferta, Total) por unidade.
    
    CORREÇÃO GARANTIDA: Arredonda todos os valores para duas casas decimais (round(..., 2))
    e usa o st.cache_data para garantir a performance após a primeira execução.
    """
    import pandas as pd
    from sqlalchemy import select, func, and_

    # NOTA: O decorador @st.cache_data DEVE estar acima desta função no seu código!

    rows = []

    def _sum_for(congregation_id, sub_congregation_id):
        cond = [
            ServiceLog.congregation_id == congregation_id,
            ServiceLog.date >= start,
            ServiceLog.date < end,
            # CONDIÇÃO CRÍTICA: EXCLUI LOGS DE MISSÕES DO FLUXO OPERACIONAL
            ServiceLog.service_type != "Culto de Missões",
        ]
        
        # Lógica de filtro para sub-congregação
        if sub_congregation_id == "ALL":
            pass # Sem filtro de sub_congregation_id
        elif sub_congregation_id is None:
            cond.append(ServiceLog.sub_congregation_id.is_(None))
        elif isinstance(sub_congregation_id, int):
            cond.append(ServiceLog.sub_congregation_id == sub_congregation_id)

        diz = float(db.scalar(select(func.coalesce(func.sum(ServiceLog.dizimo), 0.0)).where(and_(*cond))) or 0.0)
        ofe = float(db.scalar(select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(and_(*cond))) or 0.0)
        return diz, ofe, diz + ofe

    # Caso 1: Toda a Rede (parent = None) -> lista por congregação (sem detalhar sub)
    if parent is None:
        congs = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        for c in congs:
            diz, ofe, tot = _sum_for(c.id, "ALL") 
            rows.append({
                "Unidade": c.name,
                "Dízimos": round(diz, 2),
                "Ofertas": round(ofe, 2),
                "Total Entradas": round(tot, 2),
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("Total Entradas", ascending=False).reset_index(drop=True)
        return df

    # Caso 2: Há congregação selecionada (parent != None)
    if sub_id == "ALL":
        # Principal
        diz, ofe, tot = _sum_for(parent.id, None)
        rows.append({
            "Unidade": f"{parent.name} (Principal)",
            "Dízimos": round(diz, 2),
            "Ofertas": round(ofe, 2),
            "Total Entradas": round(tot, 2),
        })
        # Cada Sub
        subs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent.id).order_by(SubCongregation.name)).all()
        for s in subs:
            diz, ofe, tot = _sum_for(parent.id, s.id)
            rows.append({
                "Unidade": f"↳ {s.name}",
                "Dízimos": round(diz, 2),
                "Ofertas": round(ofe, 2),
                "Total Entradas": round(tot, 2),
            })
    elif sub_id is None:
        # Somente Principal
        diz, ofe, tot = _sum_for(parent.id, None)
        rows.append({
            "Unidade": f"{parent.name} (Principal)",
            "Dízimos": round(diz, 2),
            "Ofertas": round(ofe, 2),
            "Total Entradas": round(tot, 2),
        })
    elif isinstance(sub_id, int):
        # Sub específica
        sub = db.get(SubCongregation, sub_id)
        name = sub.name if sub else "Sub Desconhecida"
        diz, ofe, tot = _sum_for(parent.id, sub_id)
        rows.append({
            "Unidade": f"{name}",
            "Dízimos": round(diz, 2),
            "Ofertas": round(ofe, 2),
            "Total Entradas": round(tot, 2),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["Unidade"]).reset_index(drop=True)
    return df   


def _ultimos_movimentos(parent, sub_id, start, end, db, limit: int = 12):
    """
    Retorna um DataFrame com os movimentos (entradas/saídas) mais recentes do período:
    Colunas: Data | Tipo | Valor | Descrição | Unidade
    - parent: Congregation ou None (None = Toda a Rede)
    - sub_id: None (Principal) | int (Sub específica) | "ALL" (todas as unidades da congregação)
    - start, end: intervalo [start, end)
    - limit: quantidade máxima de registros (default 12)
    """
    import pandas as pd
    from sqlalchemy import select
    from sqlalchemy.orm import joinedload

    cond = [
        Transaction.date >= start,
        Transaction.date < end,
    ]
    if parent:
        cond.append(Transaction.congregation_id == parent.id)

    if sub_id == "ALL":
        # pega principal + todas as subs da congregação
        pass
    elif sub_id is None:
        cond.append(Transaction.sub_congregation_id.is_(None))
    else:
        cond.append(Transaction.sub_congregation_id == sub_id)

    q = (
        select(Transaction)
        .options(
            joinedload(Transaction.category),
            joinedload(Transaction.congregation),
            joinedload(Transaction.sub_congregation),
        )
        .where(*cond)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .limit(limit)
    )

    txs = db.scalars(q).all()
    rows = []
    for t in txs:
        # Unidade: mostra Sub se houver; senão, Principal (nome da congregação)
        if getattr(t, "sub_congregation_id", None):
            unidade = getattr(t.sub_congregation, "name", "Sub")
        else:
            unidade = getattr(t.congregation, "name", "Principal")

        tipo = getattr(t, "type", "") or ""
        cat  = getattr(getattr(t, "category", None), "name", "")
        desc = t.description or cat or ""

        rows.append({
            "Data":  t.date,
            "Tipo":  tipo,                     # geralmente "ENTRADA" / "SAÍDA"
            "Valor": float(t.amount or 0.0),
            "Descrição": desc,
            "Unidade": unidade,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Data", ascending=False).reset_index(drop=True)
    return df

def _goto(page_name: str, session_key: str = "nav"):
    """
    Navega para uma página do menu da sidebar.
    - page_name: rótulo exatamente igual ao que aparece no st.radio do menu.
    - session_key: chave usada no st.radio (no seu caso, "nav").
    """
    try:
        st.session_state[session_key] = page_name
    except Exception:
        # garante que a chave exista
        st.session_state[session_key] = page_name
    # força a troca imediata
    st.rerun()

            # REMOVIDO: Botão de salvar e toda a sua lógica
# ===================== MAIN =====================
# ===================== MAIN =====================
# ===================== MAIN =====================
# ===================== MAIN =====================
def main():
    # Importação garantida para o bloco try/except
    import streamlit as st 
    
    try:
        ensure_seed()

        user = current_user()

        # === BLOCO DE RECUPERAÇÃO DE SESSÃO (ROBUSTO) ===
        # Tenta carregar o usuário a partir do token de cookie se não estiver logado na sessão
        if not user:
            try:
                import extra_streamlit_components as stx
                cm = stx.CookieManager()
                tok = cm.get(COOKIE_NAME)
                data = _read_token(tok)
                if data:
                    with SessionLocal() as db:
                        u = db.get(User, int(data["uid"]))
                        if u:
                            st.session_state.uid = u.id
                            st.rerun() # Reruns para carregar o usuário na sessão
            except Exception:
                pass
        # === FIM BLOCO DE RECUPERAÇÃO DE SESSÃO ===

        if 'uid' not in st.session_state or not st.session_state.uid:
            # ESTADO DESLOGADO: Mostra a UI de login
            login_ui()
        else:
            # ESTADO LOGADO: Carrega o usuário e mostra a interface principal
            user = current_user()
            if user:
                
                # === LOGO GLOBAL ===
                LOGO_PATH = "images/logo_igreja.png" 
                try:
                    st.logo(LOGO_PATH, size="large") 
                except Exception:
                    pass 
                # === FIM LOGO GLOBAL ===
                
                page = sidebar_common(user)

                # >>> ROTEAMENTO SIMPLIFICADO E RESTRITO <<<
                if page == "Painel Principal":
                    page_inicio(user)
                elif page == "Lançamentos":
                    page_lancamentos(user)
                elif page == "Relatórios Financeiros":
                    page_relatorios_unificados(user) 
                elif page == "Gestão Missões":
                    if getattr(user, "role", "") == "TESOUREIRO MISSIONÁRIO":
                        page_relatorio_missoes(user)
                    else:
                        page_relatorio_missoes_congregacao(user)
                elif page == "Configurações":
                    page_cadastro(user)
                
                # --- LÓGICA DE RESTRIÇÃO DA IA ---
                elif page == "Assistente IA":
                    if getattr(user, "role", "") == "SEDE":
                        page_assistente_ia(user)
                    else:
                        st.error("Acesso Negado: O assistente de IA é exclusivo para o perfil SEDE.")
                        page_inicio(user) 
                # --- FIM RESTRIÇÃO DA IA ---
                        
                else:
                    page_inicio(user) 
            else:
                logout()

    except Exception as e:
        # st está garantido por estar definido localmente no início da função.
        st.error("Ocorreu un erro crítico na aplicação.")
        st.exception(e)



        # ===================== PAGE: ASSISTENTE IA ========================
# ===================== PAGE: ASSISTENTE IA (COM RESUMO RÁPIDO E ANÁLISE LIVRE) =====================
# ===================== PAGE: ASSISTENTE IA (VERSÃO ESTÁVEL E FINAL) ===================

def query_financial_details_for_ai(db, start_dt, end_dt, cong_id=None, sub_cong_id=None):
    """
    Retorna um dict com dados detalhados (listas + totais):
      - tithes_list: lista de dicts {date, tither_name, amount, payment_method}
      - total_dizimos, by_payment_method, count_dizimistas
      - service_offers_culto_list: lista de dicts {date, service_type, oferta}
      - total_ofertas_culto
      - service_offers_missoes_list: lista de dicts (Culto de Missões)
      - total_ofertas_missoes
      - total_ofertas_transacoes (categoria 'oferta')
      - saídas: list de transações (date, category, amount, description)
      - total_saidas_by_category: dict categoria -> total
    Nota: não altera dados.
    """
    out = {}

    # filtros comuns
    filters_tithe = [Tithe.date >= start_dt, Tithe.date < end_dt]
    filters_service = [ServiceLog.date >= start_dt, ServiceLog.date < end_dt]
    filters_tx = [Transaction.date >= start_dt, Transaction.date < end_dt]

    if cong_id is not None:
        filters_tithe.append(Tithe.congregation_id == cong_id)
        filters_service.append(ServiceLog.congregation_id == cong_id)
        filters_tx.append(Transaction.congregation_id == cong_id)
    if sub_cong_id is not None:
        filters_tithe.append(Tithe.sub_congregation_id == sub_cong_id)
        filters_service.append(ServiceLog.sub_congregation_id == sub_cong_id)
        filters_tx.append(Transaction.sub_congregation_id == sub_cong_id)

    # ------------------ Dízimos (lista + totais + por forma) ------------------
    try:
        # lista de dizimos detalhados (limitar a 200 linhas para performance)
        tithes_q = (
            select(Tithe.date, Tithe.tither_name, Tithe.amount, Tithe.payment_method)
            .where(*filters_tithe)
            .order_by(Tithe.date.desc())
            .limit(200)
        )
        tithes_rows = db.execute(tithes_q).all()
        tithes_list = []
        for r in tithes_rows:
            tithes_list.append({
                "date": r.date,
                "tither_name": (r.tither_name or "").strip(),
                "amount": float(r.amount or 0.0),
                "payment_method": (r.payment_method or "—")
            })
        out["tithes_list"] = tithes_list

        # total dizimos
        total_diz = float(db.scalar(
            select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(*filters_tithe)
        ) or 0.0)
        out["total_dizimos"] = total_diz

        # por forma de pagamento
        pm_q = (
            select(Tithe.payment_method, func.coalesce(func.sum(Tithe.amount), 0.0))
            .where(*filters_tithe)
            .group_by(Tithe.payment_method)
        )
        pm_rows = db.execute(pm_q).all()
        by_pm = {}
        for pm, sm in pm_rows:
            key = (pm or "Não informado")
            by_pm[key] = float(sm or 0.0)
        out["by_payment_method"] = by_pm

        # quantidade de registros (dizimistas lançados)
        count_diz = int(db.scalar(select(func.count()).select_from(Tithe).where(*filters_tithe)) or 0)
        out["count_dizimistas"] = count_diz
    except Exception:
        out["tithes_list"] = []
        out["total_dizimos"] = 0.0
        out["by_payment_method"] = {}
        out["count_dizimistas"] = 0

    # ------------------ ServiceLog: Ofertas por Culto e Missões (detalhes + totais) ------------------
    try:
        # Ofertas de cultos (exclui Culto de Missões)
        cultos_q = (
            select(ServiceLog.date, ServiceLog.service_type, ServiceLog.oferta)
            .where(*filters_service, ServiceLog.oferta.is_not(None))
        )
        cultos_rows = db.execute(cultos_q).all()
        ofertas_culto_list, ofertas_missoes_list = [], []
        total_culto = 0.0
        total_missoes = 0.0
        for d, stype, val in cultos_rows:
            v = float(val or 0.0)
            stl = (stype or "").lower()
            if ("miss" in stl) or ("missões" in stl) or ("missao" in stl):
                ofertas_missoes_list.append({"date": d, "service_type": stype, "oferta": v})
                total_missoes += v
            else:
                ofertas_culto_list.append({"date": d, "service_type": stype, "oferta": v})
                total_culto += v
        out["service_offers_culto_list"] = ofertas_culto_list
        out["total_ofertas_culto"] = total_culto
        out["service_offers_missoes_list"] = ofertas_missoes_list
        out["total_ofertas_missoes"] = total_missoes
    except Exception:
        out["service_offers_culto_list"] = []
        out["total_ofertas_culto"] = 0.0
        out["service_offers_missoes_list"] = []
        out["total_ofertas_missoes"] = 0.0

    # ------------------ Transações: Ofertas em categoria e Saídas ------------------
    try:
        # identificar categoria 'oferta' e 'missões' por nome (case-insensitive)
        type_in = globals().get("TYPE_IN", "ENTRADA")
        total_ofertas_trans = float(db.scalar(
            select(func.coalesce(func.sum(Transaction.amount), 0.0))
            .join(Category, isouter=True)
            .where(*(filters_tx + [Transaction.type == type_in,
                                   func.lower(func.coalesce(Category.name, "")).like("%ofert%")]))
        ) or 0.0)
        out["total_ofertas_transacoes"] = total_ofertas_trans

        total_missoes_trans = float(db.scalar(
            select(func.coalesce(func.sum(Transaction.amount), 0.0))
            .join(Category, isouter=True)
            .where(*(filters_tx + [Transaction.type == type_in,
                                   func.lower(func.coalesce(Category.name, "")).like("%miss%")]))
        ) or 0.0)
        out["total_ofertas_missoes_transacoes"] = total_missoes_trans

        # Saídas (Transaction.type == 'SAÍDA') -> lista e totais por categoria
        txs_q = (
            select(Transaction.date, Transaction.amount, Transaction.description, Category.name)
            .join(Category, isouter=True)
            .where(*(filters_tx + [Transaction.type == "SAÍDA"]))
            .order_by(Transaction.date.desc())
            .limit(200)
        )
        txs_rows = db.execute(txs_q).all()
        txs_list = []
        for date_, amt, desc, catname in txs_rows:
            txs_list.append({
                "date": date_,
                "amount": float(amt or 0.0),
                "category": (catname or "—"),
                "description": (desc or "")
            })
        out["saidas_list"] = txs_list

        cat_tot_q = (
            select(
                func.coalesce(func.lower(func.coalesce(Category.name, "Não informado")), "não informado").label("cat"),
                func.coalesce(func.sum(Transaction.amount), 0.0)
            )
            .join(Category, isouter=True)
            .where(*(filters_tx + [Transaction.type == "SAÍDA"]))
            .group_by(func.lower(func.coalesce(Category.name, "Não informado")))
        )
        cat_tot_rows = db.execute(cat_tot_q).all()
        total_by_cat = {}
        for cat, sm in cat_tot_rows:
            total_by_cat[cat.title()] = float(sm or 0.0)
        out["total_saidas_by_category"] = total_by_cat
    except Exception:
        out["total_ofertas_transacoes"] = 0.0
        out["total_ofertas_missoes_transacoes"] = 0.0
        out["saidas_list"] = []
        out["total_saidas_by_category"] = {}

    return out


def page_assistente_ia(user: "User"):
    """
    Assistente IA — respostas curtas/objetivas sobre:
      - dízimos (total/por pessoa/por forma de pagamento)
      - ofertas do culto (ServiceLog, sem missões)
      - ofertas de missões (Transaction - categoria 'Missões', com fallback no ServiceLog)
      - ofertas como transações da categoria 'Oferta'
      - saídas por tipo/total
      - listagens/tabelas quando pedir "tabela", "planilha", "csv", "excel"…
      - COMANDO RF: "<nome da congregação> RF" -> tabela com Dízimos, Ofertas do Culto, Saídas e Saldo
    Mantém “Todas as Congregações” e reconhece nome citado na pergunta.
    """
    import io
    import re
    import unicodedata
    import pandas as pd
    from sqlalchemy import select, func

    ensure_seed()

    # ----------------- helpers visuais -----------------
    def _fmt_brl(v) -> str:
        try:
            return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return f"R$ {v}"

    def _fmt_date(d) -> str:
        try:
            return d.strftime("%d/%m/%Y")
        except Exception:
            return str(d)

    def _norm(s: str) -> str:
        if not s:
            return ""
        s = unicodedata.normalize("NFD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
        return re.sub(r"\s+", " ", s).strip()

    # ----------------- mensagens persistidas -----------------
    if 'status_message' in st.session_state:
        msg_type, msg_text = st.session_state.status_message
        if msg_type == "success":
            st.success(msg_text)
        elif msg_type == "error":
            st.error(msg_text)
        elif msg_type == "warning":
            st.warning(msg_text)
        del st.session_state.status_message

    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Assistente IA</h1>", unsafe_allow_html=True)

        # ---- Congregações (inclui 'Todas as Congregações')
        congs_all = db.scalars(select(Congregation).order_by(Congregation.name)).all()
        cong_options = ["Todas as Congregações"] + [c.name for c in congs_all]
        cong_sel_name = st.selectbox("Congregação", cong_options)

        selected_cong_id = None
        if cong_sel_name != "Todas as Congregações":
            c = next((x for x in congs_all if x.name == cong_sel_name), None)
            selected_cong_id = c.id if c else None

        # Mês/Ano
        ref_tab = get_month_selector("Mês de Referência — Mês")
        start_tab, end_tab = month_bounds(ref_tab)

        st.divider()
        st.markdown("## 2. Faça sua Pergunta")
        user_question = st.text_area("Sua pergunta:", height=140, key="ai_question")
        analyze_btn = st.button("Analisar com IA")

        # ----------------- filtros auxiliares -----------------
        def _cong_filter(col, target_id):
            return [] if target_id is None else [col == target_id]

        # casar congregação pelo texto normalizado (para RF e para resolver nome dentro da pergunta)
        def _match_cong_by_norm(name_norm: str):
            for c in congs_all:
                if _norm(c.name) == name_norm:
                    return c
            for c in congs_all:
                if name_norm in _norm(c.name):
                    return c
            return None

        def resolve_congregation_from_text(text: str):
            nt = _norm(text)
            if not nt:
                return None
            best = None
            best_len = 0
            for c in congs_all:
                cn = _norm(c.name)
                if cn and cn in nt and len(cn) > best_len:
                    best = c
                    best_len = len(cn)
            return best

        # ----------------- detecção de nomes na pergunta -----------------
        def _name_hits_in_text(prompt_text: str, cong_id: int | None) -> set[str]:
            """
            Detecta nomes citados na pergunta e cruza com os dizimistas existentes no período.
            Retorna um set com os nomes originais tal como estão lançados.
            """
            nt = _norm(prompt_text or "")
            if not nt:
                return set()

            # lista distinta de nomes no período/escopo
            q = (
                select(Tithe.tither_name)
                .where(Tithe.date >= start_tab, Tithe.date < end_tab, *(_cong_filter(Tithe.congregation_id, cong_id)))
                .distinct()
            )
            names = [(n or "").strip() for (n,) in db.execute(q).all() if (n or "").strip()]

            # normalizado -> originais
            norm_map: dict[str, set[str]] = {}
            for nm in names:
                key = _norm(nm)
                if not key:
                    continue
                norm_map.setdefault(key, set()).add(nm)

            hits: set[str] = set()
            for key, originals in norm_map.items():
                if key and key in nt:
                    hits.update(originals)

            if not hits:
                tokens = nt.split()
                for key, originals in norm_map.items():
                    parts = key.split()
                    if len(parts) >= 2 and all(p in tokens for p in parts[:2]):
                        hits.update(originals)

            return hits

        # ----------------- consultas que as tabelas usam -----------------
        def query_tithes(cong_id: int | None, method: str | None = None, only_names: set[str] | None = None):
            wh = [Tithe.date >= start_tab, Tithe.date < end_tab, *_cong_filter(Tithe.congregation_id, cong_id)]
            if method:
                wh.append(func.lower(Tithe.payment_method) == method.lower())
            if only_names:
                wh.append(Tithe.tither_name.in_(list(only_names)))
            q = select(Tithe).where(*wh).order_by(Tithe.date, Tithe.tither_name)
            return db.scalars(q).all()

        def query_mission_offers_transactions(cong_id: int | None):
            # Category 'Missões' (Entrada)
            cat_id = db.scalar(
                select(Category.id).where(
                    func.lower(Category.name) == "missões",
                    Category.type == TYPE_IN
                )
            )
            if not cat_id:
                return []
            q = select(Transaction).where(
                Transaction.date >= start_tab, Transaction.date < end_tab,
                Transaction.type == TYPE_IN,
                Transaction.category_id == cat_id,
                *_cong_filter(Transaction.congregation_id, cong_id)
            ).order_by(Transaction.date)
            return db.scalars(q).all()

        def query_mission_offers_fallback_servicelog(cong_id: int | None):
            q = select(ServiceLog).where(
                ServiceLog.date >= start_tab, ServiceLog.date < end_tab,
                func.lower(ServiceLog.service_type).in_(["culto de missões", "culto de missoes"]),
                *_cong_filter(ServiceLog.congregation_id, cong_id)
            ).order_by(ServiceLog.date)
            return db.scalars(q).all()

        def query_service_offers_cultos(cong_id: int | None):
            q = select(ServiceLog).where(
                ServiceLog.date >= start_tab, ServiceLog.date < end_tab,
                func.lower(ServiceLog.service_type).notin_(["culto de missões", "culto de missoes"]),
                *_cong_filter(ServiceLog.congregation_id, cong_id)
            ).order_by(ServiceLog.date)
            return db.scalars(q).all()

        def query_outgoings(cong_id: int | None):
            q = (
                select(Transaction, Category.name)
                .join(Category, Category.id == Transaction.category_id)
                .where(
                    Transaction.date >= start_tab, Transaction.date < end_tab,
                    Transaction.type == TYPE_OUT,
                    *_cong_filter(Transaction.congregation_id, cong_id)
                )
                .order_by(Transaction.date)
            )
            return db.execute(q).all()

        def query_transactions_by_category_name(name_lower: str, tx_type: str, cong_id: int | None):
            cat_id = db.scalar(
                select(Category.id).where(
                    func.lower(Category.name) == name_lower,
                    Category.type == tx_type
                )
            )
            if not cat_id:
                return []
            q = select(Transaction).where(
                Transaction.date >= start_tab, Transaction.date < end_tab,
                Transaction.type == tx_type,
                Transaction.category_id == cat_id,
                *_cong_filter(Transaction.congregation_id, cong_id)
            ).order_by(Transaction.date)
            return db.scalars(q).all()

        # ----------------- respostas curtas (modo livre) -----------------
                # ----------------- respostas curtas (modo livre) -----------------
        def interpret_and_answer(prompt: str, cong_id: int | None) -> str:
            """
            Se o usuário digitar um NOME, retorna uma linha POR LANÇAMENTO no mês:
            **Nome, dizimou R$ X,XX, na congregação Y, na data DD/MM/AAAA, através de PIX/DINHEIRO.**
            Mantém os demais comportamentos para outras perguntas.
            """
            txt = _norm(prompt or "")
            parts: list[str] = []

            # Palavras-chave que indicam que NÃO é só busca por nome
            keywords = (
                "dizim", "ofert", "miss", "culto", "saída", "saida", "despesa", "gasto", "rf",
                "tabela", "planilha", "excel", "csv", "total", "pix", "dinheiro", "qtd", "quant"
            )
            looks_like_name = txt and all(k not in txt for k in keywords)

            # nomes existentes no período que apareçam na pergunta
            name_hits = _name_hits_in_text(prompt, cong_id)

            # ===== 1) MODO NOME → uma linha por lançamento =====
            if looks_like_name or name_hits:
                # usa catálogo quando disponível; senão faz like pelo texto digitado
                name_to_search = (prompt or "").strip()
                name_condition = []
                if name_hits:
                    name_condition.append(Tithe.tither_name.in_(list(name_hits)))
                else:
                    name_condition.append(func.lower(Tithe.tither_name).like(f"%{name_to_search.lower()}%"))

                q_lines = (
                    select(
                        Tithe.tither_name, Tithe.date, Tithe.amount, Tithe.payment_method,
                        Congregation.name
                    )
                    .join(Congregation, Congregation.id == Tithe.congregation_id, isouter=True)
                    .where(
                        Tithe.date >= start_tab, Tithe.date < end_tab,
                        *_cong_filter(Tithe.congregation_id, cong_id),
                        *name_condition
                    )
                    .order_by(Tithe.date)
                )
                rows = db.execute(q_lines).all()

                if rows:
                    lines_md: list[str] = []
                    for nm, d, amt, method, cong_name in rows:
                        mth = (method or "—").strip().upper()
                        if mth.lower() == "pix":
                            mth = "PIX"
                        elif mth.lower() == "dinheiro":
                            mth = "DINHEIRO"

                        # cada lançamento em NEGRITO, um por linha (parágrafo separado)
                        lines_md.append(
                            f"**{(nm or '').strip()}, dizimou {_fmt_brl(float(amt or 0.0))}, "
                            f"na congregação {cong_name or '—'}, na data {_fmt_date(d)}, "
                            f"através de {mth}.**"
                        )
                    # quebra de linha entre lançamentos
                    return "\n\n".join(lines_md)
                else:
                    # nenhum lançamento no mês para o nome pesquisado
                    return f"**{name_to_search}, dizimou {_fmt_brl(0.0)} no período selecionado.**"

            # ===== 2) Demais perguntas (mantém comportamentos existentes) =====

            # Dízimos – total (quando explicitamente pedido)
            if ("dizim" in txt or "dízim" in txt) and ("total" in txt or "som" in txt):
                q = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start_tab, Tithe.date < end_tab, *_cong_filter(Tithe.congregation_id, cong_id)
                )
                parts.append(f"Total Dízimos: {_fmt_brl(float(db.scalar(q) or 0.0))}")

            # Dízimos por método
            if "pix" in txt:
                q = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start_tab, Tithe.date < end_tab,
                    func.lower(Tithe.payment_method) == "pix",
                    *_cong_filter(Tithe.congregation_id, cong_id)
                )
                parts.append(f"Dízimos (PIX): {_fmt_brl(float(db.scalar(q) or 0.0))}")
            if "dinheiro" in txt:
                q = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                    Tithe.date >= start_tab, Tithe.date < end_tab,
                    func.lower(Tithe.payment_method) == "dinheiro",
                    *_cong_filter(Tithe.congregation_id, cong_id)
                )
                parts.append(f"Dízimos (Dinheiro): {_fmt_brl(float(db.scalar(q) or 0.0))}")

            # Ofertas (cultos x missões)
            if "oferta" in txt and "miss" in txt:
                txs = query_mission_offers_transactions(cong_id)
                tot = sum(float(tx.amount or 0) for tx in txs)
                if tot == 0:
                    sl = query_mission_offers_fallback_servicelog(cong_id)
                    tot = sum(float(s.oferta or 0) for s in sl)
                parts.append(f"Ofertas (Missões): {_fmt_brl(tot)}")
            elif "oferta" in txt:
                tot = sum(float(sv.oferta or 0) for sv in query_service_offers_cultos(cong_id))
                parts.append(f"Ofertas (Cultos): {_fmt_brl(tot)}")

            # Saídas
            if any(k in txt for k in ["saída", "saida", "despesa", "gasto"]):
                q = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                    Transaction.date >= start_tab, Transaction.date < end_tab,
                    Transaction.type == TYPE_OUT, *_cong_filter(Transaction.congregation_id, cong_id)
                )
                parts.append(f"Total Saídas: {_fmt_brl(float(db.scalar(q) or 0.0))}")

            return " • ".join(parts) if parts else ""


        # ===========================
        # TABELAS (quando pedir tabela/planilha/csv/excel)
        # ===========================
        def build_table_from_prompt(prompt: str, cong_id: int | None):
            text = _norm(prompt or "")
            wants_table = any(k in text for k in ["tabela", "planilha", "excel", "csv", "lista", "detalhe", "detalhes"])
            if not wants_table:
                return pd.DataFrame(), ""

            is_diz = "dizim" in text or "dízim" in text or "dizimista" in text
            is_saida = any(k in text for k in ["saída", "saida", "despesa", "gasto"])
            is_oferta = "oferta" in text or "ofertas" in text
            is_missoes = any(k in text for k in ["missões", "missoes", "missao"])
            is_culto = any(k in text for k in ["culto", "cultos"])
            filt_pix = "pix" in text
            filt_din = "dinheiro" in text or "cash" in text
            resumo_por_nome = any(k in text for k in ["por nome", "por pessoa", "resumo", "agrupado", "somado"])

            # nomes citados (para filtrar a tabela quando fizer sentido)
            name_hits = _name_hits_in_text(prompt, cong_id)

            # DÍZIMOS
            if is_diz:
                rows = query_tithes(
                    cong_id,
                    method=("pix" if filt_pix else ("dinheiro" if filt_din else None)),
                    only_names=(name_hits if name_hits else None)
                )
                if not rows:
                    return pd.DataFrame(), "Dízimos — sem registros."

                data = [{
                    "Data": _fmt_date(t.date),
                    "Dizimista": t.tither_name or "",
                    "Forma": (t.payment_method or "").upper(),
                    "Valor": float(t.amount or 0.0),
                } for t in rows]
                df = pd.DataFrame(data)

                if resumo_por_nome or name_hits:
                    df = df.groupby(["Dizimista", "Forma"], dropna=False, as_index=False).agg(
                        Quantidade=("Valor", "count"),
                        Total=("Valor", "sum"),
                    ).sort_values(["Total", "Dizimista"], ascending=[False, True])

                return df, "Dízimos"

            # MISSÕES (transações) com fallback no ServiceLog
            if is_oferta and is_missoes and not is_culto:
                txs = query_mission_offers_transactions(cong_id)
                data = [{
                    "Data": _fmt_date(tx.date),
                    "Descrição": tx.description or "",
                    "Valor": float(tx.amount or 0.0),
                } for tx in txs]
                if not data:
                    sl = query_mission_offers_fallback_servicelog(cong_id)
                    data = [{
                        "Data": _fmt_date(sv.date),
                        "Origem": "Culto de Missões",
                        "Valor": float(sv.oferta or 0.0),
                    } for sv in sl]
                return (pd.DataFrame(data) if data else pd.DataFrame()), "Ofertas de Missões"

            # OFERTAS CULTO
            if (is_oferta and is_culto) or (is_oferta and not is_missoes):
                rows = query_service_offers_cultos(cong_id)
                if not rows:
                    return pd.DataFrame(), "Ofertas do Culto — sem registros."
                data = [{
                    "Data": _fmt_date(sv.date),
                    "Tipo de Culto": sv.service_type,
                    "Oferta": float(sv.oferta or 0.0),
                } for sv in rows]
                return pd.DataFrame(data), "Ofertas do Culto"

            # SAÍDAS
            if is_saida:
                rows = query_outgoings(cong_id)
                if not rows:
                    return pd.DataFrame(), "Saídas — sem registros."
                data = [{
                    "Data": _fmt_date(tx.date),
                    "Categoria": cat,
                    "Descrição": tx.description or "",
                    "Valor": float(tx.amount or 0.0),
                } for tx, cat in rows]
                df = pd.DataFrame(data)
                if resumo_por_nome:
                    df = df.groupby(["Categoria"], as_index=False)\
                           .agg(Total=("Valor", "sum"))\
                           .sort_values("Total", ascending=False)
                return df, "Saídas"

            # CATEGORIA 'OFERTA' (transação ENTRADA)
            if is_oferta and ("categoria" in text or "transa" in text):
                rows = query_transactions_by_category_name("oferta", TYPE_IN, cong_id)
                if not rows:
                    return pd.DataFrame(), "Transações — categoria 'Oferta' sem registros."
                data = [{
                    "Data": _fmt_date(tx.date),
                    "Descrição": tx.description or "",
                    "Valor": float(tx.amount or 0.0),
                } for tx in rows]
                return pd.DataFrame(data), "Transações — Categoria 'Oferta' (Entrada)"

            return pd.DataFrame(), ""

        # ----------------- Execução -----------------
        if analyze_btn:
            qtext = (user_question or "").strip()
            if not qtext:
                st.warning("Digite a pergunta.")
                return

            # “Todas as Congregações” + possível nome citado na pergunta
            target_cong_id = selected_cong_id
            if target_cong_id is None:
                resolved = resolve_congregation_from_text(qtext)
                if resolved:
                    target_cong_id = resolved.id

            # ---------------------- COMANDO "RF" ----------------------
            nt = _norm(qtext)
            m = re.search(r"(.+?)\s+rf\b$", nt)
            if m:
                cong_txt_norm = m.group(1).strip()
                if cong_txt_norm in ("todas as congregacoes", "todas as congregações", "todas"):
                    rf_cong_id = None
                    rf_cong_titulo = "Todas as Congregações"
                else:
                    cong_match = _match_cong_by_norm(cong_txt_norm)
                    if not cong_match:
                        st.error("Não encontrei essa congregação para o comando RF.")
                        return
                    rf_cong_id = cong_match.id
                    rf_cong_titulo = cong_match.name

                diz_total = float(db.scalar(
                    select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                        Tithe.date >= start_tab, Tithe.date < end_tab,
                        *_cong_filter(Tithe.congregation_id, rf_cong_id)
                    )
                ) or 0.0)

                ofertas_culto = float(db.scalar(
                    select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
                        ServiceLog.date >= start_tab, ServiceLog.date < end_tab,
                        func.lower(ServiceLog.service_type).notin_(["culto de missoes", "culto de missões"]),
                        *_cong_filter(ServiceLog.congregation_id, rf_cong_id)
                    )
                ) or 0.0)

                saidas_total = float(db.scalar(
                    select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                        Transaction.date >= start_tab, Transaction.date < end_tab,
                        Transaction.type == TYPE_OUT,
                        *_cong_filter(Transaction.congregation_id, rf_cong_id)
                    )
                ) or 0.0)

                saldo = diz_total + ofertas_culto - saidas_total

                df_rf = pd.DataFrame([{
                    "Dízimos (Entradas)": diz_total,
                    "Ofertas do Culto (Entradas)": ofertas_culto,
                    "Saídas (Totais)": saidas_total,
                    "Saldo": saldo,
                }])

                df_show = df_rf.copy()
                for ccol in df_show.columns:
                    df_show[ccol] = df_show[ccol].map(_fmt_brl)

                st.markdown(f"**Relatório Financeiro — {rf_cong_titulo} — {ref_tab.strftime('%m/%Y')}**")
                st.dataframe(df_show, use_container_width=True)

                # downloads
                csv_bytes = df_rf.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Baixar CSV",
                    data=csv_bytes,
                    file_name=f"rf_{_norm(rf_cong_titulo)}_{ref_tab.strftime('%Y_%m')}.csv",
                    mime="text/csv",
                )

                excel_ok = False
                for eng, module in (("xlsxwriter", "xlsxwriter"), ("openpyxl", "openpyxl")):
                    try:
                        __import__(module)
                        buf = io.BytesIO()
                        with pd.ExcelWriter(buf, engine=eng) as writer:
                            df_rf.to_excel(writer, sheet_name="RF", index=False)
                        st.download_button(
                            "Baixar Excel",
                            data=buf.getvalue(),
                            file_name=f"rf_{_norm(rf_cong_titulo)}_{ref_tab.strftime('%Y_%m')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                        excel_ok = True
                        break
                    except Exception:
                        continue
                if not excel_ok:
                    st.caption("Para Excel, instale 'xlsxwriter' ou 'openpyxl'. Use o CSV por enquanto.")
                return
            # -------------------- FIM COMANDO "RF" --------------------

            # Se pediu lista/tabela/planilha/detalhes -> DataFrame
            df, titulo = build_table_from_prompt(qtext, target_cong_id)
            if not df.empty:
                st.markdown(f"**{titulo}**")
                st.dataframe(df, use_container_width=True)

                # ---- Downloads: CSV (sempre) + Excel (xlsxwriter → openpyxl) ----
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button("Baixar CSV", data=csv_bytes,
                                   file_name=f"{_norm(titulo or 'dados')}.csv", mime="text/csv")

                excel_done = False
                excel_filename = f"{_norm(titulo or 'dados')}.xlsx"
                excel_mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                for eng, module in (("xlsxwriter", "xlsxwriter"), ("openpyxl", "openpyxl")):
                    try:
                        __import__(module)
                        buf = io.BytesIO()
                        with pd.ExcelWriter(buf, engine=eng) as writer:
                            df.to_excel(writer, sheet_name="Dados", index=False)
                        st.download_button("Baixar Excel", data=buf.getvalue(),
                                           file_name=excel_filename, mime=excel_mime)
                        excel_done = True
                        break
                    except Exception:
                        continue
                if not excel_done:
                    st.caption("Para exportar em Excel, instale 'xlsxwriter' ou 'openpyxl'. Use o CSV por enquanto.")

                # resumo curto para não poluir
                resumo = interpret_and_answer(qtext, target_cong_id)
                if resumo:
                    st.caption(resumo)
                return

            # Caso não tenha pedido tabela/planilha, responde conciso (modo livre)
            answer = interpret_and_answer(qtext, target_cong_id)

            # Fallback amplo (sem alterar funcionalidades já existentes)
            if not answer or answer.strip() == "Sem dados para a sua pergunta no período.":
                parts = []
                txt2 = _norm(qtext)
                if "dizim" in txt2 or "dízim" in txt2 or "dizimista" in txt2:
                    q = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                        Tithe.date >= start_tab, Tithe.date < end_tab, *_cong_filter(Tithe.congregation_id, target_cong_id)
                    )
                    parts.append(f"Total Dízimos: {_fmt_brl(float(db.scalar(q) or 0.0))}")
                if "miss" in txt2 or ("oferta" in txt2 and "miss" in txt2):
                    txs = query_mission_offers_transactions(target_cong_id)
                    tot = sum(tx.amount or 0 for tx in txs)
                    if tot == 0:
                        sl = query_mission_offers_fallback_servicelog(target_cong_id)
                        tot = sum(s.oferta or 0 for s in sl)
                    parts.append(f"Ofertas (Missões): {_fmt_brl(tot)}")
                if "oferta" in txt2 and ("culto" in txt2 or "miss" not in txt2):
                    parts.append(f"Ofertas (Cultos): {_fmt_brl(sum(sv.oferta or 0 for sv in query_service_offers_cultos(target_cong_id)))}")
                if any(k in txt2 for k in ["saída", "saida", "despesa", "gasto"]):
                    q = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
                        Transaction.date >= start_tab, Transaction.date < end_tab,
                        Transaction.type == TYPE_OUT, *_cong_filter(Transaction.congregation_id, target_cong_id)
                    )
                    parts.append(f"Total Saídas: {_fmt_brl(float(db.scalar(q) or 0.0))}")

                answer = " • ".join(parts) if parts else "Sem dados para a sua pergunta no período."

            st.markdown(answer)

 
                # Para depuração local, você pode descomentar a linha abaixo:
                # st.exception(e)

                # opcional: para debug local, descomente a linha abaixo
                # st.exception(e)
            # ... (O restante do código para o Tesoureiro Missionário permanece o mesmo)
            # ...
            
            
if __name__ == "__main__":
    main()
