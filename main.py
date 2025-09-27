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

# --- COLE AQUI: funções de suporte para "Todas as Congregações" e resumo IA ---

SUB_ALL = "__ALL__"

from sqlalchemy import select, func
from sqlalchemy.orm import joinedload
import datetime

def render_ai_context_selector(user, db, key_prefix="ai"):
    """
    Renderiza o seletor de contexto para a análise IA:
      - Para SEDE: adiciona a opção "Todas as Congregações"
      - Retorna: (cong_id, sub_cong_id, label)
        * cong_id is None => todas as congregações
        * sub_cong_id == SUB_ALL => incluir todas as sub-congregações (quando cong_id is None)
        * sub_cong_id is None => filtrar registros com sub_congregation_id IS NULL (comportamento antigo)
        * sub_cong_id == <id> => filtrar por sub
    """
    # usuário com perfil SEDE: mostrar "Todas as Congregações" + lista
    if user.role == "SEDE":
        congs_all = order_congs_sede_first(cong_options_for(user, db))
        labels = ["Todas as Congregações"] + [c.name for c in congs_all]
        sel_label = st.selectbox("Congregação", labels, key=f"{key_prefix}_cong_sel")
        if sel_label == "Todas as Congregações":
            cong_id = None
            sub_cong_id = SUB_ALL
            label = "Todas as Congregações"
        else:
            cong_obj = next((c for c in congs_all if c.name == sel_label), None)
            if cong_obj is None:
                st.error("Congregação selecionada não encontrada.")
                return None, None, None
            cong_id = cong_obj.id
            # carregar sub-congregações dessa congregação
            sub_congs = db.scalars(
                select(SubCongregation).where(SubCongregation.congregation_id == cong_obj.id)
            ).all()
            if sub_congs:
                mapping = {f"{cong_obj.name} (Principal)": None}
                for s in sub_congs:
                    mapping[s.name] = s.id
                chosen = st.selectbox(
                    "Unidade (sub-congregação)",
                    list(mapping.keys()),
                    key=f"{key_prefix}_sub_sel"
                )
                sub_cong_id = mapping[chosen]
            else:
                sub_cong_id = None
            label = cong_obj.name
    else:
        # usuário normal: fixo na sua congregação
        cong_obj = db.get(Congregation, user.congregation_id)
        if not cong_obj:
            st.error("Sua congregação não foi encontrada.")
            return None, None, None
        st.markdown(f"**Congregação:** {cong_obj.name}")
        cong_id = cong_obj.id
        sub_congs = db.scalars(
            select(SubCongregation).where(SubCongregation.congregation_id == cong_obj.id)
        ).all()
        if sub_congs:
            mapping = {f"{cong_obj.name} (Principal)": None}
            for s in sub_congs:
                mapping[s.name] = s.id
            chosen = st.selectbox(
                "Unidade (sub-congregação)",
                list(mapping.keys()),
                key=f"{key_prefix}_sub_sel_user"
            )
            sub_cong_id = mapping[chosen]
        else:
            sub_cong_id = None
        label = cong_obj.name

    return cong_id, sub_cong_id, label


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


def summarize_financials_for_ai(db, start_date, end_date, cong_id=None, sub_cong_id=None):
    """
    Retorna um dicionário resumo com:
      - total_dizimos (Tithe)
      - total_ofertas_culto (ServiceLog.oferta, EXCETO 'Culto de Missões')
      - total_ofertas_missoes (ServiceLog.oferta somente tipo 'Culto de Missões')
      - total_ofertas_transacoes (Transaction tipo entrada com categoria 'Oferta')
    Observa: mantém lógica separada entre 'ofertas culto' e 'ofertas missões' (não soma).
    """
    result = {
        "total_dizimos": 0.0,
        "total_ofertas_culto": 0.0,
        "total_ofertas_missoes": 0.0,
        "total_ofertas_transacoes": 0.0
    }

    # Dízimos (Tithe)
    conds = _build_common_date_and_congreg_filters(Tithe, start_date, end_date, cong_id, sub_cong_id)
    total_diz = db.scalar(select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(*conds)) or 0.0
    result["total_dizimos"] = float(total_diz)

    # ServiceLog: ofertas separadas por tipo
    conds_sl_all = _build_common_date_and_congreg_filters(ServiceLog, start_date, end_date, cong_id, sub_cong_id)
    # Missões
    total_missoes = db.scalar(
        select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(*conds_sl_all, ServiceLog.service_type == "Culto de Missões")
    ) or 0.0
    result["total_ofertas_missoes"] = float(total_missoes)
    # Cultos (exceto Missões)
    total_culto = db.scalar(
        select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(*conds_sl_all, ServiceLog.service_type != "Culto de Missões")
    ) or 0.0
    result["total_ofertas_culto"] = float(total_culto)

    # Transações de entrada com categoria 'Oferta' (checa Category)
    # JOIN Transaction -> Category
    tx_subconds = _build_common_date_and_congreg_filters(Transaction, start_date, end_date, cong_id, sub_cong_id)
    # buscar id(s) de categorias 'Oferta' (pode ser mais de uma variação)
    cat_offer_ids = [c.id for c in db.scalars(select(Category).where(func.lower(Category.name).like("%oferta%"), Category.type == TYPE_IN)).all()]
    if cat_offer_ids:
        total_tx_off = db.scalar(
            select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(*tx_subconds, Transaction.type == TYPE_IN, Transaction.category_id.in_(cat_offer_ids))
        ) or 0.0
        result["total_ofertas_transacoes"] = float(total_tx_off)
    else:
        result["total_ofertas_transacoes"] = 0.0

    return result


def format_currency_br(value):
    """Formata número float -> R$ 1.234,56 (simples)."""
    try:
        v = float(value or 0.0)
    except Exception:
        v = 0.0
    # separar inteiros e decimais
    inteiro = int(abs(v))
    dec = int(round((abs(v) - inteiro) * 100))
    s_int = f"{inteiro:,}".replace(",", ".")
    sign = "-" if v < 0 else ""
    return f"{sign}R$ {s_int},{dec:02d}"


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

# --- FIM DAS FUNÇÕES ---

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

st.markdown("""
<style>
.inline-missoes-alert{
  background: transparent !important;
  border: none !important;
  color: #b45309 !important;   /* âmbar escuro */
  font-weight: 700 !important; /* negrito */
}
</style>
""", unsafe_allow_html=True)


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
def voice_input_ui():
    """
    Cria um componente HTML/JS para capturar a fala do usuário e retornar o texto.
    """
    import streamlit.components.v1 as components

    html_code = """
    <style>
        #talk-btn {
            padding: 10px 15px;
            border-radius: 8px;
            background-color: #28a745; /* Verde */
            color: white;
            border: none;
            cursor: pointer;
            font-size: 16px;
            width: 100%;
            height: 48px;
            transition: background-color 0.2s;
        }
        #talk-btn:hover { background-color: #218838; }
        #talk-btn.listening { background-color: #dc3545; } /* Vermelho */
    </style>

    <button id="talk-btn">🎤 Falar</button>

    <script>
        const btn = document.getElementById('talk-btn');
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        
        if (!SpeechRecognition) {
            btn.innerHTML = "Voz não suportada";
            btn.disabled = true;
        } else {
            const recognition = new SpeechRecognition();
            recognition.lang = 'pt-BR';
            recognition.continuous = false;
            recognition.interimResults = false;

            recognition.onstart = function() {
                btn.innerHTML = "Ouvindo...";
                btn.classList.add("listening");
            };

            recognition.onresult = function(event) {
                const transcript = event.results[0][0].transcript;
                // Envia o texto transcrito de volta para o Python/Streamlit
                window.parent.Streamlit.setComponentValue(transcript);
            };

            recognition.onerror = function(event) {
                console.error("Erro no reconhecimento de voz:", event.error);
                btn.innerHTML = "Erro";
            };

            recognition.onend = function() {
                btn.innerHTML = "🎤 Falar";
                btn.classList.remove("listening");
            };

            btn.addEventListener('click', () => {
                recognition.start();
            });
        }
    </script>
    """
    transcribed_text = components.html(html_code, height=60)
    return transcribed_text

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
    from datetime import date
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


def summarize_financials_for_ai(db, start_date, end_date, cong_id=None, sub_cong_id=None):
    """
    Retorna um dicionário com totais e quebras necessárias para a IA responder:
      - total_dizimos
      - total_ofertas_culto (ofertas do resumo de culto + transações de 'oferta' não-missão)
      - total_ofertas_missoes (transações com categoria 'missões' e tx.type == ENTRADA)
      - total_entradas_por_categoria (dict categoria -> valor)
      - total_saidas_excl_missoes (SAÍDA excluindo categoria 'missões')
      - tithes_by_payment (dict pagamento -> valor)
    NOTA: mantém o comportamento de filtrar sub_congregation_id quando sub_cong_id é None.
    """
    # modelos: ajuste import se necessário
    from models import Tithe, Transaction, Category, ServiceLog

    # 1) Total de dízimos nominais
    conds_tithe = _build_common_date_and_congreg_filters(Tithe, start_date, end_date, cong_id, sub_cong_id)
    total_dizimos = float(db.scalar(select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(*conds_tithe)) or 0.0)

    # 2) Ofertas registradas no resumo de culto (ServiceLog.oferta) EXCLUINDO 'Culto de Missões'
    conds_service = _build_common_date_and_congreg_filters(ServiceLog, start_date, end_date, cong_id, sub_cong_id)
    conds_service.append(ServiceLog.service_type != "Culto de Missões")
    total_ofertas_service = float(db.scalar(select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(*conds_service)) or 0.0)

    # 3) Transações de ENTRADA agrupadas por categoria (para separar 'missões' de 'oferta' e outras)
    conds_tx_in = _build_common_date_and_congreg_filters(Transaction, start_date, end_date, cong_id, sub_cong_id)
    conds_tx_in.append(Transaction.type == TX_TYPE_IN)

    # Agrupa por categoria (nome em minúsculas) — soma por categoria
    rows = db.execute(
        select(func.lower(Category.name), func.coalesce(func.sum(Transaction.amount), 0.0))
        .join(Category, Transaction.category_id == Category.id)
        .where(*conds_tx_in)
        .group_by(func.lower(Category.name))
    ).all()

    total_ofertas_missoes = 0.0
    total_tx_in_other = 0.0
    tx_in_by_category = {}
    for cat_name_lower, soma in rows:
        valor = float(soma or 0.0)
        tx_in_by_category[cat_name_lower] = valor
        if cat_name_lower.strip() == "missões" or cat_name_lower.strip() == "missoes":
            total_ofertas_missoes += valor
        else:
            total_tx_in_other += valor

    # 4) Decide o que é considerado "oferta de culto" para apresentar ao usuário:
    #    - soma ServiceLog.oferta (resumo de culto, exceto Culto de Missões) + transações ENTRADA de categorias que contenham 'ofert'
    offers_tx_from_named_offers = 0.0
    for cat_lower, v in tx_in_by_category.items():
        if "ofert" in cat_lower:  # captura 'oferta', 'ofertas', etc.
            offers_tx_from_named_offers += v

    total_ofertas_culto = float(total_ofertas_service) + float(offers_tx_from_named_offers)

    # 5) Saídas (SAÍDA) excluindo categorias 'missões'
    conds_tx_out = _build_common_date_and_congreg_filters(Transaction, start_date, end_date, cong_id, sub_cong_id)
    conds_tx_out.append(Transaction.type == TX_TYPE_OUT)
    rows_out = db.execute(
        select(func.lower(Category.name), func.coalesce(func.sum(Transaction.amount), 0.0))
        .join(Category, Transaction.category_id == Category.id)
        .where(*conds_tx_out)
        .group_by(func.lower(Category.name))
    ).all()

    total_saidas_excl_missoes = 0.0
    tx_out_by_category = {}
    for cat_name_lower, soma in rows_out:
        valor = float(soma or 0.0)
        tx_out_by_category[cat_name_lower] = valor
        if cat_name_lower.strip() in ("missões", "missoes"):
            # ignora
            continue
        total_saidas_excl_missoes += valor

    # 6) Quebra de dizimos por forma de pagamento (se existir campo payment_method)
    tithe_pay_rows = db.execute(
        select(Tithe.payment_method, func.coalesce(func.sum(Tithe.amount), 0.0))
        .where(*conds_tithe)
        .group_by(Tithe.payment_method)
    ).all()
    tithes_by_payment = { (pm or "Não informado"): float(val or 0.0) for pm, val in tithe_pay_rows }

    # Monta o dicionário de retorno
    summary = {
        "total_dizimos": total_dizimos,
        "total_ofertas_culto": total_ofertas_culto,
        "total_ofertas_missoes": total_ofertas_missoes,
        "tx_in_by_category": tx_in_by_category,
        "tx_out_by_category": tx_out_by_category,
        "total_saidas_excl_missoes": total_saidas_excl_missoes,
        "tithes_by_payment": tithes_by_payment,
    }
    return summary


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
        


import datetime

def now_bahia():
    """
    Retorna datetime.now() seguro — usa datetime.datetime.now() para evitar
    o AttributeError quando 'datetime' foi importado como módulo.
    Se quiser validar timezone mais tarde, podemos ajustar aqui.
    """
    # se você quiser usar timezone fixa, podemos alterar aqui; por enquanto
    # retornamos a hora local do servidor.
    return datetime.datetime.now()

def today_bahia():
    """
    Retorna a data (date) atual baseada em now_bahia().
    """
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

# ===================== ENGINE / SESSION =====================
@st.cache_resource
def get_engine():
    db_url = os.environ.get("DATABASE_URL")
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
        "Assistente IA": "🤖", "Cadastro": "🛠️",
    }
    
    role = getattr(user, "role", "")
    if role == "SEDE":
        menu_options_plain = [
            "Lançamentos", "Relatório de Entrada", "Relatório de Saída",
            "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral",
            "Assistente IA", "Cadastro"
        ]
    elif role == "TESOUREIRO":
        menu_options_plain = [
            "Lançamentos", "Relatório de Entrada", "Relatório de Saída",
            "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral"
        ]
    elif role == "TESOUREIRO MISSIONÁRIO":
        menu_options_plain = ["Relatório de Missões", "Assistente IA"]
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
@st.cache_data(ttl=600) # Cache de 10 minutos para dados atualizados
def get_dashboard_summary(cong_id: int, start: date, end: date):
    """
    Busca e calcula os 5 totais financeiros essenciais para uma congregação e período.
    Agora considera ServiceLog.oferta como fonte alternativa para Ofertas (usa MAIOR entre fontes).
    """
    with SessionLocal() as db:
        # 1. Total de Saídas
        q_saidas = select(func.coalesce(func.sum(Transaction.amount), 0.0)).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type == 'SAÍDA'
        )
        total_saida = float(db.scalar(q_saidas) or 0.0)

        # 2. Total de Ofertas: calcular separadamente (transações) e (ServiceLog), depois usar max()
        q_ofertas_tx = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            Transaction.type.in_((TYPE_IN, "RECEITA")),
            func.lower(func.replace(Category.name, " ", "")) == "oferta"
        )
        total_oferta_tx = float(db.scalar(q_ofertas_tx) or 0.0)

        q_ofertas_sl = select(func.coalesce(func.sum(ServiceLog.oferta), 0.0)).where(
            ServiceLog.congregation_id == cong_id,
            ServiceLog.date >= start, ServiceLog.date < end
        )
        total_oferta_sl = float(db.scalar(q_ofertas_sl) or 0.0)

        # Aplica regra: use a maior soma entre as fontes
        total_oferta = max(total_oferta_tx, total_oferta_sl)

        # 3. Total de Dízimos (de Transações) — case-insensitive
        q_dizimos_trans = select(func.coalesce(func.sum(Transaction.amount), 0.0)).join(Category).where(
            Transaction.congregation_id == cong_id,
            Transaction.date >= start, Transaction.date < end,
            func.lower(func.replace(Category.name, " ", "")) == "dizimo"
        )
        total_dizimo_transacao = float(db.scalar(q_dizimos_trans) or 0.0)

        # 4. Total de Dízimos (Nominais)
        q_dizimos_nominal = select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
            Tithe.congregation_id == cong_id,
            Tithe.date >= start, Tithe.date < end
        )
        total_dizimo_nominal = float(db.scalar(q_dizimos_nominal) or 0.0)

        # Aplicando a regra de negócio para o total de dízimo
        total_dizimo = max(total_dizimo_transacao, total_dizimo_nominal)
        
        # Cálculos finais
        total_dizimo_mais_oferta = total_dizimo + total_oferta
        saldo = total_dizimo_mais_oferta - total_saida

        return {
            "total_dizimo": total_dizimo,
            "total_oferta": total_oferta,
            "total_dizimo_mais_oferta": total_dizimo_mais_oferta,
            "total_saida": total_saida,
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


def page_lancamentos(user: "User"):
    ensure_seed()

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

    with SessionLocal() as db:
        st.markdown(f"<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)

        # Seleção da congregação principal por perfil
        parent_cong_obj = None
        if user.role == "SEDE":
            congs_all = order_congs_sede_first(cong_options_for(user, db))
            cong_sel_name = st.selectbox(
                "Selecione a Congregação Principal:",
                [c.name for c in congs_all],
                key="lan_cong_sel_sede"
            )
            parent_cong_obj = next((c for c in congs_all if c.name == cong_sel_name), None)
        else:
            parent_cong_obj = db.get(Congregation, user.congregation_id)

        if not parent_cong_obj:
            st.error("Nenhuma congregação selecionada ou encontrada.")
            return

        st.markdown(f"### CONGREGAÇÃO: {parent_cong_obj.name.upper()}")

        modo = st.radio(
            "Modo de lançamento:",
            ["Formulário único", "Editar direto na tabela"],
            horizontal=True,
            key="lan_modo_sel"
        )
        st.divider()

        sub_congs = db.scalars(
            select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id)
        ).all()
        tipos_de_culto = [
            "Culto da Noite (Padrão)",
            "Trabalhos pela Manhã (EBD, CO, FESTIVIDADES)",
            "Culto de Missões",
            "Evento Especial",
            "Outro"
        ]

        # ========================== FORMULÁRIO ÚNICO ==========================
        if modo == "Formulário único":
            target_cong_obj = parent_cong_obj
            contexto_selecionado = f"{parent_cong_obj.name} (Principal)"
            target_sub_cong_id = None

            if sub_congs:
                opcoes = {f"{parent_cong_obj.name} (Principal)": None}
                for sub in sub_congs:
                    opcoes[sub.name] = sub.id
                contexto_selecionado = st.selectbox(
                    "Lançar em:", list(opcoes.keys()), key="lan_sub_sel_context_form"
                )
                target_sub_cong_id = opcoes[contexto_selecionado]

            st.markdown(f"#### Unidade selecionada: *{contexto_selecionado}*")
            st.divider()

            # ---- ENTRADA (Resumo do Culto)
            with st.expander("➕ Lançar ENTRADA (Resumo do Culto)", expanded=True):
                st.markdown('<div class="adrf-entrada">', unsafe_allow_html=True)
                with st.form("form_entrada_resumo"):
                    ent_data = st.date_input("Data do Culto", value=today_bahia(), key="ent_data_form")
                    ent_tipo = st.selectbox("Tipo de Culto", options=tipos_de_culto, key="ent_tipo_form")
                    c1, c2 = st.columns(2)
                    ent_dizimo = c1.number_input("Valor do Dízimo", min_value=0.0, value=0.0, format="%.2f", key="ent_dizimo_form")
                    ent_oferta = c2.number_input("Valor da Oferta", min_value=0.0, value=0.0, format="%.2f", key="ent_oferta_form")

                    if st.form_submit_button("Salvar Entrada do Culto"):
                        if ent_dizimo <= 0 and ent_oferta <= 0:
                            st.session_state.status_message = ("warning", "Nenhum valor foi inserido.")
                        else:
                            try:
                                # Busca log existente, tratando sub_congregacao corretamente
                                log_existente = db.scalar(
                                    select(ServiceLog).where(
                                        ServiceLog.date == ent_data,
                                        ServiceLog.service_type == ent_tipo,
                                        ServiceLog.congregation_id == target_cong_obj.id,
                                        ServiceLog.sub_congregation_id.is_(None) if target_sub_cong_id is None else (ServiceLog.sub_congregation_id == target_sub_cong_id)
                                    )
                                )

                                if ent_tipo == "Culto de Missões":
                                    # Ofertas de missões viram Transaction categoria 'Missões'
                                    if ent_oferta > 0:
                                        cat_missoes = db.scalar(
                                            select(Category).where(
                                                func.lower(Category.name) == 'missões',
                                                Category.type == TYPE_IN
                                            )
                                        )
                                        if cat_missoes:
                                            db.add(Transaction(
                                                date=ent_data, type=TYPE_IN,
                                                category_id=cat_missoes.id, amount=float(ent_oferta),
                                                description="Oferta do Culto de Missões",
                                                congregation_id=target_cong_obj.id,
                                                sub_congregation_id=target_sub_cong_id
                                            ))
                                        else:
                                            st.session_state.status_message = (
                                                "error",
                                                "ERRO: Categoria 'Missões' não encontrada. A oferta não foi salva."
                                            )
                                            db.rollback()
                                            st.rerun()

                                    if log_existente:
                                        log_existente.dizimo = (log_existente.dizimo or 0.0) + float(ent_dizimo)
                                    else:
                                        db.add(ServiceLog(
                                            date=ent_data, service_type=ent_tipo,
                                            dizimo=float(ent_dizimo), oferta=0.0,
                                            congregation_id=target_cong_obj.id,
                                            sub_congregation_id=target_sub_cong_id
                                        ))

                                    st.session_state.status_message = (
                                        "success",
                                        "Atenção: As ofertas do Culto de Missões são lançadas automaticamente no menu 'Relatório de Missões'."
                                    )
                                else:
                                    # Caso normal: grava ServiceLog (dízimo + oferta)
                                    if log_existente:
                                        log_existente.dizimo = (log_existente.dizimo or 0.0) + float(ent_dizimo)
                                        log_existente.oferta = (log_existente.oferta or 0.0) + float(ent_oferta)
                                    else:
                                        db.add(ServiceLog(
                                            date=ent_data, service_type=ent_tipo,
                                            dizimo=float(ent_dizimo), oferta=float(ent_oferta),
                                            congregation_id=target_cong_obj.id,
                                            sub_congregation_id=target_sub_cong_id
                                        ))
                                    st.session_state.status_message = ("success", "Registro de culto salvo com sucesso!")

                                # Commit + limpar cache para forçar leitura atualizada
                                try:
                                    db.commit()
                                    try:
                                        st.cache_data.clear()
                                    except Exception:
                                        # Não fatal: apenas garante que, se falhar, não interrompe a UX
                                        pass
                                except IntegrityError as ie:
                                    db.rollback()
                                    st.session_state.status_message = ("error", "Erro de integridade: possível lançamento duplicado para a mesma data/tipo/unidade.")
                                    st.exception(ie)
                                except Exception as e:
                                    db.rollback()
                                    st.session_state.status_message = ("error", f"Erro inesperado ao salvar: {str(e)}")
                                    st.exception(e)
                            except Exception as e:
                                try:
                                    db.rollback()
                                except Exception:
                                    pass
                                st.session_state.status_message = ("error", f"Erro ao processar entrada: {e}")
                                st.exception(e)

                        st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            # ---- DÍZIMO NOMINAL
            with st.expander("👤 Lançar DÍZIMO (Nominal)"):
                st.markdown('<div class="adrf-dizimo">', unsafe_allow_html=True)
                with st.form("form_dizimo"):
                    dz_data = st.date_input("Data do Dízimo", value=today_bahia(), key="dz_data")
                    dz_nome = st.text_input("Nome do dizimista", key="dz_nome")
                    dz_valor = st.number_input("Valor (R$)", min_value=0.0, value=0.0, format="%.2f", key="dz_valor")
                    dz_payment = st.selectbox("Forma de Pagamento", ["Dinheiro", "PIX", "Cartão", "Transferência"], key="dz_pay")
                    if st.form_submit_button("Salvar DIZIMISTA"):
                        if dz_valor > 0 and dz_nome.strip():
                            try:
                                db.add(Tithe(
                                    date=dz_data, tither_name=dz_nome.strip(), amount=float(dz_valor),
                                    congregation_id=target_cong_obj.id,
                                    sub_congregation_id=target_sub_cong_id,
                                    payment_method=dz_payment
                                ))
                                db.commit()
                                try:
                                    st.cache_data.clear()
                                except Exception:
                                    pass
                                st.session_state.status_message = ("success", "Dízimo registrado com sucesso!")
                            except Exception as e:
                                db.rollback()
                                st.session_state.status_message = ("error", f"Erro ao salvar dízimo: {e}")
                                st.exception(e)
                        else:
                            st.session_state.status_message = ("warning", "Preencha o nome e o valor do dízimo.")
                        st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

            # ---- SAÍDAS
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
                            try:
                                db.add(Transaction(
                                    date=sai_data, type="SAÍDA", category_id=cat_obj.id,
                                    amount=float(sai_valor), description=(sai_desc or None),
                                    congregation_id=target_cong_obj.id,
                                    sub_congregation_id=target_sub_cong_id
                                ))
                                db.commit()
                                try:
                                    st.cache_data.clear()
                                except Exception:
                                    pass
                                st.session_state.status_message = ("success", "Saída registrada com sucesso!")
                            except Exception as e:
                                db.rollback()
                                st.session_state.status_message = ("error", f"Erro ao salvar saída: {e}")
                                st.exception(e)
                        else:
                            st.session_state.status_message = ("warning", "Preencha o valor e a categoria da saída.")
                        st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

        # ====================== EDITAR DIRETO NA TABELA =======================
        elif modo == "Editar direto na tabela":
            contexto_tabela = f"{parent_cong_obj.name} (Principal)"
            target_sub_cong_id = None
            if sub_congs:
                opcoes_tabela = {f"{parent_cong_obj.name} (Principal)": None}
                for sub in sub_congs:
                    opcoes_tabela[sub.name] = sub.id
                contexto_tabela = st.selectbox(
                    "Selecione a unidade para editar:",
                    list(opcoes_tabela.keys()),
                    key="lan_tabela_contexto"
                )
                target_sub_cong_id = opcoes_tabela[contexto_tabela]

            st.info(f"Editando lançamentos de: **{contexto_tabela}**")

            ref_tab = get_month_selector("Mês de referência da tabela")
            start_tab, end_tab = month_bounds(ref_tab)

            st.markdown("##### Resumo de Entradas por Culto")

            df_logs = _load_service_logs(
                parent_cong_obj.id, start_tab, end_tab, sub_cong_id=target_sub_cong_id
            )

            # Divergência Dízimos (resumo x nominal)
            declarado_total = 0.0
            if isinstance(df_logs, pd.DataFrame) and not df_logs.empty and ("Dízimo" in df_logs.columns):
                try:
                    declarado_total = float(df_logs["Dízimo"].sum() or 0.0)
                except Exception:
                    declarado_total = 0.0
            with SessionLocal() as _db_chk:
                tithe_sub_filter = (
                    Tithe.sub_congregation_id.is_(None)
                    if target_sub_cong_id is None
                    else (Tithe.sub_congregation_id == target_sub_cong_id)
                )
                real_total = float(_db_chk.scalar(
                    select(func.coalesce(func.sum(Tithe.amount), 0.0)).where(
                        Tithe.congregation_id == parent_cong_obj.id,
                        Tithe.date >= start_tab, Tithe.date < end_tab,
                        tithe_sub_filter
                    )
                ) or 0.0)
            diff_total = round(declarado_total - real_total, 2)
            if abs(diff_total) >= 0.01:
                st.markdown(f"""
<div class="alert-danger">
  <strong>Divergência de Dízimos no período</strong> — Declarado no resumo: <strong>{format_currency(declarado_total)}</strong> • Nominal (dizimistas): <strong>{format_currency(real_total)}</strong> • Diferença: <strong>{format_currency(diff_total)}</strong>
</div>
""", unsafe_allow_html=True)

            if df_logs.empty:
                df_logs = pd.DataFrame([{
                    "Data do Culto": today_bahia(),
                    "Tipo de Culto": tipos_de_culto[0],
                    "Dízimo": 0.0,
                    "Oferta": 0.0,
                    "Total": 0.0,
                    "ID": None
                }])

            # --- Placeholder do aviso (fica ACIMA visualmente da tabela) ---
            _aviso_top = st.empty()

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
                    "Total": st.column_config.NumberColumn("Total", help="Soma do Dízimo e Oferta. Atualiza após salvar.", format="R$ %.2f", disabled=True)
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

            # Botão salvar mudanças do resumo (ServiceLog + Missões automática)
            def on_save_click():
                result = _apply_service_log_changes(
                    df_logs, edited_df, parent_cong_obj.id, sub_cong_id=target_sub_cong_id
                )
                # limpa cache para refletir mudanças imediatas
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
                    st.session_state.status_message = (
                        "error",
                        "Erro: Tentativa de criar um lançamento duplicado. Verifique os dados."
                    )
                elif result == "erro_categoria":
                    st.session_state.status_message = (
                        "error",
                        "ERRO CRÍTICO: Categoria 'Missões' (Entrada) não encontrada."
                    )
                elif result == "erro_geral":
                    st.session_state.status_message = (
                        "error",
                        "Ocorreu um erro inesperado ao salvar."
                    )
                st.rerun()

            st.button(
                "Salvar alterações na tabela",
                on_click=on_save_click,
                key=f"save_table_{parent_cong_obj.id}",
                type="primary"
            )

            # Seções auxiliares (dizimistas e saídas) abaixo
            st.markdown("---")
            tithes_query = select(Tithe).where(
                Tithe.congregation_id == parent_cong_obj.id,
                Tithe.date >= start_tab, Tithe.date < end_tab,
                (Tithe.sub_congregation_id.is_(None) if target_sub_cong_id is None else (Tithe.sub_congregation_id == target_sub_cong_id))
            )
            tithes = db.scalars(tithes_query.order_by(Tithe.date)).all()
            _editor_dizimos(
                tithes, f"Dizimistas - {contexto_tabela}",
                force_cong_id=parent_cong_obj.id, force_sub_cong_id=target_sub_cong_id
            )

            st.markdown("---")
            txs_out_query = select(Transaction).options(joinedload(Transaction.category)).where(
                Transaction.congregation_id == parent_cong_obj.id,
                Transaction.date >= start_tab, Transaction.date < end_tab,
                Transaction.type == "SAÍDA",
                (Transaction.sub_congregation_id.is_(None) if target_sub_cong_id is None else (Transaction.sub_congregation_id == target_sub_cong_id))
            )
            txs_out = db.scalars(txs_out_query.order_by(Transaction.date)).all()
            _editor_lancamentos(
                txs_out, f"Saídas - {contexto_tabela}", tx_type_hint="SAÍDA",
                force_cong_id=parent_cong_obj.id, force_sub_cong_id=target_sub_cong_id
            )


            # ... (demais seções permanecem iguais)


            # (O restante da página com as tabelas de Dizimistas e Saídas permanece igual)
            # ...

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
            df_principal = _load_service_logs(parent_cong_obj.id, start, end, sub_cong_id=None)
            all_units_data.append({"Unidade": f"{parent_cong_obj.name} (Principal)", "Total Entradas": df_principal['Total'].sum()})
            for sub in sub_congs:
                df_sub = _load_service_logs(parent_cong_obj.id, start, end, sub_cong_id=sub.id)
                all_units_data.append({"Unidade": f"↳ {sub.name}", "Total Entradas": df_sub['Total'].sum()})
            
            df_agg = pd.DataFrame(all_units_data)
            st.dataframe(df_agg.style.format({"Total Entradas": format_currency}), use_container_width=True, hide_index=True)
            total_geral = df_agg["Total Entradas"].sum()
            st.metric("Total Geral da Congregação", format_currency(total_geral))
        else:
            report_df = _load_service_logs(parent_cong_obj.id, start, end, sub_cong_id=target_sub_cong_id_or_all)
            
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
                
                # --- INÍCIO DA ALTERAÇÃO ---
                elif page == "Assistente IA":
                    page_assistente_ia(user)
                # --- FIM DA ALTERAÇÃO ---

                else:
                    page_visao_geral(user)
            else:
                # Caso raro: UID na sessão mas usuário não encontrado no DB. Força logout.
                logout()

    except Exception as e:
        st.error("Ocorreu um erro crítico na aplicação.")
        st.exception(e)

        # ===================== PAGE: ASSISTENTE IA ========================
# ===================== PAGE: ASSISTENTE IA (COM RESUMO RÁPIDO E ANÁLISE LIVRE) =====================
# ===================== PAGE: ASSISTENTE IA (COM ENTRADA DE VOZ) =====================
# ===================== PAGE: ASSISTENTE IA (ORDEM DE EXECUÇÃO CORRIGIDA) =====================
# ===================== PAGE: ASSISTENTE IA (VERSÃO ESTÁVEL E FINAL) =====================
def page_assistente_ia(user: "User"):
    """
    Página do Assistente IA (substitua pela sua versão antiga).
    Mantém a lógica existente — apenas usa now_bahia() / today_bahia() corrigidos.
    """
    ensure_seed()

    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Assistente IA</h1>", unsafe_allow_html=True)

        # Seleção do contexto (inclui 'Todas as Congregações' se sua função suportar)
        cong_id, sub_cong_id, cong_label = render_ai_context_selector(user, db, key_prefix="ai")

        # Seleção do mês/ano de referência — não passa 'key' (compatível com sua get_month_selector)
        ref_tab = get_month_selector("Mês de Referência — Mês")
        start_tab, end_tab = month_bounds(ref_tab)

        st.divider()

        # Campo de pergunta
        st.subheader("2. Faça sua Pergunta")
        question_text = st.text_area(
            "Sua pergunta:",
            placeholder="Ex: Qual o total de dízimos? Liste as 3 maiores saídas. Quem foi o dizimista com maior valor?",
            key="ai_question_text",
            height=140
        )

        # Botão para analisar com IA
        analyze_clicked = st.button("Analisar com IA", key="ai_analyze_btn", type="primary")

        # Container para resposta
        resposta_container = st.empty()

        if analyze_clicked:
            # Permissões: se não for SEDE e não escolheu congregação válida, bloqueia
            if cong_id is None and user.role != "SEDE":
                resposta_container.error("Você não tem permissão para consultar 'Todas as Congregações'.")
                return

            try:
                # Consulta resumo (reabre sessão para segurança)
                with SessionLocal() as db_q:
                    summary = summarize_financials_for_ai(
                        db_q, start_tab, end_tab,
                        cong_id=cong_id, sub_cong_id=sub_cong_id
                    )

                # Monta label do período (robusto)
                try:
                    period_label = ref_tab.strftime("%m/%Y")
                except Exception:
                    period_label = str(ref_tab)

                # Formata resposta limpa — sem listar fontes ou dados extras
                lines = []
                lines.append(f"**Resumo financeiro — {cong_label} — {period_label}**")
                lines.append("")

                total_dizimos = summary.get("total_dizimos", 0.0)
                total_ofertas_culto = summary.get("total_ofertas_culto", 0.0)
                total_ofertas_missoes = summary.get("total_ofertas_missoes", 0.0)
                total_ofertas_transacoes = summary.get("total_ofertas_transacoes", 0.0)

                lines.append(f"- **Total Dízimos:** {format_currency_br(total_dizimos)}")
                lines.append(f"- **Total Ofertas (Cultos):** {format_currency_br(total_ofertas_culto)}")
                lines.append(f"- **Total Ofertas (Missões):** {format_currency_br(total_ofertas_missoes)}")
                lines.append(f"- **Total Ofertas (Categoria 'Oferta' - transações):** {format_currency_br(total_ofertas_transacoes)}")

                # Breakdown por forma de pagamento (opcional)
                pay_breakdown = summary.get("by_payment_method")
                if isinstance(pay_breakdown, dict) and pay_breakdown:
                    lines.append("")
                    lines.append("- **Dízimos por forma de pagamento:**")
                    for pm, val in pay_breakdown.items():
                        lines.append(f"  - {pm}: {format_currency_br(val)}")

                resposta_md = "\n".join(lines)

                # Exibe a resposta de forma limpa (caixa de informação)
                resposta_container.info(resposta_md)

            except Exception as e:
                resposta_container.error("Ocorreu um erro ao gerar a análise. Verifique os logs do servidor.")
                import traceback
                print("Erro em page_assistente_ia:", e)
                traceback.print_exc()


            # ... (O restante do código para o Tesoureiro Missionário permanece o mesmo)
            # ...
            
if __name__ == "__main__":
    main()
