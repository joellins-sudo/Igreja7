# main.py — AD Relatório Financeiro — v9.2 (libera exclusão para todas as congregações)
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

# ... [CSS, assets, locale, utilidades, DB base, models e setup de conexão/usuários iguais ao seu código original] ...
# (Tudo até a definição das páginas, igual)

# ===================== PAGE: RELATÓRIO DE ENTRADA =====================
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

        # ... [DADOS E RESUMOS IGUAIS] ...

        st.divider()
        rows_csv = [{
            "Data": t.date.strftime("%Y-%m-%d"),
            "Congregação": t.congregation.name,
            "Categoria": t.category.name,
            "Valor": float(t.amount),
            "Descrição": t.description or ""
        } for t in data["tx_in"]]
        csv = pd.DataFrame(rows_csv).to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Baixar CSV das ENTRADAS do período", data=csv, file_name=f"entradas_{start.strftime('%Y-%m')}.csv", mime="text/csv")

        # ===== Exclusões (TODOS PODEM EXCLUIR DA SUA CONGREGAÇÃO) =====
        if not is_all:
            st.divider()
            st.subheader("Exclusões (sua congregação)")

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
                            _db.query(Transaction).filter(Transaction.id.in_(ids)).delete(synchronize_session=False)
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
                            _db.query(Tithe).filter(Tithe.id.in_(ids2)).delete(synchronize_session=False)
                            _db.commit()
                        st.success(f"{len(ids2)} dízimo(s) excluído(s).")
                        st.rerun()
                else:
                    st.caption("Sem dízimos no período/escopo.")

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

        # ... [RESUMOS E TABELAS IGUAIS] ...

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

        # ===== Exclusão de SAÍDAS para todos =====
        if not is_all:
            st.divider(); st.subheader("Excluir SAÍDAS (sua congregação)")
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

# ... [Todas as outras páginas e funções permanecem IGUAIS ao seu arquivo anterior] ...

# ===================== MAIN =====================
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
