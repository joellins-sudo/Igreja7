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
            escopo_opts = [
                "-- Relatório Hierárquico (Visualização) --", 
                "-- Visão Agregada (Editável) --"
            ] + [c.name for c in congs_all]
            
            escopo_selecionado = st.selectbox("Selecione o escopo do relatório:", escopo_opts, key="re_sede_escopo")
            
            if escopo_selecionado == "-- Relatório Hierárquico (Visualização) --":
                display_entry_hierarchy(congs_all, start, end, db)
                return
            elif escopo_selecionado == "-- Visão Agregada (Editável) --":
                st.info("Modo de edição do total de entradas por congregação principal.")
                _editor_entradas_agg_all(congs_all, start, end)
                return
            else:
                parent_cong_obj = next((c for c in congs_all if c.name == escopo_selecionado), None)
        else: # TESOUREIRO
            parent_cong_obj = db.get(Congregation, user.congregation_id)

        if not parent_cong_obj:
            st.info("Nenhuma congregação para analisar."); return

        st.divider()
        sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id).order_by(SubCongregation.name)).all()
        
        opcoes = {"-- Todas (Principal + Subs) --": "ALL"}
        opcoes[parent_cong_obj.name + " (Principal)"] = None
        for sub in sub_congs:
            opcoes[sub.name] = sub.id

        contexto_selecionado = st.selectbox("Filtrar por unidade:", list(opcoes.keys()), key="re_sub_sel")
        target_sub_cong_id_or_all = opcoes[contexto_selecionado]
        
        st.info(f"Exibindo dados para: **{contexto_selecionado}**")

        if target_sub_cong_id_or_all == "ALL":
            all_units = [(parent_cong_obj.name + " (Principal)", None)] + [(s.name, s.id) for s in sub_congs]
            rows, total_geral = [], 0.0
            for name, sub_id in all_units:
                totals = _collect_month_data(db, parent_cong_obj.id, start, end, sub_cong_id=sub_id)["totals"]
                total_entradas_unidade = totals["entradas_total_sem_missoes"]
                rows.append({
                    "Unidade": name,
                    "Dízimos": totals["dizimos"],
                    "Ofertas": totals["ofertas"],
                    "Total Entradas": total_entradas_unidade
                })
                total_geral += total_entradas_unidade
            
            df_agg = pd.DataFrame(rows)
            st.dataframe(df_agg.style.format({"Dízimos": format_currency, "Ofertas": format_currency, "Total Entradas": format_currency}), use_container_width=True)
            st.metric("Total Geral de Entradas (Principal + Subs)", format_currency(total_geral))
        
        else:
            base_df = _entrada_summary_df(db, parent_cong_obj.id, start, end, sub_cong_id=target_sub_cong_id_or_all)
            
            edited_df = st.data_editor(
                base_df,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                column_config={
                    "Data do Culto": st.column_config.DateColumn("Data", required=True, format="DD/MM/YYYY"),
                    "Dízimo": st.column_config.NumberColumn("Dízimo (R$)", format="R$ %.2f"),
                    "Oferta": st.column_config.NumberColumn("Oferta (R$)", format="R$ %.2f"),
                    "Total": st.column_config.NumberColumn("Total (R$)", disabled=True, format="R$ %.2f"),
                },
                key="re_editor_detalhado"
            )

            try:
                total_dizimo, total_oferta, total_geral_unidade = 0.0, 0.0, 0.0
                if isinstance(edited_df, pd.DataFrame) and not edited_df.empty:
                    df_calc = edited_df.copy()
                    df_calc["Dízimo"] = df_calc["Dízimo"].map(_to_float_brl)
                    df_calc["Oferta"] = df_calc["Oferta"].map(_to_float_brl)
                    total_dizimo = df_calc["Dízimo"].sum()
                    total_oferta = df_calc["Oferta"].sum()
                    total_geral_unidade = total_dizimo + total_oferta
            except Exception: pass
            
            st.divider()
            col1, col2, col3 = st.columns(3)
            col1.metric("Soma Dízimos (tabela)", format_currency(total_dizimo))
            col2.metric("Soma Ofertas (tabela)", format_currency(total_oferta))
            col3.metric("Soma Geral (tabela)", format_currency(total_geral_unidade))

            def _save_summary():
                _apply_entrada_summary_changes(parent_cong_obj.id, start, end, edited_df, sub_cong_id=target_sub_cong_id_or_all)
                st.toast("💾 Alterações salvas com sucesso!", icon="✅")
                st.rerun()

            _save_btn(_save_summary, "entrada_sum_detalhado", theme="entrada")
