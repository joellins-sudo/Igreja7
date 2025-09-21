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
            
            # --- SECÇÃO DE PESQUISA COMPLETA ---
            c1, c2 = st.columns(2)
            with c1:
                ano_pesq = st.number_input("Ano da Pesquisa", value=today_bahia().year, step=1, format="%d", key="missions_search_year")
            with c2:
                mes_opt = ["Todos"] + MONTHS
                mes_sel = st.selectbox("Mês da Pesquisa", mes_opt, index=0, key="missions_search_month")

            df_search, total_periodo, num_congs, top_month, top_year = _build_missions_analytics(db, date(ano_pesq, MONTHS.index(mes_sel)+1 if mes_sel != "Todos" else 1, 1))

            st.divider()
            
            st.markdown("##### Destaques do Período")
            c1, c2, c3 = st.columns(3)
            c1.metric("Total de Entradas", format_currency(total_periodo))
            c2.metric("Nº de Congregações Contribuintes", f"{num_congs}")
            
            # Métrica do maior contribuinte do mês
            if top_month:
                c3.metric("Principal Contribuinte (Mês)", top_month[0], f"{format_currency(top_month[1])}")
            else:
                c3.metric("Principal Contribuinte (Mês)", "N/A")

            # Métrica do maior contribuinte do ano
            if top_year:
                st.metric("Principal Contribuinte (Ano)", top_year[0], f"{format_currency(top_year[1])}")
            else:
                st.metric("Principal Contribuinte (Ano)", "N/A")

            st.markdown("###### Tabela de Contribuições")
            if not df_search.empty:
                st.dataframe(
                    df_search.style.format({"Total no Mês (R$)": format_currency, "Total no Ano (R$)": format_currency}),
                    use_container_width=True, hide_index=True
                )
            else:
                st.caption("Nenhuma contribuição de missões encontrada para os filtros selecionados.")
