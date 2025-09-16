def page_visao_geral(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        # sidebar_common(user) <--- CHAMADA REMOVIDA
        
        st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)
        
        congs = cong_options_for(user, db)
        ordered = order_congs_sede_first(congs)
        
        is_all = (user.role == "SEDE")
        if is_all:
            st.info("Escopo: **Todas as congregações**")
        elif congs:
            cong_obj = congs[0]
            st.info(f"Escopo: **{cong_obj.name}**")
        else:
            st.info("Sem congregação vinculada.")
            return

        agg_total = []
        if is_all:
            for c in ordered:
                totals = _collect_month_data(db, c.id, start, end)["totals"]
                agg_total.append((
                    c.name,
                    totals["entradas_total_sem_missoes"],
                    totals["saidas_total"],
                    totals["saldo"],
                    totals["missoes"]
                ))
        elif congs:
            cong_obj = congs[0]
            totals = _collect_month_data(db, cong_obj.id, start, end)["totals"]
            agg_total.append((
                cong_obj.name,
                totals["entradas_total_sem_missoes"],
                totals["saidas_total"],
                totals["saldo"],
                totals["missoes"]
            ))

        # ==== SEDE (todas as congregações) ====
        if user.role == "SEDE":
            df_rank = pd.DataFrame([{
                "Congregação": n,
                "Entradas (D+O + Outras)": v,
                "Saídas": s,
                "Saldo": sal
            } for (n, v, s, sal, _m) in agg_total])

            if not df_rank.empty:
                df_sorted = df_rank.sort_values("Entradas (D+O + Outras)", ascending=False).reset_index(drop=True)
                top_n = min(5, len(df_sorted))
                cols = st.columns(top_n)
                for i in range(top_n):
                    row = df_sorted.iloc[i]
                    label = f"{i+1}º lugar"
                    text = f"{row['Congregação']} — {format_currency(float(row['Entradas (D+O + Outras)']))}"
                    render_stat_card(cols[i], label, text)

                st.divider()
                st.dataframe(
                    df_sorted.assign(**{
                        "Entradas (D+O + Outras)": df_sorted["Entradas (D+O + Outras)"].map(lambda x: format_currency(float(x))),
                        "Saídas": df_sorted["Saídas"].map(lambda x: format_currency(float(x))),
                        "Saldo": df_sorted["Saldo"].map(lambda x: format_currency(float(x))),
                    }),
                    use_container_width=True, hide_index=True, height=200
                )
            else:
                st.caption("Sem dados neste mês.")

            # === [BLOCO 9: Totais gerais — Entradas, Saídas e Saldo (todas as congregações)] ===
            try:
                _tot_in   = sum(float(v)    for (_n, v, _s, _sal, _m) in agg_total)
                _tot_out   = sum(float(_s)   for (_n, _v, _s, _sal, _m) in agg_total)
                _tot_saldo = sum(float(_sal) for (_n, _v, _s, _sal, _m) in agg_total)
            except Exception:
                _tot_in = _tot_out = _tot_saldo = 0.0

            c1, c2, c3 = st.columns(3)
            c1.metric("Total de Entradas (todas as congregações)", format_currency(_tot_in))
            c2.metric("Total de Saídas (todas as congregações)", format_currency(_tot_out))
            c3.metric("Saldo (todas as congregações)", format_currency(_tot_saldo))
            # === [FIM BLOCO 9] ===

            st.divider()
            st.subheader("Relatório Consolidado Mensal")
            st.download_button(
                "⬇️ Baixar PDF do Relatório Geral",
                data=build_consolidated_pdf(agg_total, ref),
                file_name=f"relatorio_mensal_{start.strftime('%Y-%m')}.pdf",
                mime="application/pdf",
                key=f"dl_pdf_relatorio_geral_{start.strftime('%Y_%m')}"
            )

        # ==== Tesoureiro (apenas sua congregação) ====
        if user.role != "SEDE" and agg_total:
            st.divider()
            st.subheader("Resumo Financeiro Mensal")

            # Totais detalhados para a congregação do usuário (dízimos e ofertas separadas)
            with SessionLocal() as _db_vg:
                _tot = _collect_month_data(_db_vg, cong_obj.id, start, end)["totals"]

            _dz = float(_tot.get("dizimos", 0.0))
            _of = float(_tot.get("ofertas", 0.0))
            _dz_of = _dz + _of
            _sa = float(_tot.get("saidas_total", 0.0))
            _saldo = float(_tot.get("saldo", 0.0))

            df_summary_5 = pd.DataFrame([{
                "Dízimos Total": format_currency(_dz),
                "Ofertas Total": format_currency(_of),
                "Dízimos + Ofertas": format_currency(_dz_of),
                "Total Saídas": format_currency(_sa),
                "Saldo": format_currency(_saldo),
            }])

            st.dataframe(df_summary_5, use_container_width=True, hide_index=True)

        # ==== PDF completo por congregação ====
        st.subheader("Prestação de contas (PDF completo)")
        if user.role == "SEDE":
            sel = st.selectbox(
                "Congregação",
                [c.name for c in ordered],
                key=f"pc_cong_sel_vg_{start.strftime('%Y_%m')}"
            )
            cong_obj = next(c for c in ordered if c.name == sel)
        else:
            cong_obj = ordered[0]

        st.download_button(
            "⬇️ Baixar PDF do mês (completo)",
            data=build_full_statement_pdf(cong_obj.id, cong_obj.name, ref),
            file_name=f"prestacao_{_norm(cong_obj.name)}_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf",
            key=f"dl_pdf_prestacao_{_norm(cong_obj.name)}_{start.strftime('%Y_%m')}"
        )

        # === [FIM BLOCO 8] ===

        if user.role == "SEDE":
            st.divider()
            st.subheader("Relatório Consolidado Mensal")
            st.download_button(
                "⬇️ Baixar PDF do Relatório Geral",
                data=build_consolidated_pdf(agg_total, ref),
                file_name=f"relatorio_mensal_{start.strftime('%Y-%m')}.pdf",
                mime="application/pdf"
            )

        st.subheader("Prestação de contas (PDF completo)")
        if user.role == "SEDE":
            sel = st.selectbox("Congregação", [c.name for c in ordered], key="pc_cong_sel")
            cong_obj = next(c for c in ordered if c.name == sel)
        else:
            cong_obj = ordered[0]
        st.download_button(
            "⬇️ Baixar PDF do mês (completo)",
            data=build_full_statement_pdf(cong_obj.id, cong_obj.name, ref),
            file_name=f"prestacao_{_norm(cong_obj.name)}_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf"
        )
