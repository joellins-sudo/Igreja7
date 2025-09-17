def page_visao_geral(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        st.markdown("<h1 class='page-title'>Visão Geral</h1>", unsafe_allow_html=True)
        ref = get_month_selector()
        start, end = month_bounds(ref)
        
        congs = cong_options_for(user, db)
        ordered_congs = order_congs_sede_first(congs)
        
        if user.role == "SEDE":
            st.info("Escopo: **Todas as congregações**")
            agg_total = []
            for c in ordered_congs:
                cong_total_entradas = 0.0
                cong_total_saidas = 0.0
                principal_totals = _collect_month_data(db, c.id, start, end, sub_cong_id=None)["totals"]
                cong_total_entradas += principal_totals["entradas_total_sem_missoes"]
                cong_total_saidas += principal_totals["saidas_total"]
                sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == c.id)).all()
                for sub in sub_congs:
                    sub_totals = _collect_month_data(db, c.id, start, end, sub_cong_id=sub.id)["totals"]
                    cong_total_entradas += sub_totals["entradas_total_sem_missoes"]
                    cong_total_saidas += sub_totals["saidas_total"]
                agg_total.append((c.name, cong_total_entradas, cong_total_saidas, cong_total_entradas - cong_total_saidas))

            df_rank = pd.DataFrame([{"Congregação": n, "Entradas": v, "Saídas": s, "Saldo": sal} for (n, v, s, sal) in agg_total])
            if not df_rank.empty:
                df_sorted = df_rank.sort_values("Entradas", ascending=False).reset_index(drop=True)
                st.dataframe(df_sorted.style.format({"Entradas": format_currency, "Saídas": format_currency, "Saldo": format_currency}), use_container_width=True)
                
                _tot_in  = sum(v for (_, v, _, _) in agg_total)
                _tot_out = sum(s for (_, _, s, _) in agg_total)
                _tot_saldo = _tot_in - _tot_out

                st.divider()
                c1, c2, c3 = st.columns(3)
                c1.metric("Total de Entradas (geral)", format_currency(_tot_in))
                c2.metric("Total de Saídas (geral)", format_currency(_tot_out))
                c3.metric("Saldo (geral)", format_currency(_tot_saldo))
        
        elif congs:
            parent_cong_obj = congs[0]
            st.info(f"Escopo: **{parent_cong_obj.name} e suas unidades**")
            sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id).order_by(SubCongregation.name)).all()
            all_units = [(f"{parent_cong_obj.name} (Principal)", None)] + [(s.name, s.id) for s in sub_congs]
            rows = []
            for name, sub_id in all_units:
                totals = _collect_month_data(db, parent_cong_obj.id, start, end, sub_cong_id=sub_id)["totals"]
                rows.append({"Unidade": name, "Entradas": totals["entradas_total_sem_missoes"], "Saídas": totals["saidas_total"], "Saldo": totals["saldo"]})
            df_summary = pd.DataFrame(rows)
            st.dataframe(df_summary.style.format({"Entradas": format_currency, "Saídas": format_currency, "Saldo": format_currency}), use_container_width=True, hide_index=True)
        else:
            st.info("Sem congregação vinculada.")
            return

        st.divider()
        st.subheader("Downloads de Relatórios (PDF)")
        
        if user.role == "SEDE":
            st.markdown("###### Relatório Geral Consolidado")
            st.download_button(
                "⬇️ Baixar PDF Geral (Hierárquico)",
                data=build_consolidated_pdf(ordered_congs, ref, db),
                file_name=f"relatorio_geral_detalhado_{ref.strftime('%Y-%m')}.pdf",
                mime="application/pdf",
                key="dl_pdf_geral"
            )
            
            st.markdown("###### Relatório Individual por Unidade")
            unit_options = {}
            for cong in ordered_congs:
                unit_options[f"{cong.name} (Principal)"] = {"cong_id": cong.id, "sub_id": None}
                sub_congs_pdf = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == cong.id).order_by(SubCongregation.name)).all()
                for sub in sub_congs_pdf:
                    unit_options[f"{cong.name} → {sub.name}"] = {"cong_id": cong.id, "sub_id": sub.id}
            
            sel_unit_name = st.selectbox("Selecione a unidade para gerar o PDF:", list(unit_options.keys()), key="vg_sel_unit_pdf")
            
            if sel_unit_name:
                selected_unit_info = unit_options[sel_unit_name]
                st.download_button(
                    f"⬇️ Baixar PDF de {sel_unit_name}",
                    data=build_single_unit_report_pdf(
                        cong_id=selected_unit_info["cong_id"],
                        sub_cong_id=selected_unit_info["sub_id"],
                        unit_name=sel_unit_name,
                        ref=ref,
                        db=db
                    ),
                    file_name=f"prestacao_{_norm(sel_unit_name)}_{ref.strftime('%Y-%m')}.pdf",
                    mime="application/pdf",
                    key=f"dl_pdf_unit_{_norm(sel_unit_name)}"
                )
        else: # TESOUREIRO
            parent_cong_obj = congs[0]
            st.download_button(
                f"⬇️ Baixar PDF de {parent_cong_obj.name} (Detalhado)",
                data=build_full_statement_pdf(parent_cong_obj.id, ref, db),
                file_name=f"prestacao_{_norm(parent_cong_obj.name)}_{ref.strftime('%Y-%m')}.pdf",
                mime="application/pdf",
                key=f"dl_pdf_prestacao_tesoureiro"
            )
