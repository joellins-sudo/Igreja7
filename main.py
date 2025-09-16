def page_relatorio_saida(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        # sidebar_common(user) <--- CHAMADA REMOVIDA

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

        if is_all:
            st.info("Escopo: **Todas as congregações** — edite o total mensal de saídas por congregação abaixo.")
            _editor_saidas_agg_all(ordered, start, end)

            # === [BLOCO 7: Total geral de SAÍDAS (todas as congregações)] ===
            with SessionLocal() as _db_tot_out:
                total_geral_out = 0.0
                for _c in ordered:
                    _t = _collect_month_data(_db_tot_out, _c.id, start, end)["totals"]
                    total_geral_out += float(_t["saidas_total"])
            st.metric("Total geral de SAÍDAS (todas as congregações)", format_currency(total_geral_out))
            # === [FIM DO BLOCO 7] ===

            st.divider()
            with SessionLocal() as db2:
                rows = []
                for c in ordered:
                    total = _collect_month_data(db2, c.id, start, end)["totals"]["saidas_total"]
                    rows.append({"Congregação": c.name, "Total Saídas (R$)": float(total)})
            csv = pd.DataFrame(rows).to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Baixar CSV (Saídas por congregação)", data=csv, file_name=f"saidas_congregacoes_{start.strftime('%Y-%m')}.csv", mime="text/csv")
            return

        if not cong_obj:
            st.info("Sem congregação vinculada."); return
        st.info(f"Escopo: **{cong_obj.name}**")

        q = select(Transaction).options(joinedload(Transaction.category)).where(
            Transaction.date >= start, Transaction.date < end, Transaction.type.in_(("SAÍDA", "DESPESA")),
            Transaction.congregation_id == cong_obj.id
        )
        txs = db.scalars(q).all()

        total_saidas = sum(float(t.amount) for t in txs)
        st.metric("Total de saídas", format_currency(total_saidas))

        st.divider()
        _editor_lancamentos(txs, "Saídas do período (editar na tabela)", tx_type_hint=TYPE_OUT)

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
