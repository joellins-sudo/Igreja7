def page_relatorio_missoes_congregacao(user: "User"):
    # Página do TESOUREIRO (congregações) — mostra APENAS o SALDO do mês
    if user.role != "TESOUREIRO":
        st.warning("🔒 Acesso restrito aos usuários TESOUREIRO (congregações).")
        return

    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)
        st.markdown("<h1 class='page-title'>Relatório de Missões (Minha Congregação)</h1>", unsafe_allow_html=True)

        ref = get_month_selector()
        start, end = month_bounds(ref)

        if not user.congregation_id:
            st.info("Sua conta não está vinculada a uma congregação.")
            return

        # Coleta apenas os lançamentos da congregação do usuário
        entradas, saidas = _collect_missions_data(db, start, end, only_cong_id=user.congregation_id)
        total_in = sum(float(t.amount) for t in entradas)
        total_out = sum(float(t.amount) for t in saidas)
        saldo_mes = float(total_in - total_out)

        # === Somente SALDO em destaque ===
        st.metric("Saldo de Missões (mês corrente)", format_currency(saldo_mes))

        st.divider()
        st.subheader("Baixar Relatório (PDF)")
        st.download_button(
            "⬇️ Baixar PDF (Missões da minha congregação)",
            data=build_missions_report_pdf(ref, entradas, saidas),
            file_name=f"relatorio_missoes_congregacao_{start.strftime('%Y-%m')}.pdf",
            mime="application/pdf"
        )
