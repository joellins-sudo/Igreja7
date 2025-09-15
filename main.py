def page_lancamentos(user: "User"):
    ensure_seed()
    with SessionLocal() as db:
        sidebar_common(user)

        st.markdown(f"<h1 class='page-title'>Lançamentos</h1>", unsafe_allow_html=True)

        # --- congregações disponíveis para o usuário ---
        congs = cong_options_for(user, db)
        if not congs:
            st.info("Nenhuma congregação disponível.")
            return

        # Seleção de congregação (SEDE escolhe; demais já vêm vinculados)
        if user.role == "SEDE":
            congs_ordered = order_congs_sede_first(congs)
            cong_sel = st.selectbox(
                "Selecione a congregação",
                [c.name for c in congs_ordered],
                key="lan_cong_sel"
            )
            cong_obj = next(c for c in congs_ordered if c.name == cong_sel)
        else:
            cong_obj = congs[0]

        st.markdown(f"<div class='cong-title'>CONGREGAÇÃO: {cong_obj.name.upper()}</div>", unsafe_allow_html=True)

        # ================== TABELA: Agregado Diário (Dízimo + Oferta) ==================
        st.subheader("Lançamento Agregado Diário (Dízimo e Oferta)")
        with st.expander("Clique para Inserir ou Editar Dízimo/Oferta por Data", expanded=False):
            st.info(f"Escopo: **{cong_obj.name}** — edite as linhas abaixo.")

            ref_tab = get_month_selector("Mês da tabela")
            start_tab, end_tab = month_bounds(ref_tab)

            df = _entrada_summary_df(db, cong_obj.id, start_tab, end_tab)
            if df.empty:
                df = pd.DataFrame([{
                    "Data do Culto": today_bahia(),
                    "Dízimo": 0.0,
                    "Oferta": 0.0,
                    "Total": 0.0
                }])
            df = df.copy()
            try:
                df["Dízimo"] = df["Dízimo"].map(float)
                df["Oferta"] = df["Oferta"].map(float)
            except Exception:
                pass
            df["Total"] = df["Dízimo"] + df["Oferta"]

            edited_tab = st.data_editor(
                df[["Data do Culto", "Dízimo", "Oferta", "Total"]],
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                column_config={
                    "Data do Culto": st.column_config.DateColumn("Data do Culto", required=True, format="DD/MM/YYYY"),
                    "Dízimo": st.column_config.NumberColumn("Dízimo (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
                    "Oferta": st.column_config.NumberColumn("Oferta (R$)", min_value=0.0, step=1.0, format="R$ %.2f"),
                    "Total": st.column_config.NumberColumn("Total (R$)", disabled=True, format="R$ %.2f"),
                },
                key=f"lan_tab_editor_{cong_obj.id}_{start_tab:%Y_%m}",
            )

            # métrica do mês (tabela)
            try:
                _sum_total_mes = float(
                    edited_tab.assign(
                        **{
                            "Dízimo": edited_tab["Dízimo"].map(_to_float_brl),
                            "Oferta": edited_tab["Oferta"].map(_to_float_brl)
                        }
                    ).eval("Dízimo + Oferta").sum()
                )
            except Exception:
                _sum_total_mes = 0.0
            st.metric("Total de Entradas (Dízimo + Oferta) no mês", format_currency(_sum_total_mes))

            def _save_tab():
                _apply_entrada_summary_changes(cong_obj.id, start_tab, end_tab, edited_tab)
                st.toast("💾 Tabela salva com sucesso.", icon="✅")
                st.rerun()

            _save_btn(_save_tab, f"lan_tab_{cong_obj.id}_{start_tab:%Y_%m}")

        st.markdown("---")

        # ================== FORMULÁRIO: ENTRADA ==================
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar ENTRADA (Doação)")
        with st.form("form_entrada", clear_on_submit=True):
            c1, c2, c3 = st.columns([1.1, 1.6, 2])
            ent_data = st.date_input("Data do Culto", value=today_bahia(), key="ent_data", format="DD/MM/YYYY")

            with c2:
                cats_in = categories_for_type(db, TYPE_IN)
                cats_in = [c for c in cats_in if "ajuste" not in _norm(c.name)]
                cat_names_in = [c.name for c in cats_in] or ["—"]
                desired = ["Dízimo", "Oferta", "Missões"]
                desired_norm = [_norm(x) for x in desired]
                top = [n for n in cat_names_in if _norm(n) in desired_norm]
                rest = [n for n in cat_names_in if _norm(n) not in desired_norm]
                cat_display = top + rest
                ent_cat = st.selectbox("Categoria", cat_display, key="ent_cat")

            ent_desc = st.text_input("Descrição (opcional)", key="ent_desc")
            ent_flag_missoes = _norm(ent_cat) == "oferta" and st.checkbox("Oferta de missões?", key="ent_flag_missoes")
            ent_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="ent_valor")

            if st.form_submit_button("Salvar ENTRADA", type="primary"):
                with SessionLocal() as _db:
                    cat_name = "Missões" if ent_flag_missoes else ent_cat
                    if ent_flag_missoes and not _db.scalar(select(Category).where(Category.name == "Missões")):
                        _db.add(Category(name="Missões", type=TYPE_IN)); _db.commit()
                    cat_obj = _db.scalar(select(Category).where(Category.name == cat_name))
                    if not cat_obj:
                        st.error("Informe a categoria.")
                    else:
                        _db.add(Transaction(
                            date=ent_data, type=TYPE_IN, category_id=cat_obj.id,
                            amount=float(ent_valor), description=(ent_desc or None),
                            congregation_id=cong_obj.id, payment_method=None
                        ))
                        _db.commit()
                        st.success("Entrada registrada.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # ================== FORMULÁRIO: DÍZIMISTA ==================
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Salvar DIZIMISTA")
        with st.form("form_dizimo", clear_on_submit=True):
            dz_data = st.date_input("Data do Culto", value=today_bahia(), key="dz_data", format="DD/MM/YYYY")
            dz_nome = st.text_input("Nome do dizimista", key="dz_nome")
            dz_valor = st.number_input("Valor dízimo (R$)", min_value=0.0, step=1.0, format="%.2f", key="dz_valor")
            dz_payment = st.selectbox("Forma de Pagamento", ["Dinheiro", "PIX"], key="dz_payment_method")

            if st.form_submit_button("Salvar DIZIMISTA", type="primary"):
                nome = (dz_nome or "").strip()
                if not nome:
                    st.error("Informe o nome do dizimista.")
                else:
                    with SessionLocal() as _db:
                        _db.add(Tithe(
                            date=dz_data, tither_name=nome, amount=float(dz_valor),
                            congregation_id=cong_obj.id, payment_method=dz_payment
                        ))
                        _db.commit()
                        st.success("Dízimo registrado.")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown("---")

        # ================== FORMULÁRIO: SAÍDA ==================
        st.markdown('<div class="st-container-card">', unsafe_allow_html=True)
        st.subheader("Lançar SAÍDA")
        with st.form("form_saida", clear_on_submit=True):
            sai_data = st.date_input("Data", value=today_bahia(), key="sai_data", format="DD/MM/YYYY")
            cats_out = categories_for_type(db, TYPE_OUT)
            sai_cat = st.selectbox("Tipo da saída (Categoria)", [c.name for c in cats_out] or ["—"], key="sai_cat")
            sai_desc = st.text_input("Descrição (opcional)", key="sai_desc")
            sai_valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0, format="%.2f", key="sai_valor")

            if st.form_submit_button("Salvar SAÍDA", type="primary"):
                with SessionLocal() as _db:
                    cat_obj = _db.scalar(select(Category).where(Category.name == sai_cat))
                    if not cat_obj:
                        st.error("Informe o tipo de saída.")
                    else:
                        _db.add(Transaction(
                            date=sai_data, type=TYPE_OUT, category_id=cat_obj.id,
                            amount=float(sai_valor), description=(sai_desc or None),
                            congregation_id=cong_obj.id,
                        ))
                        _db.commit()
                        st.success("Saída registrada.")
        st.markdown('</div>', unsafe_allow_html=True)
