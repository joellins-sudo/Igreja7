def sidebar_common(user: "User") -> str:
    # Se a sidebar (menu) já foi renderizada nesta execução, só retorna a página atual
    if st.session_state.get("sidebar_rendered", False):
        return st.session_state.get("main_menu_page", "Visão Geral")

    with st.sidebar:
        if os.path.exists(LOGO_PATH):
            st.image(LOGO_PATH, use_column_width=True)
        st.write(f"👤 **{user.username}** — *{user.role}*")

        MENU_ICONS = {
            "Lançamentos": "📥",
            "Relatório de Entrada": "📊",
            "Relatório de Saída": "📉",
            "Relatório de Missões": "🌍",
            "Relatório de Dizimistas": "🧾",
            "Visão Geral": "🏁",
            "Cadastro": "🛠️",
        }

        if user.role == "SEDE":
            menu_options_plain = [
                "Lançamentos", "Relatório de Entrada", "Relatório de Saída",
                "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral", "Cadastro"
            ]
        elif user.role == "TESOUREIRO":
            menu_options_plain = [
                "Lançamentos", "Relatório de Entrada", "Relatório de Saída",
                "Relatório de Missões", "Relatório de Dizimistas", "Visão Geral"
            ]
        elif user.role == "TESOUREIRO MISSIONÁRIO":
            menu_options_plain = ["Relatório de Missões"]
        else:
            menu_options_plain = ["Visão Geral"]

        menu_labels_pretty = [f"{MENU_ICONS.get(opt, '•')} {opt}" for opt in menu_options_plain]

        _prev_page = st.session_state.get("main_menu_page")
        _default_index = menu_options_plain.index(_prev_page) if _prev_page in menu_options_plain else 0

        sel_label = st.radio(
            "Menu",
            options=menu_labels_pretty,
            index=_default_index,
            key=f"main_menu_nav_{getattr(user, 'id', 'anon')}",
            label_visibility="collapsed",
        )

        sel_index = menu_labels_pretty.index(sel_label)
        page = menu_options_plain[sel_index]
        st.session_state["main_menu_page"] = page

        st.divider()
        if st.button("Sair", key=f"btn_logout_{getattr(user, 'id', 'anon')}"):
            logout()

        # Marca que a sidebar foi renderizada para evitar duplicações em chamadas futuras
        st.session_state["sidebar_rendered"] = True

        return page
