def _apply_tx_changes(orig_df: pd.DataFrame, edited_df: pd.DataFrame, tx_type: str, default_cong_id: Optional[int], default_sub_cong_id: Optional[int] = None):
    def norm_df(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        if "Valor" in d.columns: d["Valor"] = d["Valor"].map(_to_float_brl)
        if "Data" in d.columns: d["Data"] = d["Data"].map(_to_date)
        for c in ("Categoria", "Descrição"):
            if c in d.columns: d[c] = d[c].astype(str).fillna("")
        return d

    o = norm_df(orig_df)
    n = norm_df(edited_df)

    old_ids = set(int(x) for x in o["ID"].tolist() if pd.notna(x))
    new_ids = set(int(x) for x in n["ID"].tolist() if pd.notna(x) and int(x) > 0)
    to_delete = list(old_ids - new_ids)
    old_map = {int(r["ID"]): r for _, r in o.iterrows() if pd.notna(r["ID"])}

    with SessionLocal() as db:
        cats = categories_for_type(db, tx_type)
        cat_by_name = {c.name: c for c in cats}
        if to_delete:
            db.query(Transaction).filter(Transaction.id.in_(to_delete)).delete(synchronize_session=False)

        for rid in sorted(new_ids & old_ids):
            old = old_map[rid]
            new = n.loc[n["ID"] == rid].iloc[0]
            t = db.get(Transaction, rid)
            if not t: continue
            
            new_amount = _to_float_brl(new.get("Valor"))
            new_cat_name = str(new.get("Categoria", "")).strip()
            if abs(new_amount) < 0.01 or not new_cat_name:
                st.warning(f"Alteração no ID {rid} ignorada: valor ou categoria inválidos.")
                continue

            changed = False
            if t.date != _to_date(new["Data"]): t.date = _to_date(new["Data"]); changed = True
            cat = cat_by_name.get(new_cat_name)
            if cat and t.category_id != cat.id: t.category_id = cat.id; changed = True
            if t.amount != new_amount: t.amount = new_amount; changed = True
            if (t.description or "") != (new.get("Descrição", "") or ""): t.description = (new.get("Descrição", "") or None); changed = True
            if "_cong_id" in n.columns:
                new_cid = int(new.get("_cong_id", 0) or 0)
                if new_cid and new_cid != t.congregation_id:
                    t.congregation_id = new_cid; changed = True
            if changed: db.add(t)

        for _, row in n.iterrows():
            rid = row.get("ID", None)
            is_new = pd.isna(rid) or int(rid) <= 0 or int(rid) not in old_ids
            if not is_new: continue
            
            amount = _to_float_brl(row.get("Valor"))
            cat_name = str(row.get("Categoria", "")).strip()
            if not cat_name or abs(amount) < 0.01: continue

            cat = cat_by_name.get(cat_name)
            if not cat: continue
            
            # --- LÓGICA DE BUSCA DE ID CORRIGIDA ---
            # Prioriza o ID da linha (para o editor de missões)
            cong_id = int(row.get("_cong_id", 0) or 0)
            # Se não houver na linha, usa o padrão (para outros editores)
            if not cong_id:
                cong_id = default_cong_id
            
            # Se ainda assim não houver um ID válido, pula a linha.
            if not cong_id:
                continue

            db.add(Transaction(
                date=_to_date(row.get("Data")), type=tx_type, category_id=cat.id, 
                amount=amount, description=(str(row.get("Descrição", "")).strip() or None),
                congregation_id=cong_id,
                sub_congregation_id=default_sub_cong_id
            ))
        db.commit()
