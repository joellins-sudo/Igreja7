def build_full_statement_pdf(parent_cong_id: int, ref: date, db: Session) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=1.5*cm, rightMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    start, end = month_bounds(ref)

    # Estilos
    title_style = ParagraphStyle('title', parent=styles['h1'], alignment=TA_CENTER, fontSize=16, spaceAfter=4)
    subtitle_style = ParagraphStyle('subtitle', parent=styles['Normal'], alignment=TA_CENTER, fontSize=11, spaceAfter=12)
    heading_style = ParagraphStyle('heading', parent=styles['h2'], fontSize=12, spaceBefore=12, spaceAfter=6, fontName="Helvetica-Bold")
    normal_style = styles['Normal']
    signature_style = ParagraphStyle('signature', parent=styles['Normal'], alignment=TA_CENTER, spaceBefore=0)
    
    story: List = []

    # Coleta de dados
    parent_cong_obj = db.get(Congregation, parent_cong_id)
    sub_congs = db.scalars(select(SubCongregation).where(SubCongregation.congregation_id == parent_cong_obj.id).order_by(SubCongregation.name)).all()
    
    doc_title = f"{parent_cong_obj.name} e suas unidades" if sub_congs else parent_cong_obj.name
    all_units = [(f"{parent_cong_obj.name} (Principal)", None)] + [(s.name, s.id) for s in sub_congs] if sub_congs else [(parent_cong_obj.name, None)]

    grand_total_entradas = 0.0
    grand_total_saidas = 0.0

    # Cabeçalho do Documento
    story.append(Paragraph("Prestação de Contas Mensal", title_style))
    story.append(Paragraph(f"Congregação: {doc_title}", subtitle_style))
    story.append(Paragraph(f"Referente a: {ref.strftime('%B de %Y')}", subtitle_style))

    # Loop para gerar seções para cada unidade
    for name, sub_id in all_units:
        story.append(Spacer(1, 1*cm))
        story.append(Paragraph(f"Detalhes da Unidade: {name}", heading_style))
        
        data = _collect_month_data(db, parent_cong_obj.id, start, end, sub_cong_id=sub_id)
        unit_total_entradas = data["totals"]["entradas_total_sem_missoes"]
        unit_total_saidas = data["totals"]["saidas_total"]
        grand_total_entradas += unit_total_entradas
        grand_total_saidas += unit_total_saidas

        # Tabela de Entradas da Unidade
        story.append(Paragraph("<b>1. Entradas</b>", normal_style))
        df_entradas = _entrada_summary_df(db, parent_cong_obj.id, start, end, sub_cong_id=sub_id)
        if not df_entradas.empty:
            data_in = [["Data do Culto", "Dízimo", "Oferta", "Total"]]
            for _, row in df_entradas.iterrows():
                data_in.append([
                    row["Data do Culto"].strftime("%d/%m/%Y"),
                    format_currency(row["Dízimo"]),
                    format_currency(row["Oferta"]),
                    format_currency(row["Total"])
                ])
            # --- CORREÇÃO APLICADA AQUI ---
            total_entradas_paragraph = Paragraph(f"<b>{format_currency(unit_total_entradas)}</b>", styles['Normal'])
            data_in.append(["", "", Paragraph("<b>Total da Unidade:</b>", styles['Normal']), total_entradas_paragraph])
            
            tbl_in = Table(data_in, colWidths=[3.2*cm, 4.0*cm, 4.0*cm, 5.3*cm])
            tbl_in.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 1, colors.grey), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                ('ALIGN', (1,1), (-1,-1), 'RIGHT'), ('SPAN', (0,-1), (2,-1)), ('ALIGN', (0,-1), (2,-1), 'RIGHT'),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,-1), (-1,-1), colors.lightyellow)
            ]))
            story.append(tbl_in)
        else:
            story.append(Paragraph("Nenhuma entrada registrada.", normal_style))
        story.append(Spacer(1, 0.5*cm))

        # Tabela de Saídas da Unidade
        story.append(Paragraph("<b>2. Saídas</b>", normal_style))
        txs_out = data["tx_out"]
        if txs_out:
            data_out = [["Data", "Categoria", "Descrição", "Valor"]]
            for t in txs_out:
                data_out.append([t.date.strftime("%d/%m/%Y"), t.category.name, t.description or "", format_currency(t.amount)])
            # --- CORREÇÃO APLICADA AQUI ---
            total_saidas_paragraph = Paragraph(f"<b>{format_currency(unit_total_saidas)}</b>", normal_style)
            data_out.append(["", "", Paragraph("<b>Total da Unidade:</b>", styles['Normal']), total_saidas_paragraph])
            
            tbl_out = Table(data_out, colWidths=[2.5*cm, 4.5*cm, 6.5*cm, 3*cm])
            tbl_out.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 1, colors.grey), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                ('ALIGN', (3,1), (3,-1), 'RIGHT'), ('SPAN', (0,-1), (2,-1)), ('ALIGN', (0,-1), (2,-1), 'RIGHT'),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BACKGROUND', (0,-1), (-1,-1), colors.lightyellow)
            ]))
            story.append(tbl_out)
        else:
            story.append(Paragraph("Nenhuma saída registrada.", normal_style))
        story.append(Spacer(1, 0.5*cm))
        
        if sub_congs:
            # ... (código do resumo da unidade) ...

    # ... (código do resumo financeiro geral e assinaturas) ...

    doc.build(story)
    return buf.getvalue()
