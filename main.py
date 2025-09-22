import streamlit as st
import pandas as pd
import datetime

# CSS para botões coloridos
st.markdown("""
    <style>
    .stButton > button {
        font-size: 18px;
        height: 3em;
        width: 100%;
    }
    /* Botão verde para entradas */
    #entrada_form button {
        background-color: #4CAF50 !important;
        color: white !important;
    }
    /* Botão azul para dízimos */
    #dizimo_form button {
        background-color: #2196F3 !important;
        color: white !important;
    }
    /* Botão vermelho para saídas */
    #saida_form button {
        background-color: #f44336 !important;
        color: white !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("Controle Financeiro Igreja7")

tipo = st.sidebar.selectbox("Escolha o tipo de lançamento", ["ENTRADA", "DIZIMISTA", "SAÍDA"])

if tipo == "ENTRADA":
    with st.form("entrada_form"):
        valor = st.number_input("Valor da entrada", min_value=0.0, step=0.01)
        descricao = st.text_input("Descrição")
        data = st.date_input("Data", datetime.date.today())
        submitted = st.form_submit_button("Salvar Entrada")
        if submitted:
            st.success("Entrada salva com sucesso!")

elif tipo == "DIZIMISTA":
    with st.form("dizimo_form"):
        nome = st.text_input("Nome do dizimista")
        valor = st.number_input("Valor do dízimo", min_value=0.0, step=0.01)
        data = st.date_input("Data", datetime.date.today())
        submitted = st.form_submit_button("Salvar Dízimo")
        if submitted:
            st.success("Dízimo salvo com sucesso!")

else:
    with st.form("saida_form"):
        valor = st.number_input("Valor da saída", min_value=0.0, step=0.01)
        descricao = st.text_input("Descrição")
        data = st.date_input("Data", datetime.date.today())
        submitted = st.form_submit_button("Salvar Saída")
        if submitted:
            st.success("Saída salva com sucesso!")
