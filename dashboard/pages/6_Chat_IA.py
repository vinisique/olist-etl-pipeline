import streamlit as st
import os
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Chat IA — Olist", page_icon="🤖")
st.title("🤖 Chat com os Dados da Olist")
st.caption("Faça perguntas em linguagem natural sobre o e-commerce brasileiro")

DB_CONN = os.environ.get("OLIST_DW_CONN", "postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

if not GROQ_API_KEY:
    st.error("⚠️ GROQ_API_KEY não configurada.")
    st.stop()

if "messages" not in st.session_state:
    st.session_state.messages = []

SCHEMA = """
Você tem acesso a um Data Warehouse PostgreSQL com o seguinte schema estrela:

TABELA PRINCIPAL:
fato_pedidos (
    order_id VARCHAR,
    customer_id VARCHAR,
    product_id VARCHAR,
    seller_id VARCHAR,
    order_status VARCHAR,         -- 'delivered', 'canceled', etc
    order_purchase_timestamp TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    purchase_year INTEGER,        -- 2016, 2017, 2018
    purchase_month INTEGER,       -- 1-12
    purchase_day INTEGER,
    purchase_weekday INTEGER,
    delivery_days INTEGER,        -- dias para entregar
    is_late INTEGER,              -- 1=atrasado, 0=no prazo
    item_price NUMERIC,
    item_freight NUMERIC,
    item_total NUMERIC,           -- preço + frete
    payment_type VARCHAR,         -- 'credit_card', 'boleto', etc
    payment_value NUMERIC,
    installments INTEGER,
    review_score INTEGER,         -- 1 a 5
    customer_state VARCHAR,       -- ex: 'SP', 'RJ'
    customer_city VARCHAR         -- ex: 'sao paulo'
)

DIMENSÕES:
dim_clientes (customer_id, customer_unique_id, customer_zip_code, customer_city, customer_state)
dim_vendedores (seller_id, seller_zip_code, seller_city, seller_state)
dim_produtos (product_id, product_category_name, product_category_name_english, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
dim_localizacao (zip_code_prefix, city, state, lat, lng)  -- NÃO use para filtrar cidade/estado
dim_tempo (id_tempo, data_completa, ano, mes, dia, dia_semana, trimestre, nome_mes)

REGRAS:
- Sempre use ROUND(...::numeric, 2) para valores monetários
- Para filtrar estado SP: WHERE customer_state = 'SP'
- Para receita total: SUM(item_total)
- Para pedidos únicos: COUNT(DISTINCT order_id)
- Período: Setembro/2016 a Outubro/2018
- Retorne apenas SQL válido para PostgreSQL, sem explicações
- Para filtrar por cidade: use customer_city direto da fato_pedidos, NÃO faça JOIN com dim_localizacao
- Para cidades de SP: WHERE customer_state = 'SP' GROUP BY customer_city
"""

def run_query(sql: str):
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = result.fetchall()
            cols = result.keys()
            return list(cols), rows
    except Exception as e:
        return None, str(e)

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if prompt := st.chat_input("Ex: 3 cidades de SP com maior receita? Qual vendedor tem melhor avaliação?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Gerando query..."):
            try:
                from langchain_groq import ChatGroq
                from langchain_core.messages import HumanMessage, SystemMessage

                llm = ChatGroq(api_key=GROQ_API_KEY, model="llama-3.1-8b-instant")

                # Etapa 1: gerar SQL
                sql_messages = [
                    SystemMessage(content=SCHEMA),
                    HumanMessage(content=f"Gere apenas o SQL para responder: {prompt}")
                ]
                sql_response = llm.invoke(sql_messages)
                sql = sql_response.content.strip()

                # Limpa marcadores de código se o LLM incluir
                if "```" in sql:
                    sql = sql.split("```")[1]
                    if sql.startswith("sql"):
                        sql = sql[3:]
                sql = sql.strip()

                # Etapa 2: executar SQL
                cols, rows = run_query(sql)

                if cols is None:
                    answer = f"Erro ao executar a query: {rows}\n\nSQL gerado:\n```sql\n{sql}\n```"
                else:
                    # Etapa 3: interpretar resultado
                    result_text = f"Colunas: {list(cols)}\nDados: {[list(r) for r in rows]}"
                    interpret_messages = [
                        SystemMessage(content="Você é um analista de dados. Interprete o resultado da query SQL e responda a pergunta do usuário de forma clara em português. Use os números reais retornados."),
                        HumanMessage(content=f"Pergunta: {prompt}\n\nResultado da query:\n{result_text}")
                    ]
                    interpret_response = llm.invoke(interpret_messages)
                    answer = interpret_response.content

            except Exception as e:
                answer = f"Erro: {str(e)}"

        st.write(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
