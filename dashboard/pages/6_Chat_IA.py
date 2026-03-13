import streamlit as st
import os
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Chat IA — Olist", page_icon="🤖")
st.title("🤖 Chat com os Dados da Olist")
st.caption("Faça perguntas em linguagem natural sobre o e-commerce brasileiro")

DB_CONN = os.environ.get("OLIST_DW_CONN", "postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

if not GROQ_API_KEY:
    st.error("GROQ_API_KEY não configurada.")
    st.stop()

if "messages" not in st.session_state:
    st.session_state.messages = []

SCHEMA = (
    "Você tem acesso a um Data Warehouse PostgreSQL. Gere apenas SQL válido para PostgreSQL sem explicações.\n\n"
    "TABELA PRINCIPAL: fato_pedidos (\n"
    "  order_id, customer_id, product_id, seller_id,\n"
    "  order_status VARCHAR ('delivered', 'canceled', etc),\n"
    "  order_purchase_timestamp TIMESTAMP,\n"
    "  order_delivered_customer_date TIMESTAMP,\n"
    "  order_estimated_delivery_date TIMESTAMP,\n"
    "  purchase_year INTEGER, purchase_month INTEGER, purchase_day INTEGER,\n"
    "  delivery_days INTEGER, is_late INTEGER (1=atrasado, 0=no prazo),\n"
    "  item_price NUMERIC, item_freight NUMERIC, item_total NUMERIC,\n"
    "  payment_type VARCHAR, payment_value NUMERIC, installments INTEGER,\n"
    "  review_score INTEGER (1 a 5),\n"
    "  customer_state VARCHAR, customer_city VARCHAR\n"
    ")\n\n"
    "DIMENSOES:\n"
    "dim_vendedores (seller_id, seller_zip_code, seller_city, seller_state)\n"
    "dim_produtos (product_id, product_category_name, product_category_name_english, product_weight_g)\n"
    "dim_clientes (customer_id, customer_unique_id, customer_zip_code, customer_city, customer_state)\n\n"
    "REGRAS OBRIGATORIAS:\n"
    "- SEMPRE use ROUND(valor::numeric, 2) para arredondar\n"
    "- Para ano/mes use purchase_year e purchase_month da fato_pedidos\n"
    "- Para cidade use customer_city da fato_pedidos\n"
    "- review_score ja esta na fato_pedidos, nao precisa de JOIN extra\n"
    "- NUNCA faca JOIN com dim_tempo ou dim_localizacao\n\n"
    "EXEMPLOS CORRETOS:\n"
    "Receita por estado: SELECT customer_state, ROUND(SUM(item_total)::numeric,2) as receita FROM fato_pedidos WHERE order_status='delivered' GROUP BY customer_state ORDER BY receita DESC LIMIT 5;\n"
    "Receita por mes: SELECT purchase_year, purchase_month, ROUND(SUM(item_total)::numeric,2) as receita FROM fato_pedidos WHERE order_status='delivered' GROUP BY purchase_year, purchase_month ORDER BY purchase_year, purchase_month;\n"
    "Categoria com atraso: SELECT p.product_category_name_english, ROUND(100.0*SUM(f.is_late)/COUNT(*)::numeric,2) as taxa_atraso FROM fato_pedidos f JOIN dim_produtos p ON f.product_id=p.product_id WHERE f.order_status='delivered' GROUP BY p.product_category_name_english ORDER BY taxa_atraso DESC LIMIT 5;\n"
    "Vendedores com nota: SELECT f.seller_id, v.seller_city, ROUND(SUM(f.item_total)::numeric,2) as receita, ROUND(AVG(f.review_score)::numeric,2) as nota_media FROM fato_pedidos f JOIN dim_vendedores v ON f.seller_id=v.seller_id WHERE f.order_status='delivered' GROUP BY f.seller_id, v.seller_city ORDER BY receita DESC LIMIT 5;\n"
    "Taxa de atraso por categoria: SELECT p.product_category_name_english, COUNT(*) as total, SUM(f.is_late) as atrasados, ROUND(100.0*SUM(f.is_late)/COUNT(*)::numeric,2) as taxa_atraso FROM fato_pedidos f JOIN dim_produtos p ON f.product_id=p.product_id WHERE f.order_status='delivered' AND f.delivery_days IS NOT NULL GROUP BY p.product_category_name_english ORDER BY taxa_atraso DESC LIMIT 10;\n"
)

def run_query(sql):
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = result.fetchall()
            cols = list(result.keys())
            return cols, rows
    except Exception as e:
        return None, str(e)

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if prompt := st.chat_input("Ex: 3 cidades de SP com maior receita? Quais categorias mais atrasam?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Gerando query..."):
            try:
                from langchain_groq import ChatGroq
                from langchain_core.messages import HumanMessage, SystemMessage

                llm = ChatGroq(api_key=GROQ_API_KEY, model="llama-3.1-8b-instant")

                sql_messages = [
                    SystemMessage(content=SCHEMA),
                    HumanMessage(content=f"Gere apenas o SQL para responder: {prompt}")
                ]
                sql_response = llm.invoke(sql_messages)
                sql = sql_response.content.strip()

                if "```" in sql:
                    sql = sql.split("```")[1]
                    if sql.lower().startswith("sql"):
                        sql = sql[3:]
                sql = sql.strip()

                cols, rows = run_query(sql)

                if cols is None:
                    answer = f"Erro ao executar a query: {rows}\n\nSQL gerado:\n{sql}"
                else:
                    result_text = f"Colunas: {cols}\nDados: {[list(r) for r in rows]}"
                    interpret_messages = [
                        SystemMessage(content="Você é um analista de dados. Interprete o resultado e responda de forma clara em português usando os números reais."),
                        HumanMessage(content=f"Pergunta: {prompt}\n\nResultado:\n{result_text}")
                    ]
                    interpret_response = llm.invoke(interpret_messages)
                    answer = interpret_response.content

            except Exception as e:
                answer = f"Erro: {str(e)}"

        st.write(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
