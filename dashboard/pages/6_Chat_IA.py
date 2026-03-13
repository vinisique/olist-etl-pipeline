import streamlit as st
import os
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Chat IA — Olist", page_icon="🤖")
st.title("🤖 Chat com os Dados da Olist")
st.caption("Faça perguntas em linguagem natural sobre o e-commerce brasileiro")

DB_CONN = os.environ.get("OLIST_DW_CONN", "postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

if not GROQ_API_KEY:
    st.error("⚠️ GROQ_API_KEY não configurada. Adicione no .env e reinicie os containers.")
    st.stop()

if "messages" not in st.session_state:
    st.session_state.messages = []

@st.cache_data
def get_data_context():
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:
            receita = conn.execute(text("SELECT SUM(item_total) FROM fato_pedidos WHERE order_status = 'delivered'")).fetchone()[0]
            pedidos = conn.execute(text("SELECT COUNT(DISTINCT order_id) FROM fato_pedidos")).fetchone()[0]
            nota = conn.execute(text("SELECT ROUND(AVG(review_score)::numeric, 2) FROM fato_pedidos WHERE review_score IS NOT NULL")).fetchone()[0]
            top_estado = conn.execute(text("SELECT customer_state, COUNT(*) as n FROM fato_pedidos GROUP BY customer_state ORDER BY n DESC LIMIT 1")).fetchone()
            top_categoria = conn.execute(text("""
                SELECT p.product_category_name_english, ROUND(SUM(f.item_total)::numeric, 2) as receita
                FROM fato_pedidos f
                JOIN dim_produtos p ON f.product_id = p.product_id
                WHERE p.product_category_name_english != 'unknown'
                GROUP BY p.product_category_name_english
                ORDER BY receita DESC LIMIT 1
            """)).fetchone()
        return {
            "receita_total": f"R$ {receita:,.2f}" if receita else "N/A",
            "total_pedidos": f"{pedidos:,}" if pedidos else "N/A",
            "nota_media": str(nota) if nota else "N/A",
            "top_estado": top_estado[0] if top_estado else "N/A",
            "top_categoria": top_categoria[0] if top_categoria else "N/A",
        }
    except Exception as e:
        return {"erro": str(e)}

context = get_data_context()

SYSTEM_PROMPT = f"""Você é um analista de dados especialista no dataset de e-commerce brasileiro da Olist.
Responda perguntas sobre os dados de forma clara e objetiva em português.

Contexto atual dos dados:
- Receita total: {context.get('receita_total')}
- Total de pedidos: {context.get('total_pedidos')}
- Nota média dos clientes: {context.get('nota_media')}
- Estado com mais pedidos: {context.get('top_estado')}
- Categoria mais lucrativa: {context.get('top_categoria')}

Período dos dados: Setembro/2016 a Outubro/2018
Seja direto, use números quando relevante e sugira insights úteis."""

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if prompt := st.chat_input("Ex: Qual o estado com maior receita? Qual categoria vende mais?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analisando..."):
            try:
                from langchain_groq import ChatGroq
                from langchain_core.messages import HumanMessage, SystemMessage

                llm = ChatGroq(api_key=GROQ_API_KEY, model="llama-3.1-8b-instant")
                messages = [SystemMessage(content=SYSTEM_PROMPT)]
                for m in st.session_state.messages[:-1]:
                    if m["role"] == "user":
                        messages.append(HumanMessage(content=m["content"]))
                messages.append(HumanMessage(content=prompt))

                response = llm.invoke(messages)
                answer = response.content
            except Exception as e:
                answer = f"Erro ao conectar com o LLM: {str(e)}"

        st.write(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
