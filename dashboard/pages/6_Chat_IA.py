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

@st.cache_data
def get_data_context():
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:

            receita = conn.execute(text("""
                SELECT ROUND(SUM(item_total)::numeric, 2)
                FROM fato_pedidos WHERE order_status = 'delivered'
            """)).fetchone()[0]

            pedidos = conn.execute(text(
                "SELECT COUNT(DISTINCT order_id) FROM fato_pedidos"
            )).fetchone()[0]

            nota = conn.execute(text("""
                SELECT ROUND(AVG(review_score)::numeric, 2)
                FROM fato_pedidos WHERE review_score IS NOT NULL
            """)).fetchone()[0]

            receita_estado = conn.execute(text("""
                SELECT customer_state,
                       ROUND(SUM(item_total)::numeric, 2) as receita,
                       COUNT(DISTINCT order_id) as pedidos
                FROM fato_pedidos
                WHERE order_status = 'delivered'
                GROUP BY customer_state
                ORDER BY receita DESC
                LIMIT 5
            """)).fetchall()

            receita_ano = conn.execute(text("""
                SELECT purchase_year,
                       COUNT(DISTINCT order_id) as pedidos,
                       ROUND(SUM(item_total)::numeric, 2) as receita
                FROM fato_pedidos
                WHERE order_status = 'delivered'
                GROUP BY purchase_year
                ORDER BY purchase_year
            """)).fetchall()

            top_categorias = conn.execute(text("""
                SELECT p.product_category_name_english,
                       ROUND(SUM(f.item_total)::numeric, 2) as receita,
                       COUNT(DISTINCT f.order_id) as pedidos
                FROM fato_pedidos f
                JOIN dim_produtos p ON f.product_id = p.product_id
                WHERE p.product_category_name_english != 'unknown'
                  AND f.order_status = 'delivered'
                GROUP BY p.product_category_name_english
                ORDER BY receita DESC
                LIMIT 5
            """)).fetchall()

            entrega = conn.execute(text("""
                SELECT ROUND(AVG(delivery_days)::numeric, 1) as prazo_medio,
                       ROUND(100.0 * SUM(is_late) / COUNT(*), 1) as pct_atraso
                FROM fato_pedidos
                WHERE order_status = 'delivered'
                  AND delivery_days IS NOT NULL
            """)).fetchone()

            pagamento = conn.execute(text("""
                SELECT payment_type,
                       COUNT(*) as qtd,
                       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
                FROM fato_pedidos
                WHERE payment_type IS NOT NULL
                GROUP BY payment_type
                ORDER BY qtd DESC
            """)).fetchall()

        return {
            "receita_total": f"R$ {receita:,.2f}" if receita else "N/A",
            "total_pedidos": f"{pedidos:,}" if pedidos else "N/A",
            "nota_media": str(nota) if nota else "N/A",
            "prazo_medio_entrega": f"{entrega[0]} dias" if entrega else "N/A",
            "taxa_atraso": f"{entrega[1]}%" if entrega else "N/A",
            "receita_por_estado": [{"estado": r[0], "receita": f"R$ {r[1]:,.2f}", "pedidos": r[2]} for r in receita_estado],
            "receita_por_ano": [{"ano": r[0], "pedidos": r[1], "receita": f"R$ {r[2]:,.2f}"} for r in receita_ano],
            "top_categorias": [{"categoria": r[0], "receita": f"R$ {r[1]:,.2f}", "pedidos": r[2]} for r in top_categorias],
            "formas_pagamento": [{"tipo": r[0], "percentual": f"{r[2]}%"} for r in pagamento],
        }
    except Exception as e:
        return {"erro": str(e)}

context = get_data_context()

SYSTEM_PROMPT = f"""Você é um analista de dados especialista no dataset de e-commerce brasileiro da Olist.
Responda perguntas sobre os dados de forma clara e objetiva em português.
Use os dados abaixo para responder com precisão. Seja direto e use números reais.

DADOS DO DATASET (Setembro/2016 a Outubro/2018):

Métricas gerais:
- Receita total: {context.get('receita_total')}
- Total de pedidos: {context.get('total_pedidos')}
- Nota média dos clientes: {context.get('nota_media')}
- Prazo médio de entrega: {context.get('prazo_medio_entrega')}
- Taxa de atraso: {context.get('taxa_atraso')}

Top 5 estados por receita:
{context.get('receita_por_estado')}

Receita por ano:
{context.get('receita_por_ano')}

Top 5 categorias por receita:
{context.get('top_categorias')}

Formas de pagamento:
{context.get('formas_pagamento')}

Responda sempre com base nesses dados reais. Se não souber algo, diga que não há dados disponíveis."""

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if prompt := st.chat_input("Ex: Qual o estado com maior receita? Como foi 2017?"):
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
                answer = f"Erro: {str(e)}"

        st.write(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
