import streamlit as st
import os
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Chat IA — Olist", page_icon="🤖")
st.title("🤖 Chat com os Dados da Olist")
st.caption("Faça perguntas sobre métricas do e-commerce ou sobre o que os clientes falam nos reviews")

DB_CONN = st.secrets.get("OLIST_DW_CONN", os.environ.get("OLIST_DW_CONN", ""))
GROQ_API_KEY = st.secrets.get("GROQ_API_KEY", os.environ.get("GROQ_API_KEY", ""))

if not GROQ_API_KEY:
    st.error("GROQ_API_KEY não configurada.")
    st.stop()

if "messages" not in st.session_state:
    st.session_state.messages = []

SCHEMA = (
    "Você é um especialista em SQL PostgreSQL. Gere APENAS SQL válido, sem explicações, sem markdown.\n\n"
    "TABELA PRINCIPAL: fato_pedidos\n"
    "  order_id, customer_id, product_id, seller_id,\n"
    "  order_status VARCHAR ('delivered', 'canceled', etc),\n"
    "  order_purchase_timestamp TIMESTAMP,\n"
    "  order_delivered_customer_date TIMESTAMP,\n"
    "  order_estimated_delivery_date TIMESTAMP,\n"
    "  purchase_year INTEGER, purchase_month INTEGER, purchase_day INTEGER,\n"
    "  delivery_days INTEGER, is_late INTEGER (1=atrasado, 0=no prazo),\n"
    "  item_price NUMERIC, item_freight NUMERIC,\n"
    "  item_total NUMERIC (use para receita e ticket médio),\n"
    "  payment_type VARCHAR, payment_value NUMERIC, installments INTEGER,\n"
    "  review_score INTEGER (1 a 5),\n"
    "  customer_state VARCHAR, customer_city VARCHAR\n\n"
    "DIMENSÕES:\n"
    "  dim_vendedores  (seller_id PK, seller_zip_code, seller_city, seller_state)\n"
    "  dim_produtos    (product_id PK, product_category_name, product_category_name_english, product_weight_g)\n"
    "  dim_clientes    (customer_id PK, customer_unique_id, customer_zip_code, customer_city, customer_state)\n\n"
    "REGRAS OBRIGATÓRIAS:\n"
    "  1. SEMPRE use ROUND((expressao)::numeric, 2) — o cast ::numeric deve envolver TODA a expressão\n"
    "  2. Para ano e mês use purchase_year e purchase_month da fato_pedidos — nunca JOIN com dim_tempo\n"
    "  3. Para cidade e estado use customer_city e customer_state da fato_pedidos — nunca JOIN com dim_localizacao\n"
    "  4. review_score já está na fato_pedidos — não precisa de JOIN extra\n"
    "  5. Para taxa de atraso: ROUND((100.0 * SUM(is_late) / COUNT(*))::numeric, 2)\n"
    "  6. Para prazo/atraso por ESTADO use customer_state — nunca agrupe por categoria\n"
    "  7. Para crescimento percentual use subquery: ROUND((100.0*(atual-anterior)/anterior)::numeric, 2)\n"
    "  8. Retorne apenas o SQL, sem nenhum texto antes ou depois\n\n"
    "EXEMPLOS:\n"
    "SELECT customer_state, ROUND(SUM(item_total)::numeric,2) AS receita, COUNT(DISTINCT order_id) AS pedidos "
    "FROM fato_pedidos WHERE order_status='delivered' GROUP BY customer_state ORDER BY receita DESC LIMIT 5;\n"
    "SELECT p.product_category_name_english, ROUND((100.0*SUM(f.is_late)/COUNT(*))::numeric,2) AS taxa_atraso "
    "FROM fato_pedidos f JOIN dim_produtos p ON f.product_id=p.product_id "
    "WHERE f.order_status='delivered' AND f.delivery_days IS NOT NULL "
    "GROUP BY p.product_category_name_english ORDER BY taxa_atraso DESC LIMIT 10;\n"
    "SELECT f.seller_id, v.seller_city, ROUND(SUM(f.item_total)::numeric,2) AS receita, "
    "ROUND(AVG(f.review_score)::numeric,2) AS nota_media "
    "FROM fato_pedidos f JOIN dim_vendedores v ON f.seller_id=v.seller_id "
    "WHERE f.order_status='delivered' GROUP BY f.seller_id, v.seller_city ORDER BY receita DESC LIMIT 5;\n"
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

def is_review_question(prompt):
    """Detecta se a pergunta é sobre opinião/sentimento dos clientes."""
    keywords = [
        "reclamam", "reclamação", "reclamações", "falam", "dizem", "comentam",
        "opinião", "opiniões", "feedback", "avaliação", "avaliações", "review",
        "reviews", "clientes dizem", "clientes falam", "satisfação", "insatisfação",
        "elogiam", "criticam", "problema", "problemas", "experiência", "sentimento"
    ]
    prompt_lower = prompt.lower()
    return any(kw in prompt_lower for kw in keywords)

def search_reviews(prompt, top_k=5):
    """Busca reviews similares no pgvector usando embedding da pergunta."""
    from fastembed import TextEmbedding
    model = TextEmbedding("BAAI/bge-small-en-v1.5")
    embedding = list(model.embed([prompt]))[0].tolist()
    embedding_str = str(embedding)

    engine = create_engine(DB_CONN)
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT review_text, review_score
            FROM reviews_embeddings
            ORDER BY embedding <-> '{embedding_str}'::vector
            LIMIT {top_k}
        """))
        return result.fetchall()

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if prompt := st.chat_input("Ex: O que os clientes reclamam? Qual cidade de SP tem maior receita?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analisando..."):
            try:
                from langchain_groq import ChatGroq
                from langchain_core.messages import HumanMessage, SystemMessage

                llm = ChatGroq(api_key=GROQ_API_KEY, model="llama-3.3-70b-versatile")

                if is_review_question(prompt):
                    # ── RAG: busca semântica nos reviews ──
                    reviews = search_reviews(prompt, top_k=8)

                    if not reviews:
                        answer = "Não encontrei reviews relevantes para essa pergunta."
                    else:
                        context = "\n".join([
                            f"[Nota {r[1]}/5] {r[0]}" for r in reviews
                        ])
                        rag_messages = [
                            SystemMessage(content=(
                                "Você é um analista de experiência do cliente brasileiro. "
                                "Com base nos reviews reais de clientes abaixo, responda a pergunta de forma clara e objetiva em português. "
                                "Cite exemplos dos reviews quando relevante. Destaque padrões e insights importantes."
                            )),
                            HumanMessage(content=f"Pergunta: {prompt}\n\nReviews relevantes:\n{context}")
                        ]
                        response = llm.invoke(rag_messages)
                        answer = response.content

                else:
                    # ── Text-to-SQL: consulta no DW ──
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
                        answer = "Desculpe, não consegui processar essa pergunta. Tente reformulá-la de outra forma."
                    else:
                        result_text = f"Colunas: {cols}\nDados: {[list(r) for r in rows]}"
                        interpret_messages = [
                            SystemMessage(content=(
                                "Você é um analista de dados brasileiro. "
                                "Interprete o resultado da query e responda de forma clara e objetiva em português. "
                                "Use os números reais retornados. Seja direto e destaque os insights mais importantes. "
                                "Os dados de 2016 cobrem apenas setembro a dezembro, então comparações com 2016 devem mencionar isso."
                            )),
                            HumanMessage(content=f"Pergunta: {prompt}\n\nResultado:\n{result_text}")
                        ]
                        interpret_response = llm.invoke(interpret_messages)
                        answer = interpret_response.content

            except Exception as e:
                answer = f"Erro: {str(e)}"

        st.write(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
