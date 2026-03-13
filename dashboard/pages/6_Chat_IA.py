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
    "Você é um especialista em SQL PostgreSQL. Gere APENAS SQL válido, sem explicações, sem markdown.\n\n"

    "TABELA PRINCIPAL: fato_pedidos\n"
    "  order_id          VARCHAR  -- identificador único do pedido\n"
    "  customer_id       VARCHAR  -- chave para dim_clientes\n"
    "  product_id        VARCHAR  -- chave para dim_produtos\n"
    "  seller_id         VARCHAR  -- chave para dim_vendedores\n"
    "  order_status      VARCHAR  -- use 'delivered' para pedidos entregues\n"
    "  order_purchase_timestamp TIMESTAMP\n"
    "  order_delivered_customer_date TIMESTAMP\n"
    "  order_estimated_delivery_date TIMESTAMP\n"
    "  purchase_year     INTEGER  -- 2016, 2017 ou 2018\n"
    "  purchase_month    INTEGER  -- 1 a 12\n"
    "  purchase_day      INTEGER\n"
    "  delivery_days     INTEGER  -- dias entre compra e entrega\n"
    "  is_late           INTEGER  -- 1=atrasado, 0=no prazo\n"
    "  item_price        NUMERIC  -- preço do item sem frete\n"
    "  item_freight      NUMERIC  -- valor do frete\n"
    "  item_total        NUMERIC  -- item_price + item_freight, use para receita e ticket médio\n"
    "  payment_type      VARCHAR  -- 'credit_card', 'boleto', 'voucher', 'debit_card'\n"
    "  payment_value     NUMERIC  -- valor total pago (pode incluir parcelamento)\n"
    "  installments      INTEGER  -- número de parcelas\n"
    "  review_score      INTEGER  -- avaliação do cliente de 1 a 5\n"
    "  customer_state    VARCHAR  -- ex: 'SP', 'RJ', 'MG'\n"
    "  customer_city     VARCHAR  -- ex: 'sao paulo', 'campinas'\n\n"

    "DIMENSÕES (use JOIN quando precisar de atributos extras):\n"
    "  dim_vendedores  (seller_id PK, seller_zip_code, seller_city, seller_state)\n"
    "  dim_produtos    (product_id PK, product_category_name, product_category_name_english, product_weight_g, product_length_cm, product_height_cm, product_width_cm)\n"
    "  dim_clientes    (customer_id PK, customer_unique_id, customer_zip_code, customer_city, customer_state)\n\n"

    "REGRAS OBRIGATÓRIAS:\n"
    "  1. SEMPRE use ROUND(valor::numeric, 2) para arredondar — nunca ROUND(double, int)\n"
    "  2. Para ano e mês use purchase_year e purchase_month da fato_pedidos — nunca JOIN com dim_tempo\n"
    "  3. Para cidade e estado use customer_city e customer_state da fato_pedidos — nunca JOIN com dim_localizacao\n"
    "  4. review_score já está na fato_pedidos — não precisa de JOIN extra\n"
    "  5. Para receita e ticket médio use item_total\n"
    "  6. Para taxa de atraso: ROUND(100.0 * SUM(is_late) / COUNT(*)::numeric, 2)\n"
    "  7. Retorne apenas o SQL, sem nenhum texto antes ou depois\n\n"
    "  8. Para prazo médio e taxa de atraso POR ESTADO use customer_state da fato_pedidos — nunca agrupe por categoria\n"
    "  9. Para crescimento percentual entre anos use subquery: ROUND(100.0*(ano_atual - ano_anterior)/ano_anterior::numeric, 2)\n"
    "  10. Para crescimento percentual de pedidos por ano: use COUNT(DISTINCT order_id) agrupado por purchase_year — os dados de 2016 começam em setembro, então o crescimento 2016-2017 é naturalmente alto e deve ser mencionado na interpretação\n"

    "EXEMPLOS:\n"
    "-- Receita por estado:\n"
    "SELECT customer_state, ROUND(SUM(item_total)::numeric,2) AS receita, COUNT(DISTINCT order_id) AS pedidos "
    "FROM fato_pedidos WHERE order_status='delivered' GROUP BY customer_state ORDER BY receita DESC LIMIT 5;\n\n"
    "-- Taxa de atraso por categoria:\n"
    "SELECT p.product_category_name_english, "
    "ROUND(100.0*SUM(f.is_late)/COUNT(*)::numeric,2) AS taxa_atraso, COUNT(*) AS total "
    "FROM fato_pedidos f JOIN dim_produtos p ON f.product_id=p.product_id "
    "WHERE f.order_status='delivered' AND f.delivery_days IS NOT NULL "
    "GROUP BY p.product_category_name_english ORDER BY taxa_atraso DESC LIMIT 10;\n\n"
    "-- Vendedor com nota média:\n"
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

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if prompt := st.chat_input("Ex: Ticket médio por pagamento em 2017? Crescimento de pedidos por ano?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.write(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analisando..."):
            try:
                from langchain_groq import ChatGroq
                from langchain_core.messages import HumanMessage, SystemMessage

                llm = ChatGroq(api_key=GROQ_API_KEY, model="llama-3.3-70b-versatile")

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
                    answer = f"Erro ao executar a query:\n{rows}\n\nSQL gerado:\n{sql}"
                else:
                    result_text = f"Colunas: {cols}\nDados: {[list(r) for r in rows]}"
                    interpret_messages = [
                        SystemMessage(content=(
                            "Você é um analista de dados brasileiro. "
                            "Interprete o resultado da query e responda de forma clara e objetiva em português. "
                            "Use os números reais retornados. Seja direto e destaque os insights mais importantes. "
                            "IMPORTANTE: os dados de 2016 cobrem apenas setembro a outubro de 2018, "
                            "então comparações envolvendo 2016 devem mencionar que o ano está incompleto."
                        )),
                        HumanMessage(content=f"Pergunta: {prompt}\n\nResultado:\n{result_text}")
                    ]
                    interpret_response = llm.invoke(interpret_messages)
                    answer = interpret_response.content

            except Exception as e:
                answer = f"Erro: {str(e)}"

        st.write(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
