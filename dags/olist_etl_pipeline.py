"""
DAG: olist_etl_pipeline
Pipeline ETL End-to-End — Olist E-commerce Dataset

Orquestra as 4 etapas do pipeline:
  1. ingestao       → Baixa dataset do Kaggle e salva em Parquet no volume local (camada RAW)
  2. processamento  → PySpark: RAW → TRUSTED (limpeza, tipagem, joins)
  3. carga          → TRUSTED → PostgreSQL DW (esquema estrela)
  4. analise        → Queries analíticas e exportação dos CSVs para o dashboard
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

log = logging.getLogger(__name__)

default_args = {
    "owner": "vinicius.siqueira",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Caminhos do Data Lake no volume compartilhado entre containers
LAKE_BASE   = "/opt/airflow/scripts/data_lake"
RAW_PATH    = f"{LAKE_BASE}/raw"
TRUSTED_PATH = f"{LAKE_BASE}/trusted"
EXPORT_PATH = "/opt/airflow/scripts/exports"


# ─────────────────────────────────────────
# Task 1 — Ingestão
# ─────────────────────────────────────────

def task_ingestao(**context):
    """
    Baixa os CSVs do Kaggle, converte para Parquet
    e salva na camada RAW do Data Lake (volume local).
    """
    import os
    import pandas as pd
    import kagglehub
    from datetime import datetime as dt

    log.info("🚀 Iniciando ingestão — Olist Dataset")

    os.makedirs(RAW_PATH, exist_ok=True)

    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
    log.info(f"✅ Dataset baixado em: {path}")

    TABLES = {
        "olist_customers_dataset.csv":           "customers",
        "olist_sellers_dataset.csv":             "sellers",
        "olist_orders_dataset.csv":              "orders",
        "olist_order_items_dataset.csv":         "order_items",
        "olist_order_payments_dataset.csv":      "order_payments",
        "olist_order_reviews_dataset.csv":       "order_reviews",
        "olist_products_dataset.csv":            "products",
        "olist_geolocation_dataset.csv":         "geolocation",
        "product_category_name_translation.csv": "category_translation",
    }

    results = []
    total_rows = 0

    for csv_file, table_name in TABLES.items():
        df = pd.read_csv(f"{path}/{csv_file}")
        df["_ingested_at"] = dt.now().isoformat()
        df["_source_file"] = csv_file

        table_path = f"{RAW_PATH}/{table_name}"
        os.makedirs(table_path, exist_ok=True)
        output = f"{table_path}/data.parquet"
        df.to_parquet(output, index=False)

        total_rows += len(df)
        results.append({"table": table_name, "rows": len(df), "path": output})
        log.info(f"   ✅ {table_name:<25} {len(df):>10,} linhas → {output}")

    log.info(f"✅ Ingestão concluída — {len(results)} tabelas · {total_rows:,} linhas")
    return {"tables": results, "total_rows": total_rows}


# ─────────────────────────────────────────
# Task 2 — Processamento com PySpark
# ─────────────────────────────────────────

def task_processamento(**context):
    """
    Lê Parquets da camada RAW (volume local),
    aplica limpeza e tipagem com PySpark,
    e salva na camada TRUSTED.
    """
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType

    log.info("🚀 Iniciando processamento PySpark")

    spark = (
        SparkSession.builder
        .appName("olist-etl-processing")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    os.makedirs(TRUSTED_PATH, exist_ok=True)

    # ── Leitura da camada RAW ──────────────────────────────────────
    orders    = spark.read.parquet(f"{RAW_PATH}/orders")
    items     = spark.read.parquet(f"{RAW_PATH}/order_items")
    payments  = spark.read.parquet(f"{RAW_PATH}/order_payments")
    reviews   = spark.read.parquet(f"{RAW_PATH}/order_reviews")
    customers = spark.read.parquet(f"{RAW_PATH}/customers")
    sellers   = spark.read.parquet(f"{RAW_PATH}/sellers")
    products  = spark.read.parquet(f"{RAW_PATH}/products")
    category  = spark.read.parquet(f"{RAW_PATH}/category_translation")

    # ── Processar orders ───────────────────────────────────────────
    date_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    orders_clean = orders
    for col in date_cols:
        orders_clean = orders_clean.withColumn(col, F.to_timestamp(F.col(col)))

    orders_clean = (
        orders_clean
        .filter(F.col("order_status").isNotNull())
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .withColumn("purchase_year",    F.year("order_purchase_timestamp"))
        .withColumn("purchase_month",   F.month("order_purchase_timestamp"))
        .withColumn("purchase_day",     F.dayofmonth("order_purchase_timestamp"))
        .withColumn("purchase_weekday", F.dayofweek("order_purchase_timestamp"))
        .withColumn("delivery_days",
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .withColumn("is_late",
            F.when(
                F.col("order_delivered_customer_date") >
                F.col("order_estimated_delivery_date"), 1
            ).otherwise(0))
        .drop("_ingested_at", "_source_file")
    )

    # ── Processar itens ────────────────────────────────────────────
    items_clean = (
        items
        .withColumn("price",         F.col("price").cast(DoubleType()))
        .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
        .withColumn("item_total",    F.col("price") + F.col("freight_value"))
        .filter(F.col("price") > 0)
        .drop("_ingested_at", "_source_file")
    )

    # ── Agregar pagamentos por pedido ──────────────────────────────
    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment"),
            F.max("payment_installments").alias("max_installments"),
            F.first("payment_type").alias("payment_type"),
        )
    )

    # ── Agregar reviews por pedido ─────────────────────────────────
    reviews_agg = (
        reviews
        .withColumn("review_score", F.col("review_score").cast(IntegerType()))
        .filter(F.col("review_score").isNotNull())
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("review_score"))
    )

    # ── Produtos com categoria em inglês ───────────────────────────
    products_clean = (
        products
        .join(category.select("product_category_name", "product_category_name_english"),
              "product_category_name", "left")
        .fillna("unknown", subset=["product_category_name_english"])
        .drop("_ingested_at", "_source_file")
    )

    # ── Agregar itens por pedido para o fato ───────────────────────
    items_agg = (
        items_clean
        .groupBy("order_id")
        .agg(
            F.sum("item_total").alias("item_total"),
            F.sum("price").alias("item_price"),
            F.sum("freight_value").alias("freight_value"),
            F.first("seller_id").alias("seller_id"),
            F.first("product_id").alias("product_id"),
        )
    )

    # ── Montar tabela fato ─────────────────────────────────────────
    fato = (
        orders_clean
        .join(items_agg,    "order_id", "left")
        .join(payments_agg, "order_id", "left")
        .join(reviews_agg,  "order_id", "left")
        .join(
            customers.select("customer_id", "customer_state", "customer_city")
                     .drop("_ingested_at", "_source_file"),
            "customer_id", "left"
        )
    )

    # ── Salvar camada TRUSTED ──────────────────────────────────────
    trusted_tables = {
        "fato_pedidos": fato,
        "dim_products":  products_clean,
        "dim_customers": customers.drop("_ingested_at", "_source_file"),
        "dim_sellers":   sellers.drop("_ingested_at", "_source_file"),
    }

    for name, df in trusted_tables.items():
        df.write.mode("overwrite").parquet(f"{TRUSTED_PATH}/{name}")
        log.info(f"   ✅ TRUSTED/{name:<20} {df.count():>10,} linhas")

    count = fato.count()
    log.info(f"✅ Processamento concluído — fato_pedidos: {count:,} linhas")
    spark.stop()
    return {"fato_rows": count}


# ─────────────────────────────────────────
# Task 3 — Carga no Data Warehouse
# ─────────────────────────────────────────

def task_carga(**context):
    """
    Lê camada TRUSTED (volume local) e carrega
    no PostgreSQL DW com esquema estrela.
    """
    import os
    import pandas as pd
    from sqlalchemy import create_engine

    log.info("🚀 Iniciando carga no Data Warehouse")

    conn_str = os.environ.get(
        "OLIST_DW_CONN",
        "postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw"
    )
    engine = create_engine(conn_str)

    # Mapeamento: tabela DW → pasta no TRUSTED
    # O PySpark salva em múltiplos arquivos part-*.parquet por pasta
    tables = {
        "fato_pedidos":  f"{TRUSTED_PATH}/fato_pedidos",
        "dim_produtos":  f"{TRUSTED_PATH}/dim_products",
        "dim_clientes":  f"{TRUSTED_PATH}/dim_customers",
        "dim_vendedores": f"{TRUSTED_PATH}/dim_sellers",
    }

    for table_name, parquet_dir in tables.items():
        # Lê todos os arquivos part-*.parquet da pasta
        part_files = [
            os.path.join(parquet_dir, f)
            for f in os.listdir(parquet_dir)
            if f.endswith(".parquet")
        ]
        if not part_files:
            log.warning(f"⚠️  Nenhum parquet encontrado em {parquet_dir}")
            continue

        frames = [pd.read_parquet(f) for f in part_files]
        df = pd.concat(frames, ignore_index=True)

        df.to_sql(table_name, engine, if_exists="replace", index=False, chunksize=5000)
        log.info(f"   ✅ {table_name:<20} {len(df):>10,} linhas carregadas")

    log.info("✅ Carga concluída")


# ─────────────────────────────────────────
# Task 4 — Análises e exportação
# ─────────────────────────────────────────

def task_analise(**context):
    """
    Queries analíticas no DW PostgreSQL
    e exportação dos CSVs para o dashboard Streamlit.
    """
    import os
    import pandas as pd
    from sqlalchemy import create_engine, text

    log.info("🚀 Iniciando análises")

    conn_str = os.environ.get(
        "OLIST_DW_CONN",
        "postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw"
    )
    engine = create_engine(conn_str)
    os.makedirs(EXPORT_PATH, exist_ok=True)

    queries = {
        "receita_mensal": """
            SELECT
                purchase_year  AS ano,
                purchase_month AS mes,
                COUNT(DISTINCT order_id)                        AS pedidos,
                ROUND(CAST(SUM(item_total)  AS numeric), 2)    AS receita,
                ROUND(CAST(AVG(item_price)  AS numeric), 2)    AS ticket_medio,
                ROUND(CAST(AVG(freight_value) AS numeric), 2)  AS frete_medio
            FROM fato_pedidos
            WHERE order_status = 'delivered'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """,

        "performance_categorias": """
            SELECT
                p.product_category_name_english              AS categoria,
                COUNT(DISTINCT f.order_id)                   AS pedidos,
                ROUND(CAST(SUM(f.item_total)  AS numeric), 2) AS receita,
                ROUND(CAST(AVG(f.item_price)  AS numeric), 2) AS ticket_medio,
                ROUND(CAST(AVG(f.review_score) AS numeric), 2) AS nota_media
            FROM fato_pedidos f
            JOIN dim_produtos p ON f.product_id = p.product_id
            WHERE p.product_category_name_english IS NOT NULL
              AND p.product_category_name_english != 'unknown'
            GROUP BY 1
            ORDER BY receita DESC
        """,

        "satisfacao_estados": """
            SELECT
                customer_state                                     AS estado,
                COUNT(DISTINCT order_id)                           AS pedidos,
                ROUND(CAST(AVG(review_score)   AS numeric), 2)    AS nota_media,
                ROUND(CAST(AVG(delivery_days)  AS numeric), 1)    AS prazo_medio_dias,
                ROUND(CAST(AVG(is_late) * 100  AS numeric), 1)    AS pct_atraso
            FROM fato_pedidos
            WHERE order_status = 'delivered'
              AND customer_state IS NOT NULL
            GROUP BY 1
            ORDER BY pedidos DESC
        """,

        "tempo_entrega": """
            SELECT
                purchase_year  AS ano,
                purchase_month AS mes,
                ROUND(CAST(AVG(delivery_days) AS numeric), 1)   AS prazo_medio,
                MIN(delivery_days)                               AS prazo_minimo,
                MAX(delivery_days)                               AS prazo_maximo,
                SUM(is_late)                                     AS atrasados,
                COUNT(*)                                         AS total_entregas,
                ROUND(CAST(AVG(is_late) * 100 AS numeric), 1)   AS pct_atraso
            FROM fato_pedidos
            WHERE order_status = 'delivered'
              AND delivery_days IS NOT NULL
              AND delivery_days > 0
            GROUP BY 1, 2
            ORDER BY 1, 2
        """,

        "performance_vendedores": """
            SELECT
                f.seller_id,
                s.seller_state                                      AS estado,
                s.seller_city                                       AS cidade,
                COUNT(DISTINCT f.order_id)                          AS pedidos,
                ROUND(CAST(SUM(f.item_total)   AS numeric), 2)     AS receita,
                ROUND(CAST(AVG(f.item_price)   AS numeric), 2)     AS ticket_medio,
                ROUND(CAST(AVG(f.review_score) AS numeric), 2)     AS nota_media,
                ROUND(CAST(AVG(f.is_late) * 100 AS numeric), 1)    AS pct_atraso
            FROM fato_pedidos f
            JOIN dim_vendedores s ON f.seller_id = s.seller_id
            WHERE f.order_status = 'delivered'
            GROUP BY f.seller_id, s.seller_state, s.seller_city
            HAVING COUNT(DISTINCT f.order_id) >= 10
            ORDER BY receita DESC
            LIMIT 50
        """,
    }

    for name, query in queries.items():
        df = pd.read_sql(text(query), engine)
        output = f"{EXPORT_PATH}/{name}.csv"
        df.to_csv(output, index=False)
        log.info(f"   ✅ {name:<30} {len(df):>6} linhas → {output}")

    log.info("✅ Análises concluídas — CSVs prontos para o dashboard")


# ─────────────────────────────────────────
# Task 5 — Embeddings (RAG + pgvector)
# ─────────────────────────────────────────

def task_embeddings(**context):
    """
    Lê os reviews da camada RAW, gera embeddings com fastembed
    e salva na tabela reviews_embeddings no pgvector.
    """
    import os
    import pandas as pd
    from sqlalchemy import create_engine, text
    from fastembed import TextEmbedding

    log.info("🚀 Iniciando geração de embeddings")

    conn_str = os.environ.get(
        "OLIST_DW_CONN",
        "postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw"
    )
    engine = create_engine(conn_str)

    # Lê reviews da camada RAW
    reviews = pd.read_parquet(f"{RAW_PATH}/order_reviews/data.parquet")

    # Filtra apenas reviews com texto
    reviews = reviews[
        reviews["review_comment_message"].notna() &
        (reviews["review_comment_message"].str.strip() != "")
    ][["order_id", "review_score", "review_comment_message"]].copy()

    reviews = reviews.sample(n=5000, random_state=42).reset_index(drop=True)
    reviews.columns = ["order_id", "review_score", "review_text"]
    reviews["review_score"] = reviews["review_score"].astype(int)

    log.info(f"   📝 {len(reviews):,} reviews com texto encontrados")

    # Gera embeddings em batches
    model = TextEmbedding("BAAI/bge-small-en-v1.5")
    texts = reviews["review_text"].tolist()

    log.info("   🔢 Gerando embeddings...")
    embeddings = list(model.embed(texts))
    reviews["embedding"] = [e.tolist() for e in embeddings]

    # Salva no pgvector
    records = reviews[["order_id", "review_score", "review_text", "embedding"]].to_dict("records")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE reviews_embeddings RESTART IDENTITY"))

    with engine.begin() as conn:
        for i, row in enumerate(records):
            conn.execute(
                text("""
                    INSERT INTO reviews_embeddings (order_id, review_score, review_text, embedding)
                    VALUES (:order_id, :review_score, :review_text, :embedding)
                """),
                {
                    "order_id":     row["order_id"],
                    "review_score": row["review_score"],
                    "review_text":  row["review_text"],
                    "embedding":    str(row["embedding"]),
                }
            )
            if (i + 1) % 1000 == 0:
                log.info(f"   ✅ {i+1:,}/{len(records):,} embeddings salvos")

    log.info(f"✅ Embeddings concluídos — {len(records):,} reviews vetorizados")
    return {"embeddings_count": len(records)}

# ─────────────────────────────────────────
# Definição da DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="olist_etl_pipeline",
    description="Pipeline ETL End-to-End — Olist E-commerce",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["olist", "etl", "pyspark", "postgresql"],
) as dag:

    t1 = PythonOperator(
        task_id="ingestao",
        python_callable=task_ingestao,
        doc_md="Baixa CSVs do Kaggle → converte para Parquet → salva no volume RAW",
    )

    t2 = PythonOperator(
        task_id="processamento",
        python_callable=task_processamento,
        doc_md="PySpark: RAW → limpeza, tipagem, joins → TRUSTED",
    )

    t3 = PythonOperator(
        task_id="carga",
        python_callable=task_carga,
        doc_md="TRUSTED (Parquet) → PostgreSQL DW (esquema estrela)",
    )

    t4 = PythonOperator(
        task_id="analise",
        python_callable=task_analise,
        doc_md="Queries analíticas no DW → exporta CSVs para o dashboard Streamlit",
    )

    t5 = PythonOperator(
        task_id="embeddings",
        python_callable=task_embeddings,
        doc_md="Reviews RAW → fastembed → pgvector (reviews_embeddings)",
    )

    t1 >> t2 >> t3 >> t4 >> t5
