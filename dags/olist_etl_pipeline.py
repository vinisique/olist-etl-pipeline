"""
DAG: olist_etl_pipeline
Pipeline ETL End-to-End — Olist E-commerce Dataset

Orquestra as 4 etapas do pipeline:
  1. ingestao   → Baixa dataset do Kaggle e envia para S3 (LocalStack) em Parquet
  2. processamento → PySpark: RAW → TRUSTED (limpeza, tipagem, joins)
  3. carga       → TRUSTED → PostgreSQL DW (esquema estrela)
  4. analise     → Queries analíticas e exportação para o dashboard
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging

log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Configuração default da DAG
# ─────────────────────────────────────────
default_args = {
    "owner": "vinicius.siqueira",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────
# Tasks
# ─────────────────────────────────────────

def task_ingestao(**context):
    """
    Etapa 1 — Ingestão
    Baixa os CSVs do Kaggle, converte para Parquet
    e envia para o bucket S3 no LocalStack (camada RAW).
    """
    import os
    import json
    import boto3
    import pandas as pd
    import kagglehub
    from datetime import datetime
    from io import BytesIO

    log.info("🚀 Iniciando ingestão — Olist Dataset")

    # Configurar cliente S3 apontando para LocalStack
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    bucket = os.environ.get("S3_BUCKET", "olist-data-lake")

    # Criar bucket se não existir
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        log.info(f"✅ Bucket criado: {bucket}")

    # Baixar dataset
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
        df["_ingested_at"] = datetime.now().isoformat()
        df["_source_file"] = csv_file

        # Converter para Parquet em memória
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload para S3
        s3_key = f"raw/{table_name}/data.parquet"
        s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())

        total_rows += len(df)
        results.append({"table": table_name, "rows": len(df), "s3_key": s3_key})
        log.info(f"   ✅ {table_name:<25} {len(df):>10,} linhas → s3://{bucket}/{s3_key}")

    log.info(f"\n✅ Ingestão concluída — {len(results)} tabelas · {total_rows:,} linhas")

    # Passar resultado para a próxima task via XCom
    return {"tables": results, "total_rows": total_rows}


def task_processamento(**context):
    """
    Etapa 2 — Processamento com PySpark
    Lê Parquets da camada RAW (S3/LocalStack),
    aplica limpeza e tipagem, e salva na camada TRUSTED.
    """
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    log.info("🚀 Iniciando processamento PySpark")

    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")

    spark = (
        SparkSession.builder.appName("olist-etl-processing")
        .config("spark.driver.memory", "2g")
        .config("spark.hadoop.fs.s3a.endpoint", localstack_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    bucket = os.environ.get("S3_BUCKET", "olist-data-lake")
    RAW = f"s3a://{bucket}/raw"
    TRUSTED = f"s3a://{bucket}/trusted"

    # Ler RAW
    orders   = spark.read.parquet(f"{RAW}/orders")
    items    = spark.read.parquet(f"{RAW}/order_items")
    payments = spark.read.parquet(f"{RAW}/order_payments")
    reviews  = spark.read.parquet(f"{RAW}/order_reviews")
    customers = spark.read.parquet(f"{RAW}/customers")
    sellers  = spark.read.parquet(f"{RAW}/sellers")
    products = spark.read.parquet(f"{RAW}/products")
    category = spark.read.parquet(f"{RAW}/category_translation")

    # Processar orders
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
                F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), 1
            ).otherwise(0))
        .drop("_ingested_at", "_source_file")
    )

    # Processar itens
    items_clean = (
        items
        .withColumn("price",          F.col("price").cast("double"))
        .withColumn("freight_value",  F.col("freight_value").cast("double"))
        .withColumn("item_total",     F.col("price") + F.col("freight_value"))
        .drop("_ingested_at", "_source_file")
    )

    # Processar pagamentos — agregar por pedido
    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment"),
            F.max("payment_installments").alias("max_installments"),
            F.first("payment_type").alias("payment_type"),
        )
    )

    # Processar reviews — agregar por pedido
    reviews_agg = (
        reviews
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("review_score"))
    )

    # Produtos com categoria em inglês
    products_clean = (
        products
        .join(category, "product_category_name", "left")
        .drop("_ingested_at", "_source_file")
    )

    # Montar tabela fato
    fato = (
        orders_clean
        .join(items_clean.groupBy("order_id").agg(
            F.sum("item_total").alias("item_total"),
            F.sum("price").alias("item_price"),
            F.sum("freight_value").alias("freight_value"),
            F.first("seller_id").alias("seller_id"),
            F.first("product_id").alias("product_id"),
        ), "order_id", "left")
        .join(payments_agg, "order_id", "left")
        .join(reviews_agg,  "order_id", "left")
        .join(customers.select("customer_id", "customer_state", "customer_city"),
              "customer_id", "left")
    )

    # Salvar TRUSTED
    fato.write.mode("overwrite").parquet(f"{TRUSTED}/fato_pedidos")
    orders_clean.write.mode("overwrite").parquet(f"{TRUSTED}/orders")
    products_clean.write.mode("overwrite").parquet(f"{TRUSTED}/products")
    customers.drop("_ingested_at","_source_file").write.mode("overwrite").parquet(f"{TRUSTED}/customers")
    sellers.drop("_ingested_at","_source_file").write.mode("overwrite").parquet(f"{TRUSTED}/sellers")

    count = fato.count()
    log.info(f"✅ Processamento concluído — fato_pedidos: {count:,} linhas")
    spark.stop()
    return {"fato_rows": count}


def task_carga(**context):
    """
    Etapa 3 — Carga no Data Warehouse
    Lê camada TRUSTED do S3 e carrega no PostgreSQL
    com esquema estrela.
    """
    import os
    import pandas as pd
    import boto3
    from sqlalchemy import create_engine, text
    from io import BytesIO

    log.info("🚀 Iniciando carga no Data Warehouse")

    conn_str = os.environ.get(
        "OLIST_DW_CONN",
        "postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw"
    )
    engine = create_engine(conn_str)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    bucket = os.environ.get("S3_BUCKET", "olist-data-lake")

    def read_parquet_from_s3(key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(obj["Body"].read()))

    # Carregar tabelas no DW
    tables = {
        "fato_pedidos":  "trusted/fato_pedidos/data.parquet",
        "dim_clientes":  "trusted/customers/data.parquet",
        "dim_vendedores":"trusted/sellers/data.parquet",
        "dim_produtos":  "trusted/products/data.parquet",
    }

    for table_name, s3_key in tables.items():
        df = read_parquet_from_s3(s3_key)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        log.info(f"   ✅ {table_name:<20} {len(df):>10,} linhas carregadas")

    log.info("✅ Carga concluída")


def task_analise(**context):
    """
    Etapa 4 — Análises finais
    Queries analíticas no DW e exportação dos CSVs
    para consumo pelo dashboard Streamlit.
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
    export_path = "/opt/airflow/scripts/exports"
    os.makedirs(export_path, exist_ok=True)

    queries = {
        "receita_mensal": """
            SELECT purchase_year AS ano, purchase_month AS mes,
                   COUNT(DISTINCT order_id)          AS pedidos,
                   ROUND(SUM(item_total)::numeric, 2) AS receita,
                   ROUND(AVG(item_total)::numeric, 2) AS ticket_medio,
                   ROUND(AVG(freight_value)::numeric, 2) AS frete_medio
            FROM fato_pedidos
            WHERE order_status = 'delivered'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """,
        "performance_categorias": """
            SELECT p.product_category_name_english AS categoria,
                   COUNT(DISTINCT f.order_id)       AS pedidos,
                   ROUND(SUM(f.item_total)::numeric, 2) AS receita,
                   ROUND(AVG(f.item_total)::numeric, 2) AS ticket_medio,
                   ROUND(AVG(f.review_score)::numeric, 2) AS nota_media
            FROM fato_pedidos f
            JOIN dim_produtos p ON f.product_id = p.product_id
            WHERE p.product_category_name_english IS NOT NULL
            GROUP BY 1
            ORDER BY receita DESC
        """,
        "satisfacao_estados": """
            SELECT customer_state AS estado,
                   COUNT(DISTINCT order_id)           AS pedidos,
                   ROUND(AVG(review_score)::numeric, 2) AS nota_media,
                   ROUND(AVG(delivery_days)::numeric, 1) AS prazo_medio_dias,
                   ROUND(AVG(is_late::int)*100, 1)    AS pct_atraso
            FROM fato_pedidos
            WHERE order_status = 'delivered'
            GROUP BY 1
            ORDER BY pedidos DESC
        """,
        "tempo_entrega": """
            SELECT purchase_year AS ano, purchase_month AS mes,
                   ROUND(AVG(delivery_days)::numeric, 1)    AS prazo_medio,
                   ROUND(AVG(is_late::int)*100, 1)          AS pct_atraso
            FROM fato_pedidos
            WHERE order_status = 'delivered' AND delivery_days IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 2
        """,
        "performance_vendedores": """
            SELECT f.seller_id,
                   s.seller_state AS estado,
                   s.seller_city  AS cidade,
                   COUNT(DISTINCT f.order_id)               AS pedidos,
                   ROUND(SUM(f.item_total)::numeric, 2)     AS receita,
                   ROUND(AVG(f.item_total)::numeric, 2)     AS ticket_medio,
                   ROUND(AVG(f.review_score)::numeric, 2)   AS nota_media,
                   ROUND(AVG(f.is_late::int)*100, 1)        AS pct_atraso
            FROM fato_pedidos f
            JOIN dim_vendedores s ON f.seller_id = s.seller_id
            GROUP BY 1, 2, 3
            ORDER BY receita DESC
            LIMIT 50
        """,
    }

    for name, query in queries.items():
        df = pd.read_sql(text(query), engine)
        df.to_csv(f"{export_path}/{name}.csv", index=False)
        log.info(f"   ✅ {name:<30} {len(df):>6} linhas exportadas")

    log.info("✅ Análises concluídas — CSVs prontos para o dashboard")


# ─────────────────────────────────────────
# Definição da DAG
# ─────────────────────────────────────────
with DAG(
    dag_id="olist_etl_pipeline",
    description="Pipeline ETL End-to-End — Olist E-commerce",
    default_args=default_args,
    schedule_interval=None,          # Trigger manual
    start_date=days_ago(1),
    catchup=False,
    tags=["olist", "etl", "pyspark", "s3", "postgresql"],
) as dag:

    t1 = PythonOperator(
        task_id="ingestao",
        python_callable=task_ingestao,
        doc_md="Baixa CSVs do Kaggle → converte para Parquet → envia para S3 (LocalStack RAW)",
    )

    t2 = PythonOperator(
        task_id="processamento",
        python_callable=task_processamento,
        doc_md="PySpark: RAW → limpeza, tipagem, joins → TRUSTED",
    )

    t3 = PythonOperator(
        task_id="carga",
        python_callable=task_carga,
        doc_md="TRUSTED → PostgreSQL DW (esquema estrela)",
    )

    t4 = PythonOperator(
        task_id="analise",
        python_callable=task_analise,
        doc_md="Queries analíticas no DW → exporta CSVs para o dashboard",
    )

    t1 >> t2 >> t3 >> t4
