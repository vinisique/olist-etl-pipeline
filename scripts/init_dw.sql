-- Inicialização do Data Warehouse Olist
-- Executado automaticamente ao subir o container postgres-dw

-- Extensões úteis
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

-- Schema para o Data Warehouse
CREATE SCHEMA IF NOT EXISTS dw;

-- Comentário no banco
COMMENT ON DATABASE olist_dw IS 'Data Warehouse — Pipeline ETL Olist E-commerce';

-- Tabela de embeddings para RAG — armazena vetores dos reviews dos clientes (pgvector)
CREATE TABLE IF NOT EXISTS reviews_embeddings (
    id          SERIAL PRIMARY KEY,
    order_id    VARCHAR(50),
    review_score INTEGER,
    review_text TEXT,
    embedding   vector(384)
);