-- Inicialização do Data Warehouse Olist
-- Executado automaticamente ao subir o container postgres-dw

-- Extensões úteis
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Schema para o Data Warehouse
CREATE SCHEMA IF NOT EXISTS dw;

-- Comentário no banco
COMMENT ON DATABASE olist_dw IS 'Data Warehouse — Pipeline ETL Olist E-commerce';
