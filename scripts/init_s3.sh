#!/bin/bash
# Cria o bucket do Data Lake no LocalStack ao iniciar o serviço

echo "🪣 Criando bucket S3 no LocalStack..."

awslocal s3 mb s3://olist-data-lake

awslocal s3api put-bucket-versioning \
  --bucket olist-data-lake \
  --versioning-configuration Status=Enabled

echo "✅ Bucket criado: s3://olist-data-lake"
echo "   Estrutura:"
echo "   s3://olist-data-lake/raw/      ← Camada RAW (Parquet bruto)"
echo "   s3://olist-data-lake/trusted/  ← Camada TRUSTED (processado)"
