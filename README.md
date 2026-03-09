# Pipeline ETL End-to-End — Olist E-commerce

Pipeline de dados completo construído sobre o dataset público de e-commerce brasileiro da Olist, cobrindo todas as etapas de engenharia de dados: ingestão, processamento distribuído, modelagem dimensional e visualização analítica.

---

## Visão Geral

| | |
|---|---|
| **Dataset** | [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |
| **Volume** | 1.550.922 registros · 9 tabelas · 54,5 MB |
| **Período** | Setembro/2016 a Outubro/2018 |
| **Stack** | Python · PySpark · PostgreSQL · Airflow · LocalStack · Streamlit · Plotly · Docker |

---

## Arquitetura

```
Kaggle Dataset (CSV)
        │
        ▼
┌─────────────────┐
│   01_exploratory │  Análise exploratória, qualidade e relacionamentos
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│                   Apache Airflow                     │
│                                                      │
│  ┌────────────┐    ┌───────────────┐    ┌─────────┐ │
│  │  ingestao  │ →  │ processamento │ →  │  carga  │ │
│  │            │    │   (PySpark)   │    │  (DW)   │ │
│  └────────────┘    └───────────────┘    └────┬────┘ │
│        │                  │                  │       │
│        ▼                  ▼                  ▼       │
│  S3 LocalStack       S3 LocalStack      PostgreSQL   │
│  (camada RAW)       (camada TRUSTED)   (olist_dw)   │
│                                             │        │
│                                    ┌────────┘        │
│                                    ▼                 │
│                              ┌──────────┐            │
│                              │  analise │            │
│                              └────┬─────┘            │
└───────────────────────────────────┼─────────────────┘
                                    │
                                    ▼
                            CSVs analíticos
                                    │
                                    ▼
                         ┌──────────────────┐
                         │ Dashboard Streamlit│
                         └──────────────────┘
```

---

## Stack Técnica

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 2.8 |
| Processamento | PySpark 3.5 |
| Data Lake | LocalStack (S3) · Parquet |
| Data Warehouse | PostgreSQL 15 · Esquema estrela |
| Dashboard | Streamlit · Plotly |
| Containerização | Docker · Docker Compose |
| Linguagem | Python 3.11 |

---

## Como Rodar com Docker

### Pré-requisitos
- Docker Desktop instalado ([download](https://www.docker.com/products/docker-desktop))
- 8 GB de RAM disponível para os containers

### 1. Clone o repositório
```bash
git clone https://github.com/viniciussiqueira/olist-etl-pipeline
cd olist-etl-pipeline
```

### 2. Suba os containers
```bash
docker compose up -d
```

Isso sobe automaticamente:
- **Airflow** (webserver + scheduler) → `http://localhost:8080`
- **LocalStack** (S3 simulado) → `http://localhost:4566`
- **PostgreSQL** (Data Warehouse) → `localhost:5432`
- **Streamlit** (dashboard) → `http://localhost:8501`

### 3. Acesse o Airflow e rode o pipeline
```
URL:   http://localhost:8080
User:  admin
Pass:  admin
```
Na interface do Airflow, ative e dispare a DAG `olist_etl_pipeline`.

### 4. Acesse o dashboard
Após o pipeline concluir:
```
http://localhost:8501
```

### Parar os containers
```bash
docker compose down
```

### Parar e limpar volumes (reset completo)
```bash
docker compose down -v
```

---

## Desenvolvimento nos Notebooks (sem Docker)

Os notebooks foram desenvolvidos no Google Colab e podem ser executados sem Docker:

```
notebooks/01_exploratory.ipynb   → Exploração (opcional)
notebooks/02_ingestion.ipynb     → Cria a camada RAW do Data Lake
notebooks/03_processing.ipynb    → Processa e cria a camada TRUSTED
notebooks/04_load.ipynb          → Carrega no PostgreSQL
notebooks/05_analysis.ipynb      → Gera os CSVs analíticos
```

> O dataset é baixado automaticamente via `kagglehub` na primeira execução.

---

## Etapas do Pipeline

### 01 — Exploração dos Dados
Análise completa das 9 tabelas do dataset: estrutura, tipos, valores nulos, distribuições e relacionamentos. Identificação de anomalias antes da ingestão.

**Principais achados:**
- 97% dos pedidos com status `delivered`
- 73,9% dos pagamentos via cartão de crédito
- R$ 16 milhões transacionados no período
- Campos de comentário com até 88% de nulos — tratados na camada TRUSTED

### 02 — Ingestão para o Data Lake
Leitura dos CSVs brutos, conversão para Parquet com compressão de até 71,6% e salvamento na camada RAW com metadados de rastreabilidade (`_ingested_at`, `_source_file`).

```
customers         99.441 linhas   21,5% de compressão
orders            99.441 linhas   38,4% de compressão
order_items      112.650 linhas   57,2% de compressão
geolocation    1.000.163 linhas   71,6% de compressão
─────────────────────────────────────────────────────
Total          1.550.922 linhas   54,5 MB → Data Lake
```

### 03 — Processamento com PySpark
Transformação na camada TRUSTED com PySpark.

- Tipagem correta de todas as colunas de data
- Tratamento de nulos com estratégias por coluna
- Colunas derivadas: `delivery_days`, `is_late`, `purchase_weekday`
- Joins entre tabelas fato e dimensão
- Modelagem em esquema estrela

### 04 — Carga no Data Warehouse
Carga no PostgreSQL com esquema estrela:

| Tabela | Tipo | Descrição |
|---|---|---|
| `fato_pedidos` | Fato | 119.137 linhas · tabela central |
| `dim_clientes` | Dimensão | Clientes por estado e cidade |
| `dim_vendedores` | Dimensão | 3.095 vendedores ativos |
| `dim_produtos` | Dimensão | 32.951 produtos categorizados |
| `dim_tempo` | Dimensão | Granularidade diária 2016–2018 |
| `dim_localizacao` | Dimensão | Geolocalização por CEP |

### 05 — Análises e Exportação
Queries analíticas no Data Warehouse e exportação de 5 CSVs para consumo no dashboard:

- `receita_mensal.csv` — 23 períodos
- `performance_categorias.csv` — 72 categorias
- `satisfacao_estados.csv` — 27 estados
- `tempo_entrega.csv` — série temporal de logística
- `performance_vendedores.csv` — top 50 sellers

---

## Dashboard Streamlit

5 páginas analíticas interativas com filtro por ano:

**Visão Geral** — KPIs, receita acumulada, taxa de atraso, top estados, avaliações

**Análise Temporal** — Receita e volume mensal, ticket médio, frete, crescimento anual

**Categorias & Produtos** — Receita por categoria, scatter satisfação vs receita, tabela completa

**Logística & Entregas** — Prazo médio, taxa de atraso por mês, performance por estado

**Vendedores** — Receita vs satisfação, ranking por estado, top 50 sellers

---

## Resultados e Insights

- **Black Friday 2017** gerou o maior pico de volume no período
- **92,2% de entrega no prazo** — taxa de atraso de 7,8%
- **65,6% das avaliações são 5 estrelas** — nota média de 4,01
- **SP concentra** o maior volume de pedidos e vendedores
- **Ticket médio estável** em torno de R$ 137 — crescimento por volume, não por preço

---

## Estrutura do Repositório

```
olist-etl-pipeline/
├── docker-compose.yml          ← Sobe Airflow + LocalStack + PostgreSQL + Streamlit
├── .env                        ← Variáveis de ambiente
├── dags/
│   └── olist_etl_pipeline.py  ← DAG principal (4 tasks encadeadas)
├── scripts/
│   ├── init_s3.sh             ← Cria bucket no LocalStack
│   ├── init_dw.sql            ← Inicializa o Data Warehouse
│   └── exports/               ← CSVs gerados pelo pipeline
├── notebooks/
│   ├── 01_exploratory.ipynb
│   ├── 02_ingestion.ipynb
│   ├── 03_processing.ipynb
│   ├── 04_load.ipynb
│   └── 05_analysis.ipynb
├── dashboard/
│   ├── app.py                 ← Dashboard Streamlit
│   └── data/                  ← CSVs para desenvolvimento local
└── README.md
```

---

## Autor

**Vinicius Siqueira**
[LinkedIn](https://linkedin.com/in/vinicius-siqueira1) · [GitHub](https://github.com/viniciussiqueira)
