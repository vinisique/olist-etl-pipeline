# Pipeline ETL End-to-End — Olist E-commerce

Pipeline de dados completo construído sobre o dataset público de e-commerce brasileiro da Olist, cobrindo todas as etapas de engenharia de dados: ingestão, processamento distribuído, modelagem dimensional, análise semântica com NLP e visualização analítica com IA generativa.
ACESSE O STREAMLIT: https://olist-etl-pipeline-fluee76u4cfellwfvtbxph.streamlit.app/

---

## Visão Geral

| | |
|---|---|
| **Dataset** | [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |
| **Volume** | 1.550.922 registros · 9 tabelas · 54,5 MB |
| **Período** | Setembro/2016 a Outubro/2018 |
| **Stack** | Python · PySpark · PostgreSQL · pgvector · Airflow · LocalStack · Streamlit · Plotly · PyTorch · sentence-transformers · fastembed · UMAP · scikit-learn · RAG · LangChain · Groq · Docker |

---

## Arquitetura

```
Kaggle Dataset (CSV)
        │
        ▼
┌─────────────────┐
│  01_exploratory  │  Análise exploratória, qualidade e relacionamentos
└────────┬────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────┐
│                      Apache Airflow                         │
│                                                             │
│  ┌──────────┐  ┌───────────────┐  ┌────────┐  ┌────────┐  │
│  │ ingestao │→ │ processamento │→ │ carga  │→ │analise │  │
│  │          │  │   (PySpark)   │  │  (DW)  │  │        │  │
│  └──────────┘  └───────────────┘  └────────┘  └───┬────┘  │
│       │               │                │           │       │
│       ▼               ▼                ▼           ▼       │
│  Volume local     Volume local    PostgreSQL    CSVs para  │
│  (camada RAW)    (camada TRUSTED)  (olist_dw)   dashboard  │
│  Parquet          Parquet          Star Schema             │
│       │                                │                   │
│       └──────────────┐                 │                   │
│                      ▼                 ▼                   │
│               ┌────────────┐   ┌──────────────┐           │
│               │ embeddings │   │  LocalStack  │           │
│               │ (pgvector) │   │  (S3 ref.)   │           │
│               └────────────┘   └──────────────┘           │
└────────────────────────┬───────────────────────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Dashboard Streamlit  │
              │  6 páginas analíticas │
              │  + Chat IA (RAG+SQL)  │
              └──────────────────────┘
```

---

## Stack Técnica

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow 2.8 |
| Processamento distribuído | PySpark 4.0 |
| Data Lake | Volume local compartilhado · Parquet · Snappy compression |
| Data Warehouse | PostgreSQL 15 + pgvector · Esquema estrela |
| Ingestão de dados | kagglehub · pandas · SQLAlchemy |
| NLP / Embeddings | fastembed (`BAAI/bge-small-en-v1.5`) · sentence-transformers (`paraphrase-multilingual-MiniLM-L12-v2`) · UMAP |
| Busca semântica | pgvector · scikit-learn (cosine similarity) · RAG (Retrieval-Augmented Generation) |
| Deep Learning | PyTorch 2.x · CUDA (GPU Tesla T4 no Colab) |
| IA Generativa | Groq (`llama-3.3-70b-versatile`) · LangChain · Text-to-SQL |
| Dashboard | Streamlit · Plotly |
| Containerização | Docker · Docker Compose |
| Linguagem | Python 3.11 |

---

## Como Rodar com Docker

### Pré-requisitos
- Docker Desktop instalado ([download](https://www.docker.com/products/docker-desktop))
- 8 GB de RAM disponível para os containers
- Chave da API Groq para o Chat IA ([console.groq.com](https://console.groq.com))

### 1. Clone o repositório
```bash
git clone https://github.com/viniciussiqueira/olist-etl-pipeline
cd olist-etl-pipeline
```

### 2. Configure as variáveis de ambiente
```bash
cp .env.example .env
```

Edite o `.env` e adicione sua chave do Groq:
```env
GROQ_API_KEY=gsk_...
```

### 3. Suba os containers
```bash
docker compose up -d
```

Isso sobe automaticamente:
- **Airflow** (webserver + scheduler) → `http://localhost:8080`
- **LocalStack** (S3 simulado) → `http://localhost:4566`
- **PostgreSQL + pgvector** (Data Warehouse) → `localhost:5432`
- **Streamlit** (dashboard) → `http://localhost:8501`

### 4. Acesse o Airflow e rode o pipeline
```
URL:   http://localhost:8080
User:  admin
Pass:  admin
```

Na interface do Airflow, ative e dispare a DAG `olist_etl_pipeline`.

O pipeline executa 5 tasks em sequência:
```
ingestao → processamento → carga → analise → embeddings
```

| Task | O que faz |
|---|---|
| `ingestao` | Baixa dataset do Kaggle → converte para Parquet → camada RAW |
| `processamento` | PySpark: RAW → limpeza, tipagem, colunas derivadas → camada TRUSTED |
| `carga` | TRUSTED (Parquet) → PostgreSQL DW com esquema estrela |
| `analise` | Queries analíticas no DW → exporta 5 CSVs para o dashboard |
| `embeddings` | Reviews RAW → fastembed (BAAI/bge-small-en-v1.5) → pgvector |

Ao concluir, os CSVs são salvos em `scripts/exports/` (montado como `/app/data/` no Streamlit) e os embeddings ficam disponíveis no pgvector para o Chat IA.

### 5. Acesse o dashboard
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
notebooks/01_exploratory.ipynb   → Exploração inicial (opcional)
notebooks/02_ingestion.ipynb     → Ingestão: CSV → Parquet (camada RAW)
notebooks/03_processing.ipynb    → Processamento PySpark: RAW → TRUSTED
notebooks/04_load.ipynb          → Carga no PostgreSQL (esquema estrela)
notebooks/05_analysis.ipynb      → Queries analíticas → CSVs para o dashboard
notebooks/06_embeddings.ipynb    → Embeddings semânticos, UMAP e análise NLP
```

> O dataset é baixado automaticamente via `kagglehub` na primeira execução de cada notebook.

**Para rodar o Streamlit localmente** (fora do Docker), copie os CSVs gerados pelo notebook `05_analysis.ipynb` para `dashboard/data/` e exporte `GROQ_API_KEY` no ambiente.

---

## Etapas do Pipeline

### 01 — Exploração dos Dados

Análise completa das 9 tabelas do dataset: estrutura, tipos, valores nulos, distribuições e relacionamentos.

**Principais achados:**
- 97,0% dos pedidos com status `delivered`
- 73,9% dos pagamentos via cartão de crédito
- R$ 16 milhões transacionados no período
- Campos de comentário com até 88% de nulos — tratados na camada TRUSTED

### 02 — Ingestão para o Data Lake

Leitura dos CSVs brutos, adição de metadados de rastreabilidade (`_ingested_at`, `_source_file`) e salvamento em Parquet na camada RAW.

```
customers           99.441 linhas    21,5% de compressão
orders              99.441 linhas    38,4% de compressão
order_items        112.650 linhas    57,2% de compressão
order_payments     103.886 linhas    32,9% de compressão
order_reviews       99.224 linhas    32,9% de compressão
products            32.951 linhas    40,0% de compressão
geolocation      1.000.163 linhas    71,6% de compressão
sellers              3.095 linhas    22,0% de compressão
─────────────────────────────────────────────────────────
Total            1.550.922 linhas    54,5 MB → camada RAW
```

### 03 — Processamento com PySpark

Transformação na camada TRUSTED com PySpark 4.0:

- Tipagem correta de todas as colunas de data
- Tratamento de nulos com estratégias por coluna
- Colunas derivadas: `delivery_days`, `is_late`, `purchase_weekday`, `item_total`
- Deduplicação da geolocation: 1.000.163 → 19.015 registros únicos
- Join entre tabelas e montagem do `fato_pedidos` (119.137 linhas · 25 colunas)
- Tradução de categorias aplicada via `category_translation`

### 04 — Carga no Data Warehouse

Carga no PostgreSQL com pgvector e esquema estrela:

| Tabela | Tipo | Linhas | Descrição |
|---|---|---|---|
| `fato_pedidos` | Fato | 119.137 | Tabela central com todas as métricas |
| `dim_clientes` | Dimensão | 99.441 | Clientes por estado e cidade |
| `dim_vendedores` | Dimensão | 3.095 | Vendedores ativos |
| `dim_produtos` | Dimensão | 32.951 | Produtos com categorias em inglês |
| `dim_localizacao` | Dimensão | 19.015 | Geolocalização deduplicada por CEP |
| `dim_tempo` | Dimensão | 773 | Granularidade diária 2016–2018 |
| `reviews_embeddings` | Vetor | 5.000 | Reviews vetorizados (pgvector) |

### 05 — Análises e Exportação

Queries analíticas no Data Warehouse e exportação de 5 CSVs para consumo no dashboard:

| Arquivo | Linhas | Conteúdo |
|---|---|---|
| `receita_mensal.csv` | 23 | Receita, volume e ticket médio por mês |
| `performance_categorias.csv` | 72 | Métricas por categoria de produto |
| `satisfacao_estados.csv` | 27 | Nota, prazo e atraso por estado |
| `tempo_entrega.csv` | 23 | Série temporal de logística |
| `performance_vendedores.csv` | 50 | Top 50 sellers por receita |

### 06 — Embeddings e Análise Semântica *(notebook exploratório)*

Exploração aprofundada dos reviews com NLP moderno, executado no Google Colab com GPU (Tesla T4):

- **sentence-transformers** (`paraphrase-multilingual-MiniLM-L12-v2`) gera vetores de 384 dimensões para 5.000 reviews em português
- **UMAP** reduz 384D → 2D preservando estrutura semântica, revelando clusters naturais por sentimento
- **Cosine similarity** para busca de reviews semanticamente similares a qualquer texto de entrada
- **Análise de frequência de palavras** por nota (1–5), destacando vocabulário de satisfação vs insatisfação

No pipeline Airflow (Task 5), a vetorização usa o modelo leve `BAAI/bge-small-en-v1.5` via **fastembed** (sem GPU), salvando os embeddings diretamente no **pgvector**.

---

## Dashboard Streamlit

6 páginas analíticas interativas com filtro global por ano:

### 📊 Visão Geral
KPIs consolidados, receita acumulada com barras de volume, taxa de entrega (donut), top 5 estados e distribuição de avaliações por estrelas.

### 📈 Análise Temporal
Receita e volume de pedidos com eixo duplo, ticket médio e frete médio mensais, crescimento interanual.

### 🏷️ Categorias & Produtos
Receita por categoria (barras com gradiente), scatter interativo satisfação vs receita com tamanho proporcional ao volume, tabela completa com 72 categorias.

### 🚚 Logística & Entregas
Prazo médio de entrega com linha de referência, taxa de atraso por mês (escala de cor verde→vermelho), performance por estado com seletor de métrica.

### 🧑‍💼 Vendedores
Scatter receita vs satisfação colorido por taxa de atraso, ranking de receita por estado, tabela dos top 50 sellers.

### 🧠 Embeddings & NLP
- **Mapa Semântico UMAP** — visualização 2D de todos os reviews vetorizados, filtrável por sentimento e tamanho de amostra
- **Busca por Similaridade** — campo de busca livre que retorna os reviews semanticamente mais próximos com barras de similaridade
- **Análise de Sentimento** — distribuição de notas, donut de sentimentos, comparativo de palavras frequentes por nota com select slider

### 🤖 Chat IA *(RAG + Text-to-SQL)*
Interface de chat que responde perguntas sobre os dados com duas estratégias automáticas:

- **Text-to-SQL**: para perguntas numéricas e métricas, o modelo Groq (`llama-3.3-70b-versatile`) gera SQL, executa no PostgreSQL e interpreta os resultados em linguagem natural
- **RAG semântico**: para perguntas sobre opiniões e sentimentos dos clientes, busca no pgvector os reviews mais similares e gera uma análise interpretativa baseada nos textos reais

> Requer `GROQ_API_KEY` configurada no `.env` ou nas variáveis de ambiente do container Streamlit.

---

## Resultados e Insights

- **Black Friday 2017** gerou o maior pico de volume no período (novembro/2017)
- **92,2% de entrega no prazo** — taxa de atraso de 7,8%
- **65,6% das avaliações são 5 estrelas** — nota média de 4,01
- **SP concentra** o maior volume de pedidos e vendedores
- **Ticket médio estável** em torno de R$ 137 — crescimento por volume, não por preço
- **Clusters semânticos visíveis no UMAP** — reviews de reclamação e elogio formam regiões distintas sem supervisão
- **Reviews de nota 1** concentram: prazo, errado, cancelado, devolvido
- **Reviews de nota 5** concentram: ótimo, rápido, recomendo, perfeito

---

## Estrutura do Repositório

```
olist-etl-pipeline/
├── docker-compose.yml              ← Orquestra Airflow + LocalStack + PostgreSQL + Streamlit
├── Dockerfile                      ← Imagem base do Airflow com dependências do projeto
├── requirements.txt                ← Dependências Python do Airflow/pipeline
├── .env                            ← Variáveis de ambiente (GROQ_API_KEY, credenciais)
├── .gitignore
│
├── dags/
│   └── olist_etl_pipeline.py      ← DAG principal (5 tasks encadeadas)
│
├── scripts/
│   ├── init_s3.sh                 ← Cria bucket no LocalStack ao iniciar
│   ├── init_dw.sql                ← Inicializa schema do DW + extensão pgvector
│   │
│   ├── data_lake/                 ← Camadas do Data Lake (volume local compartilhado)
│   │   ├── raw/                   ← Camada RAW: Parquet bruto por tabela
│   │   │   ├── customers/data.parquet
│   │   │   ├── orders/data.parquet
│   │   │   ├── order_items/data.parquet
│   │   │   ├── order_payments/data.parquet
│   │   │   ├── order_reviews/data.parquet
│   │   │   ├── products/data.parquet
│   │   │   ├── sellers/data.parquet
│   │   │   ├── geolocation/data.parquet
│   │   │   └── category_translation/data.parquet
│   │   └── trusted/               ← Camada TRUSTED: Parquet Snappy (saída do PySpark)
│   │       ├── fato_pedidos/
│   │       ├── dim_customers/
│   │       ├── dim_sellers/
│   │       └── dim_products/
│   │
│   └── exports/                   ← CSVs analíticos gerados pela task analise
│       ├── receita_mensal.csv
│       ├── performance_categorias.csv
│       ├── satisfacao_estados.csv
│       ├── tempo_entrega.csv
│       ├── performance_vendedores.csv
│       ├── reviews_sample.csv     ← Amostra de reviews para a página NLP
│       └── reviews_umap.csv       ← Coordenadas UMAP pré-computadas
│
├── notebooks/
│   ├── 01_exploratory.ipynb       ← Análise exploratória dos dados brutos
│   ├── 02_ingestion.ipynb         ← Ingestão: CSV → Parquet (camada RAW)
│   ├── 03_processing.ipynb        ← PySpark: RAW → TRUSTED
│   ├── 04_load.ipynb              ← Carga no PostgreSQL (esquema estrela)
│   ├── 05_analysis.ipynb          ← Queries analíticas → CSVs para o dashboard
│   ├── 06_embeddings.ipynb        ← Embeddings, UMAP e busca semântica
│   └── data/
│       └── reviews_sample.csv     ← Amostra de 5.000 reviews (entrada do notebook 06)
│
└── dashboard/
    ├── app.py                     ← Dashboard principal (5 páginas analíticas + NLP)
    ├── requirements.txt           ← Dependências Python do Streamlit
    ├── pages/
    │   └── 6_Chat_IA.py           ← Página de Chat IA (RAG + Text-to-SQL via Groq)
    └── data/                      ← CSVs para desenvolvimento local (fora do Docker)
        ├── receita_mensal.csv
        ├── performance_categorias.csv
        ├── satisfacao_estados.csv
        ├── tempo_entrega.csv
        ├── performance_vendedores.csv
        └── reviews_sample.csv
```

> **Como o pipeline alimenta o dashboard:**
> A task `analise` salva os CSVs em `scripts/exports/`. O Docker Compose monta essa pasta como `/app/data/` no container Streamlit — sem intervenção manual. A task `embeddings` salva os vetores no pgvector do mesmo PostgreSQL acessado pelo Chat IA em `dashboard/pages/6_Chat_IA.py`.

---

## Variáveis de Ambiente

| Variável | Descrição | Valor padrão |
|---|---|---|
| `GROQ_API_KEY` | API key do Groq para o Chat IA | — (obrigatório) |
| `OLIST_DW_CONN` | Connection string do PostgreSQL DW | `postgresql+psycopg2://olist:olist2024@postgres-dw:5432/olist_dw` |
| `DATA_DIR` | Diretório dos CSVs analíticos no Streamlit | `/app/data` |
| `AWS_ACCESS_KEY_ID` | Credencial do LocalStack (S3) | `test` |
| `AWS_SECRET_ACCESS_KEY` | Credencial do LocalStack (S3) | `test` |
| `S3_BUCKET` | Nome do bucket no LocalStack | `olist-data-lake` |

---

## Autor

**Vinicius Siqueira**
[LinkedIn](https://linkedin.com/in/vinicius-siqueira1) · [GitHub](https://github.com/viniciussiqueira)
