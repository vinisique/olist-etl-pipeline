import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import re
from collections import Counter

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Olist Analytics",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# DESIGN SYSTEM
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Cabinet+Grotesk:wght@400;500;700;800;900&family=Geist+Mono:wght@300;400;500&display=swap');

:root {
    --bg-base:      #080A0F;
    --bg-surface:   #0E1118;
    --bg-elevated:  #141720;
    --bg-overlay:   #1A1E2B;
    --border:       rgba(255,255,255,0.06);
    --border-hover: rgba(99,102,241,0.5);
    --accent:       #6366F1;
    --accent-bright:#818CF8;
    --accent-glow:  rgba(99,102,241,0.12);
    --cyan:         #22D3EE;
    --green:        #10B981;
    --amber:        #F59E0B;
    --red:          #F43F5E;
    --orange:       #FB923C;
    --text-primary: #F1F5F9;
    --text-secondary:#94A3B8;
    --text-muted:   #475569;
    --font-display: 'Cabinet Grotesk', sans-serif;
    --font-mono:    'Geist Mono', monospace;
}

html, body, [class*="css"] { font-family: var(--font-display) !important; }

.stApp {
    background-color: var(--bg-base);
    background-image: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(99,102,241,0.07) 0%, transparent 60%);
    color: var(--text-primary);
}

header[data-testid="stHeader"] { background: transparent; }

[data-testid="stSidebar"] {
    background: var(--bg-surface) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text-secondary) !important; }

.nav-item {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 14px; border-radius: 10px; margin-bottom: 3px;
    font-size: 13px; font-weight: 600; cursor: pointer;
    color: var(--text-muted); border: 1px solid transparent;
    transition: all 0.15s ease;
}
.nav-item:hover { background: var(--bg-elevated); color: var(--text-primary); border-color: var(--border); }

[data-testid="stSidebar"] [data-testid="stPills"] {
    display: flex;
    flex-direction: column;
    gap: 3px;
}
[data-testid="stSidebar"] [data-testid="stPills"] button {
    background: transparent !important;
    border: 1px solid transparent !important;
    border-radius: 10px !important;
    color: var(--text-muted) !important;
    font-family: var(--font-display) !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    text-align: left !important;
    padding: 10px 14px !important;
    width: 100% !important;
    justify-content: flex-start !important;
    transition: none !important;
    box-shadow: none !important;
    transform: none !important;
}
[data-testid="stSidebar"] [data-testid="stPills"] button:hover {
    background: var(--bg-elevated) !important;
    color: var(--text-primary) !important;
    border-color: var(--border) !important;
    transition: none !important;
    box-shadow: none !important;
    transform: none !important;
}
[data-testid="stSidebar"] [data-testid="stPills"] button[aria-checked="true"] {
    background: var(--accent-glow) !important;
    color: var(--accent-bright) !important;
    border-color: rgba(99,102,241,0.25) !important;
    box-shadow: none !important;
    transform: none !important;
}
.nav-item.active { background: var(--accent-glow); color: var(--accent-bright) !important; border-color: rgba(99,102,241,0.25); }

.page-header {
    padding: 36px 0 28px; border-bottom: 1px solid var(--border);
    margin-bottom: 36px; animation: fadeSlideIn 0.4s ease forwards;
}
@keyframes fadeSlideIn {
    from { opacity:0; transform:translateY(10px); }
    to   { opacity:1; transform:translateY(0); }
}
.page-eyebrow {
    display: inline-flex; align-items: center; gap: 6px;
    background: var(--accent-glow); border: 1px solid rgba(99,102,241,0.25);
    color: var(--accent-bright); font-family: var(--font-mono);
    font-size: 10px; letter-spacing: 2px; text-transform: uppercase;
    padding: 4px 12px; border-radius: 20px; margin-bottom: 14px;
}
.page-title {
    font-size: 40px; font-weight: 900; color: var(--text-primary);
    letter-spacing: -1.5px; line-height: 1.05; margin: 0;
}
.page-desc { font-size: 14px; color: var(--text-muted); margin-top: 8px; }

.kpi-card {
    background: var(--bg-surface); border: 1px solid var(--border);
    border-radius: 16px; padding: 22px 24px 20px;
    position: relative; overflow: hidden;
    transition: border-color 0.2s, transform 0.2s, box-shadow 0.2s;
    animation: fadeSlideIn 0.4s ease forwards;
}
.kpi-card:hover { border-color: rgba(99,102,241,0.3); transform: translateY(-2px); box-shadow: 0 12px 40px rgba(0,0,0,0.3); }
.kpi-card::before {
    content:''; position:absolute; top:0; left:0; right:0; height:1px;
    background: linear-gradient(90deg, transparent, var(--accent), transparent); opacity:0.5;
}
.kpi-label {
    font-family: var(--font-mono); font-size: 10px; font-weight: 500;
    letter-spacing: 2px; text-transform: uppercase; color: var(--text-muted); margin-bottom: 10px;
}
.kpi-value { font-size: 30px; font-weight: 900; color: var(--text-primary); letter-spacing: -1px; line-height: 1; margin-bottom: 8px; }
.kpi-delta {
    font-family: var(--font-mono); font-size: 11px; color: var(--green); font-weight: 500;
    display: inline-flex; align-items: center; gap: 4px;
    background: rgba(16,185,129,0.08); padding: 2px 8px; border-radius: 20px;
}
.kpi-delta.neg { color: var(--red); background: rgba(244,63,94,0.08); }

.section-title {
    font-size: 17px; font-weight: 800; color: var(--text-primary);
    letter-spacing: -0.3px; margin: 28px 0 14px;
    display: flex; align-items: center; gap: 10px;
}
.section-title::after {
    content:''; flex:1; height:1px;
    background: linear-gradient(90deg, var(--border), transparent); margin-left:8px;
}

.divider { height:1px; background: linear-gradient(90deg, var(--border), transparent); margin: 32px 0; }

.insight-box {
    background: linear-gradient(135deg, rgba(99,102,241,0.06), rgba(34,211,238,0.03));
    border: 1px solid rgba(99,102,241,0.18); border-left: 3px solid var(--accent);
    border-radius: 10px; padding: 16px 20px; margin: 16px 0;
    font-size: 13px; color: var(--text-secondary); line-height: 1.7;
}
.insight-box strong { color: var(--accent-bright); }

.badge-row { margin-top: 14px; display: flex; flex-wrap: wrap; gap: 6px; }
.tech-badge {
    font-family: var(--font-mono); font-size: 10px; letter-spacing: 0.5px;
    background: rgba(34,211,238,0.06); border: 1px solid rgba(34,211,238,0.18);
    color: var(--cyan); padding: 4px 10px; border-radius: 6px;
}

.result-card {
    background: var(--bg-surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 16px 20px; margin-bottom: 10px;
    transition: border-color 0.2s, box-shadow 0.2s; animation: fadeSlideIn 0.3s ease forwards;
}
.result-card:hover { border-color: rgba(99,102,241,0.3); box-shadow: 0 8px 24px rgba(0,0,0,0.2); }
.result-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:10px; }
.result-stars { font-size: 18px; font-weight: 900; letter-spacing: 1px; }
.result-sim { font-family: var(--font-mono); font-size: 10px; letter-spacing: 1px; color: var(--text-muted); margin-left: 10px; }
.result-badge { font-family: var(--font-mono); font-size: 10px; background: var(--bg-elevated); border: 1px solid var(--border); color: var(--text-muted); padding: 3px 10px; border-radius: 6px; }
.result-text { font-size: 13px; color: var(--text-secondary); line-height: 1.65; }
.sim-bar { margin-top: 12px; background: var(--bg-base); border-radius: 4px; height: 3px; overflow: hidden; }
.sim-fill { height: 100%; border-radius: 4px; transition: width 0.6s cubic-bezier(0.4,0,0.2,1); }

.stat-pill {
    display: inline-flex; align-items: center; gap: 6px;
    background: var(--bg-elevated); border: 1px solid var(--border);
    border-radius: 8px; padding: 6px 14px; font-size: 12px; color: var(--text-secondary);
    margin-right: 8px; margin-bottom: 8px;
}
.stat-pill span { color: var(--text-primary); font-weight: 700; }

.empty-state { text-align: center; padding: 72px 0; }
.empty-icon { font-size: 52px; margin-bottom: 16px; opacity: 0.4; }
.empty-title { font-size: 20px; font-weight: 800; color: var(--text-muted); letter-spacing: -0.3px; margin-bottom: 8px; }
.empty-sub { font-size: 13px; color: var(--text-muted); opacity: 0.6; }

.stTabs [data-baseweb="tab-list"] {
    background: var(--bg-surface) !important; border-bottom: 1px solid var(--border) !important; gap: 4px; padding: 0 4px;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important; border: none !important;
    color: var(--text-muted) !important; font-family: var(--font-display) !important;
    font-weight: 600 !important; font-size: 13px !important;
    padding: 12px 18px !important; border-radius: 8px 8px 0 0 !important;
}
.stTabs [aria-selected="true"] {
    background: var(--accent-glow) !important; color: var(--accent-bright) !important;
    border-bottom: 2px solid var(--accent) !important;
}
.stTabs [data-baseweb="tab-panel"] { background: transparent !important; padding-top: 24px !important; }

.stSelectbox > div > div, .stMultiSelect > div > div {
    background: var(--bg-elevated) !important; border-color: var(--border) !important; border-radius: 10px !important;
}
.stTextInput > div > div > input {
    background: var(--bg-elevated) !important; border-color: var(--border) !important;
    border-radius: 10px !important; color: var(--text-primary) !important;
    font-family: var(--font-display) !important; font-size: 14px !important;
    padding: 12px 16px !important; transition: border-color 0.2s !important;
}
.stTextInput > div > div > input:focus {
    border-color: var(--accent) !important; box-shadow: 0 0 0 3px var(--accent-glow) !important;
}
.stMultiSelect span[data-baseweb="tag"] {
    background: var(--accent-glow) !important; border-color: rgba(99,102,241,0.3) !important;
}
.stSelectbox label, .stMultiSelect label, .stSlider label, .stTextInput label {
    font-family: var(--font-mono) !important; font-size: 10px !important;
    letter-spacing: 2px !important; text-transform: uppercase !important;
    color: var(--text-muted) !important; font-weight: 500 !important;
}
.stDataFrame { border: 1px solid var(--border) !important; border-radius: 12px !important; overflow: hidden !important; }
.stSpinner > div { border-top-color: var(--accent) !important; }

::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg-base); }
::-webkit-scrollbar-thumb { background: var(--bg-overlay); border-radius: 3px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# PLOTLY THEME
# ─────────────────────────────────────────────
C = {
    "accent":"#6366F1","cyan":"#22D3EE","green":"#10B981","amber":"#F59E0B",
    "red":"#F43F5E","orange":"#FB923C","purple":"#A78BFA",
    "bg":"#0E1118","surface":"#141720","grid":"rgba(255,255,255,0.05)",
    "text":"#64748B","white":"#F1F5F9",
}
PALETTE = ["#6366F1","#22D3EE","#10B981","#F59E0B","#A78BFA","#F43F5E","#06B6D4","#84CC16","#FB923C","#EC4899"]
SENTIMENT_COLORS = {"Muito Negativo":"#F43F5E","Negativo":"#FB923C","Neutro":"#F59E0B","Positivo":"#10B981","Muito Positivo":"#6366F1"}
SENTIMENT_MAP    = {1:"Muito Negativo",2:"Negativo",3:"Neutro",4:"Positivo",5:"Muito Positivo"}

def apply_theme(fig, height=420):
    fig.update_layout(
        paper_bgcolor=C["bg"], plot_bgcolor=C["bg"],
        font=dict(family="Cabinet Grotesk", color=C["text"], size=12),
        height=height, margin=dict(l=16,r=16,t=36,b=16),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=C["grid"], font=dict(size=11)),
        xaxis=dict(gridcolor=C["grid"], linecolor="rgba(0,0,0,0)", tickfont=dict(size=11), title_font=dict(size=11), zeroline=False),
        yaxis=dict(gridcolor=C["grid"], linecolor="rgba(0,0,0,0)", tickfont=dict(size=11), title_font=dict(size=11), zeroline=False),
    )
    return fig

# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))

@st.cache_data
def load_data():
    receita    = pd.read_csv(f"{DATA_DIR}/receita_mensal.csv")
    categorias = pd.read_csv(f"{DATA_DIR}/performance_categorias.csv")
    estados    = pd.read_csv(f"{DATA_DIR}/satisfacao_estados.csv")
    entrega    = pd.read_csv(f"{DATA_DIR}/tempo_entrega.csv")
    vendedores = pd.read_csv(f"{DATA_DIR}/performance_vendedores.csv")
    receita["periodo"] = receita["ano"].astype(str) + "-" + receita["mes"].astype(str).str.zfill(2)
    entrega["periodo"] = entrega["ano"].astype(str) + "-" + entrega["mes"].astype(str).str.zfill(2)
    return receita, categorias, estados, entrega, vendedores

@st.cache_data
def load_umap_data():
    path = os.path.join(DATA_DIR, "reviews_umap.csv")
    if not os.path.exists(path):
        raise FileNotFoundError("reviews_umap.csv não encontrado em scripts/exports/")
    df = pd.read_csv(path)
    df["texto_curto"] = df["review_text"].str[:90] + "..."
    return df

receita, categorias, estados, entrega, vendedores = load_data()

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
PAGES = [("📊","Visão Geral"),("📈","Análise Temporal"),("🏷️","Categorias & Produtos"),
         ("🚚","Logística & Entregas"),("🧑‍💼","Vendedores"),("🧠","Embeddings & NLP")]

with st.sidebar:
    st.markdown("""
    <div style='padding:28px 4px 24px;'>
        <div style='font-size:10px;font-family:var(--font-mono);letter-spacing:3px;
                    text-transform:uppercase;color:var(--text-muted);margin-bottom:6px;'>Pipeline ETL</div>
        <div style='font-size:26px;font-weight:900;color:var(--text-primary);letter-spacing:-1px;line-height:1.1;'>
            Olist<br>Analytics
        </div>
    </div>""", unsafe_allow_html=True)

    page = st.pills(
        "",
        options=[n for _, n in PAGES],
        format_func=lambda n: next(i for i, name in PAGES if name == n) + "  " + n,
        default="Visão Geral",
        label_visibility="collapsed"
    )
    st.markdown("<div class='divider' style='margin:20px 0'></div>", unsafe_allow_html=True)

    anos = sorted(receita["ano"].unique().tolist())
    anos_sel = st.multiselect("Filtrar por ano", anos, default=anos)

    st.markdown("<div class='divider' style='margin:16px 0'></div>", unsafe_allow_html=True)
    st.markdown("""
    <div style='font-family:var(--font-mono);font-size:10px;color:var(--text-muted);line-height:2.2;'>
        <div style='letter-spacing:2px;text-transform:uppercase;margin-bottom:6px;'>Stack</div>
        Python · PySpark · SQLite<br>Airflow · LocalStack<br>sentence-transformers · pgvector
        <div style='letter-spacing:2px;text-transform:uppercase;margin:10px 0 6px;'>Dataset</div>
        Olist E-Commerce BR<br>99.441 pedidos · 2016–2018
    </div>""", unsafe_allow_html=True)

receita_f = receita[receita["ano"].isin(anos_sel)]
entrega_f = entrega[entrega["ano"].isin(anos_sel)]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
total_receita = receita_f["receita"].sum()
total_pedidos = receita_f["pedidos"].sum()
ticket_medio  = receita_f["ticket_medio"].mean()
nota_media    = 4.01
taxa_atraso   = 7.8
prazo_medio   = entrega_f["prazo_medio"].mean() if len(entrega_f) > 0 else 0

def kpi(label, value, delta=None, neg=False):
    d = ""
    if delta:
        d = f'<div class="kpi-delta {"neg" if neg else ""}">{"↓" if neg else "↑"} {delta}</div>'
    return f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{value}</div>{d}</div>'

def section(title):
    return f'<div class="section-title">{title}</div>'

# ─────────────────────────────────────────────
# PAGE: VISÃO GERAL
# ─────────────────────────────────────────────
if page == "Visão Geral":
    st.markdown("""<div class="page-header">
        <div class="page-eyebrow">📊 Overview</div>
        <h1 class="page-title">Visão Geral do Negócio</h1>
        <p class="page-desc">Performance consolidada do e-commerce Olist — 2016 a 2018</p>
    </div>""", unsafe_allow_html=True)

    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(kpi("Receita Total", f"R$ {total_receita/1e6:.1f}M","2016–2018"), unsafe_allow_html=True)
    with c2: st.markdown(kpi("Pedidos Entregues", f"{int(total_pedidos):,}".replace(",","."), "período selecionado"), unsafe_allow_html=True)
    with c3: st.markdown(kpi("Ticket Médio", f"R$ {ticket_medio:.0f}","por pedido"), unsafe_allow_html=True)
    with c4: st.markdown(kpi("Nota Média", f"{nota_media:.2f} / 5","satisfação clientes"), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    col1,col2 = st.columns([3,2])

    with col1:
        st.markdown(section("Receita Acumulada por Mês"), unsafe_allow_html=True)
        rs = receita_f.sort_values("periodo").copy()
        rs["receita_acum"] = rs["receita"].cumsum()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=rs["periodo"],y=rs["receita_acum"],fill="tozeroy",
            fillcolor="rgba(99,102,241,0.10)",line=dict(color=C["accent"],width=2.5),
            mode="lines",name="Acumulada",hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"))
        fig.add_trace(go.Bar(x=rs["periodo"],y=rs["receita"],marker_color="rgba(34,211,238,0.2)",
            marker_line_width=0,name="Mensal",yaxis="y2",
            hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"))
        fig.update_layout(yaxis2=dict(overlaying="y",side="right",showgrid=False,gridcolor="rgba(0,0,0,0)"),hovermode="x unified")
        apply_theme(fig,380); st.plotly_chart(fig,use_container_width=True)

    with col2:
        st.markdown(section("Receita por Ano"), unsafe_allow_html=True)
        pa = receita_f.groupby("ano").agg(receita=("receita","sum")).reset_index()
        fig2 = go.Figure(go.Bar(x=pa["ano"].astype(str),y=pa["receita"],
            marker=dict(color=[C["accent"],C["cyan"],C["purple"]],line=dict(width=0)),
            text=[f"R$ {v/1e6:.1f}M" for v in pa["receita"]],
            textposition="outside",textfont=dict(color=C["white"],size=12),
            hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"))
        apply_theme(fig2,380); fig2.update_layout(showlegend=False); st.plotly_chart(fig2,use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    col3,col4,col5 = st.columns(3)

    with col3:
        st.markdown(section("Taxa de Entrega"), unsafe_allow_html=True)
        fig3 = go.Figure(go.Pie(values=[100-taxa_atraso,taxa_atraso],labels=["No Prazo","Atrasado"],
            hole=0.72,marker=dict(colors=[C["green"],C["red"]],line=dict(color=C["bg"],width=3)),
            textinfo="none",hovertemplate="<b>%{label}</b>: %{percent}<extra></extra>"))
        fig3.add_annotation(text=f"<b>{100-taxa_atraso:.1f}%</b><br><span style='font-size:10px'>No Prazo</span>",
            x=0.5,y=0.5,showarrow=False,font=dict(size=18,color=C["white"]))
        apply_theme(fig3,280); st.plotly_chart(fig3,use_container_width=True)

    with col4:
        st.markdown(section("Top 5 Estados"), unsafe_allow_html=True)
        top5 = estados.head(5)
        fig4 = go.Figure(go.Bar(x=top5["pedidos"],y=top5["estado"],orientation="h",
            marker=dict(color=PALETTE[:5],line=dict(width=0)),
            text=[f"{v:,}".replace(",",".") for v in top5["pedidos"]],
            textposition="outside",textfont=dict(color=C["white"],size=11),
            hovertemplate="<b>%{y}</b>: %{x:,}<extra></extra>"))
        apply_theme(fig4,280); fig4.update_layout(showlegend=False,yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig4,use_container_width=True)

    with col5:
        st.markdown(section("Distribuição de Avaliações"), unsafe_allow_html=True)
        fig5 = go.Figure(go.Bar(x=["★","★★","★★★","★★★★","★★★★★"],y=[2.8,3.0,8.7,19.9,65.6],
            marker=dict(color=[C["red"],C["orange"],C["amber"],C["cyan"],C["green"]],line=dict(width=0)),
            text=["2.8%","3.0%","8.7%","19.9%","65.6%"],textposition="outside",
            textfont=dict(color=C["white"],size=11),hovertemplate="<b>%{x}</b>: %{y}%<extra></extra>"))
        apply_theme(fig5,280); fig5.update_layout(showlegend=False); st.plotly_chart(fig5,use_container_width=True)

# ─────────────────────────────────────────────
# PAGE: ANÁLISE TEMPORAL
# ─────────────────────────────────────────────
elif page == "Análise Temporal":
    st.markdown("""<div class="page-header">
        <div class="page-eyebrow">📈 Temporal</div>
        <h1 class="page-title">Evolução ao Longo do Tempo</h1>
        <p class="page-desc">Tendências de receita, volume e ticket médio por período</p>
    </div>""", unsafe_allow_html=True)

    c1,c2,c3 = st.columns(3)
    with c1: st.markdown(kpi("Receita Total",f"R$ {total_receita/1e6:.1f}M"), unsafe_allow_html=True)
    with c2:
        melhor = receita_f.loc[receita_f["receita"].idxmax(),"periodo"] if len(receita_f)>0 else "—"
        st.markdown(kpi("Melhor Mês",melhor), unsafe_allow_html=True)
    with c3:
        if 2017 in anos_sel and 2018 in anos_sel:
            v17 = receita_f[receita_f["ano"]==2017]["receita"].sum()
            v18 = receita_f[receita_f["ano"]==2018]["receita"].sum()
            cres = (v18/v17-1)*100 if v17>0 else 0
        else: cres = 0
        st.markdown(kpi("Crescimento 17→18",f"+{cres:.0f}%"), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown(section("Receita e Volume de Pedidos"), unsafe_allow_html=True)
    fig = make_subplots(specs=[[{"secondary_y":True}]])
    rs = receita_f.sort_values("periodo")
    fig.add_trace(go.Bar(x=rs["periodo"],y=rs["receita"],name="Receita (R$)",
        marker=dict(color=C["accent"],opacity=0.85,line=dict(width=0)),
        hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"),secondary_y=False)
    fig.add_trace(go.Scatter(x=rs["periodo"],y=rs["pedidos"],name="Pedidos",
        line=dict(color=C["cyan"],width=2.5),mode="lines+markers",marker=dict(size=5),
        hovertemplate="<b>%{x}</b><br>%{y:,} pedidos<extra></extra>"),secondary_y=True)
    fig.update_layout(paper_bgcolor=C["bg"],plot_bgcolor=C["bg"],
        font=dict(family="Cabinet Grotesk",color=C["text"]),height=400,hovermode="x unified",
        margin=dict(l=16,r=16,t=20,b=16),legend=dict(bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(gridcolor=C["grid"],tickangle=45),
        yaxis=dict(gridcolor=C["grid"],title="Receita (R$)"),
        yaxis2=dict(gridcolor="rgba(0,0,0,0)",title="Pedidos"))
    st.plotly_chart(fig,use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    col1,col2 = st.columns(2)
    with col1:
        st.markdown(section("Ticket Médio por Mês"), unsafe_allow_html=True)
        fig2 = go.Figure(go.Scatter(x=rs["periodo"],y=rs["ticket_medio"],fill="tozeroy",
            fillcolor="rgba(167,139,250,0.08)",line=dict(color=C["purple"],width=2),
            mode="lines+markers",marker=dict(size=5,color=C["purple"]),
            hovertemplate="<b>%{x}</b><br>R$ %{y:.2f}<extra></extra>"))
        apply_theme(fig2,300); st.plotly_chart(fig2,use_container_width=True)
    with col2:
        st.markdown(section("Frete Médio por Mês"), unsafe_allow_html=True)
        fig3 = go.Figure(go.Scatter(x=rs["periodo"],y=rs["frete_medio"],fill="tozeroy",
            fillcolor="rgba(245,158,11,0.08)",line=dict(color=C["amber"],width=2),
            mode="lines+markers",marker=dict(size=5,color=C["amber"]),
            hovertemplate="<b>%{x}</b><br>R$ %{y:.2f}<extra></extra>"))
        apply_theme(fig3,300); st.plotly_chart(fig3,use_container_width=True)

    st.markdown(f"""<div class="insight-box"><strong>Insight:</strong> O volume cresceu consistentemente em 2017–2018,
        com pico em novembro 2017 (Black Friday). O ticket médio estável em
        <strong>R$ {receita_f['ticket_medio'].mean():.0f}</strong> indica crescimento por volume, não por preço.
    </div>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# PAGE: CATEGORIAS & PRODUTOS
# ─────────────────────────────────────────────
elif page == "Categorias & Produtos":
    st.markdown("""<div class="page-header">
        <div class="page-eyebrow">🏷️ Produtos</div>
        <h1 class="page-title">Categorias & Produtos</h1>
        <p class="page-desc">Receita, satisfação e performance por categoria</p>
    </div>""", unsafe_allow_html=True)

    top_n = st.slider("Número de categorias",5,20,10)
    cat_top = categorias.head(top_n)
    col1,col2 = st.columns(2)

    with col1:
        st.markdown(section("Receita por Categoria"), unsafe_allow_html=True)
        cs = cat_top.sort_values("receita")
        fig = go.Figure(go.Bar(x=cs["receita"],y=cs["categoria"],orientation="h",
            marker=dict(color=cs["receita"],
                colorscale=[[0,C["surface"]],[0.5,C["accent"]],[1,C["cyan"]]],line=dict(width=0)),
            text=[f"R$ {v/1e3:.0f}k" for v in cs["receita"]],
            textposition="outside",textfont=dict(color=C["white"],size=10),
            hovertemplate="<b>%{y}</b><br>R$ %{x:,.0f}<extra></extra>"))
        apply_theme(fig,420); fig.update_layout(showlegend=False); st.plotly_chart(fig,use_container_width=True)

    with col2:
        st.markdown(section("Satisfação vs Receita"), unsafe_allow_html=True)
        fig2 = px.scatter(cat_top,x="receita",y="nota_media",size="pedidos",color="ticket_medio",
            hover_name="categoria",color_continuous_scale=[[0,C["accent"]],[1,C["cyan"]]],size_max=40,
            labels={"receita":"Receita (R$)","nota_media":"Nota Média","ticket_medio":"Ticket Médio"})
        apply_theme(fig2,420); st.plotly_chart(fig2,use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown(section("Tabela Completa"), unsafe_allow_html=True)
    cd = categorias.copy()
    cd["receita"]      = cd["receita"].apply(lambda x: f"R$ {x:,.0f}")
    cd["ticket_medio"] = cd["ticket_medio"].apply(lambda x: f"R$ {x:.2f}")
    cd["nota_media"]   = cd["nota_media"].apply(lambda x: f"{x:.2f} ★")
    cd.columns = ["Categoria","Pedidos","Receita","Ticket Médio","Nota Média"]
    st.dataframe(cd,use_container_width=True,height=400)

# ─────────────────────────────────────────────
# PAGE: LOGÍSTICA & ENTREGAS
# ─────────────────────────────────────────────
elif page == "Logística & Entregas":
    st.markdown("""<div class="page-header">
        <div class="page-eyebrow">🚚 Logística</div>
        <h1 class="page-title">Logística & Entregas</h1>
        <p class="page-desc">Prazos, atrasos e performance regional</p>
    </div>""", unsafe_allow_html=True)

    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(kpi("Prazo Médio",f"{prazo_medio:.1f} dias"), unsafe_allow_html=True)
    with c2: st.markdown(kpi("Taxa de Atraso",f"{taxa_atraso}%",neg=True), unsafe_allow_html=True)
    with c3: st.markdown(kpi("Entregas Atrasadas","9.067",neg=True), unsafe_allow_html=True)
    with c4: st.markdown(kpi("No Prazo","106.652","97,0% do total"), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    col1,col2 = st.columns(2)
    es = entrega_f.sort_values("periodo")

    with col1:
        st.markdown(section("Prazo Médio por Mês"), unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=es["periodo"],y=es["prazo_medio"],fill="tozeroy",
            fillcolor="rgba(16,185,129,0.07)",line=dict(color=C["green"],width=2.5),
            mode="lines+markers",marker=dict(size=5),
            hovertemplate="<b>%{x}</b><br>%{y:.1f} dias<extra></extra>"))
        fig.add_hline(y=es["prazo_medio"].mean(),line_dash="dash",line_color=C["amber"],
            annotation_text=f"Média: {es['prazo_medio'].mean():.1f}d",annotation_font_color=C["amber"])
        apply_theme(fig,340); st.plotly_chart(fig,use_container_width=True)

    with col2:
        st.markdown(section("Taxa de Atraso por Mês"), unsafe_allow_html=True)
        fig2 = go.Figure(go.Bar(x=es["periodo"],y=es["pct_atraso"],
            marker=dict(color=es["pct_atraso"],
                colorscale=[[0,C["green"]],[0.5,C["amber"]],[1,C["red"]]],line=dict(width=0)),
            hovertemplate="<b>%{x}</b><br>%{y:.1f}%<extra></extra>"))
        apply_theme(fig2,340); fig2.update_layout(showlegend=False); st.plotly_chart(fig2,use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown(section("Performance por Estado"), unsafe_allow_html=True)
    metric = st.selectbox("Métrica",["pct_atraso","prazo_medio_dias","nota_media"],
        format_func=lambda x: {"pct_atraso":"Taxa de Atraso (%)","prazo_medio_dias":"Prazo Médio (dias)","nota_media":"Nota Média"}[x])

    col3,col4 = st.columns([2,1])
    with col3:
        es2 = estados.sort_values(metric,ascending=False)
        fig3 = go.Figure(go.Bar(x=es2["estado"],y=es2[metric],
            marker=dict(color=es2[metric],colorscale=[[0,C["green"]],[0.5,C["amber"]],[1,C["red"]]],line=dict(width=0)),
            hovertemplate="<b>%{x}</b><br>%{y:.1f}<extra></extra>"))
        apply_theme(fig3,320); fig3.update_layout(showlegend=False); st.plotly_chart(fig3,use_container_width=True)
    with col4:
        st.markdown(section("Ranking"), unsafe_allow_html=True)
        rank = estados[["estado",metric,"pedidos"]].sort_values(metric).head(8)
        rank.columns = ["Estado",metric,"Pedidos"]
        st.dataframe(rank,use_container_width=True,height=320)

# ─────────────────────────────────────────────
# PAGE: VENDEDORES
# ─────────────────────────────────────────────
elif page == "Vendedores":
    st.markdown("""<div class="page-header">
        <div class="page-eyebrow">🧑‍💼 Sellers</div>
        <h1 class="page-title">Performance dos Vendedores</h1>
        <p class="page-desc">Receita, satisfação e atrasos por seller</p>
    </div>""", unsafe_allow_html=True)

    c1,c2,c3 = st.columns(3)
    with c1: st.markdown(kpi("Vendedores Ativos","3.095","com ≥ 10 pedidos"), unsafe_allow_html=True)
    with c2: st.markdown(kpi("Receita Média/Seller",f"R$ {vendedores['receita'].mean():,.0f}"), unsafe_allow_html=True)
    with c3: st.markdown(kpi("Top Estado",vendedores.groupby('estado')['receita'].sum().idxmax()), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    col1,col2 = st.columns(2)

    with col1:
        st.markdown(section("Receita vs Satisfação"), unsafe_allow_html=True)
        fig = px.scatter(vendedores,x="receita",y="nota_media",size="pedidos",color="pct_atraso",
            hover_name="seller_id",color_continuous_scale=[[0,C["green"]],[0.5,C["amber"]],[1,C["red"]]],
            size_max=30,labels={"receita":"Receita (R$)","nota_media":"Nota Média","pct_atraso":"% Atraso"})
        apply_theme(fig,380); st.plotly_chart(fig,use_container_width=True)

    with col2:
        st.markdown(section("Receita por Estado"), unsafe_allow_html=True)
        pe = vendedores.groupby("estado").agg(receita=("receita","sum")).reset_index().sort_values("receita",ascending=False).head(10)
        fig2 = go.Figure(go.Bar(x=pe["estado"],y=pe["receita"],
            marker=dict(color=PALETTE[:len(pe)],line=dict(width=0)),
            text=[f"R$ {v/1e3:.0f}k" for v in pe["receita"]],
            textposition="outside",textfont=dict(color=C["white"],size=10),
            hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"))
        apply_theme(fig2,380); fig2.update_layout(showlegend=False); st.plotly_chart(fig2,use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown(section("Top 50 Vendedores"), unsafe_allow_html=True)
    vd = vendedores.copy()
    vd["receita"]      = vd["receita"].apply(lambda x: f"R$ {x:,.0f}")
    vd["ticket_medio"] = vd["ticket_medio"].apply(lambda x: f"R$ {x:.2f}")
    vd["nota_media"]   = vd["nota_media"].apply(lambda x: f"{x:.2f} ★")
    vd["pct_atraso"]   = vd["pct_atraso"].apply(lambda x: f"{x:.1f}%")
    vd["seller_id"]    = vd["seller_id"].str[:12] + "..."
    vd.columns = ["Seller ID","Estado","Cidade","Pedidos","Receita","Ticket Médio","Nota Média","% Atraso"]
    st.dataframe(vd,use_container_width=True,height=400)

# ─────────────────────────────────────────────
# PAGE: EMBEDDINGS & NLP
# ─────────────────────────────────────────────
elif page == "Embeddings & NLP":
    st.markdown("""<div class="page-header">
        <div class="page-eyebrow">🧠 NLP · Semântica</div>
        <h1 class="page-title">Embeddings &<br>Análise Semântica</h1>
        <p class="page-desc">Representação vetorial de reviews com sentence-transformers + PyTorch, redução dimensional com UMAP</p>
        <div class="badge-row">
            <span class="tech-badge">sentence-transformers</span>
            <span class="tech-badge">paraphrase-multilingual-MiniLM-L12-v2</span>
            <span class="tech-badge">PyTorch · GPU Colab</span>
            <span class="tech-badge">UMAP · 384D→2D</span>
            <span class="tech-badge">cosine similarity</span>
        </div>
    </div>""", unsafe_allow_html=True)

    try:
        df = load_umap_data()
    except FileNotFoundError as e:
        st.error(f"❌ {e}")
        st.stop()

    pct_pos = (df["review_score"] >= 4).mean() * 100
    pct_neg = (df["review_score"] <= 2).mean() * 100

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(kpi("Reviews Analisados", f"{len(df):,}".replace(",", ".")), unsafe_allow_html=True)
    with c2: st.markdown(kpi("Dimensão do Vetor", "384D", "MiniLM-L12-v2"), unsafe_allow_html=True)
    with c3: st.markdown(kpi("Reviews Positivos", f"{pct_pos:.1f}%", "nota ≥ 4"), unsafe_allow_html=True)
    with c4: st.markdown(kpi("Reviews Negativos", f"{pct_neg:.1f}%", "nota ≤ 2", neg=True), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["🗺️  Mapa Semântico UMAP", "🔍  Busca por Similaridade", "📊  Análise de Sentimento"])

    # ── TAB 1: UMAP ──────────────────────────────────────────────
    with tab1:
        st.markdown("""<div class="insight-box">
            <strong>Como ler este mapa:</strong> Cada ponto é um review vetorizado em <strong>384 dimensões</strong>
            pelo modelo <strong>sentence-transformers + PyTorch</strong> (GPU Colab T4).
            O UMAP comprime para 2D preservando vizinhança semântica — textos similares ficam agrupados.
        </div>""", unsafe_allow_html=True)

        col_ctrl, col_info = st.columns([1, 3])
        with col_ctrl:
            filtro_sent = st.multiselect(
                "Sentimentos",
                options=list(SENTIMENT_MAP.values()),
                default=list(SENTIMENT_MAP.values())
            )
            sample_size = st.slider("Pontos no gráfico", 500, len(df), min(2000, len(df)), step=250)

        df_filtered = df[df["sentimento"].isin(filtro_sent)]
        total_shown = min(sample_size, len(df_filtered))

        with col_info:
            st.markdown(f"""<div style="display:flex;flex-wrap:wrap;align-items:center;padding:16px 0;">
                <div class="stat-pill">Modelo <span>MiniLM-L12-v2</span></div>
                <div class="stat-pill">Vetores <span>{len(df):,} × 384</span></div>
                <div class="stat-pill">Pontos exibidos <span>{total_shown:,}</span></div>
                <div class="stat-pill">Redução <span>384D → 2D</span></div>
            </div>""", unsafe_allow_html=True)

        df_plot = df_filtered.sample(n=total_shown, random_state=42)
        fig_umap = px.scatter(
            df_plot, x="x", y="y", color="sentimento",
            color_discrete_map=SENTIMENT_COLORS,
            hover_data={"texto_curto": True, "review_score": True, "x": False, "y": False},
            labels={"sentimento": "Sentimento"}, opacity=0.75
        )
        fig_umap.update_traces(marker=dict(size=4))
        apply_theme(fig_umap, 580)
        fig_umap.update_layout(
            legend=dict(orientation="h", y=-0.06),
            xaxis=dict(showticklabels=False, title="", showgrid=False, zeroline=False),
            yaxis=dict(showticklabels=False, title="", showgrid=False, zeroline=False)
        )
        st.plotly_chart(fig_umap, use_container_width=True)

    # ── TAB 2: BUSCA SEMÂNTICA ────────────────────────────────────
    with tab2:
        st.markdown("""<div class="insight-box">
            <strong>Busca semântica:</strong> Compara sua query com os reviews via
            <strong>similaridade de cosseno (TF-IDF)</strong> — retorna reviews semanticamente próximos
            mesmo com variações de escrita.
        </div>""", unsafe_allow_html=True)

        col_q, col_k = st.columns([3, 1])
        with col_q:
            query = st.text_input("Busca semântica",
                placeholder="Ex: produto chegou quebrado · entrega muito rápida · péssimo atendimento…")
        with col_k:
            top_k = st.slider("Resultados", 3, 15, 7)

        if query:
            with st.spinner("Calculando similaridade..."):
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.metrics.pairwise import cosine_similarity as cos_sim

                corpus = df["review_text"].tolist()
                vectorizer = TfidfVectorizer(max_features=5000)
                tfidf_matrix = vectorizer.fit_transform(corpus)
                q_vec = vectorizer.transform([query])
                sims = cos_sim(q_vec, tfidf_matrix)[0]
                top_idx = sims.argsort()[::-1][:top_k]
                resultados = df.iloc[top_idx][["review_score", "review_text", "sentimento"]].copy()
                resultados["similaridade"] = sims[top_idx]

            avg_sim = resultados["similaridade"].mean() * 100
            st.markdown(f"""<div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;">
                <div style="font-size:18px;font-weight:900;color:var(--text-primary);">
                    {top_k} resultados · <em style="color:var(--accent-bright)">"{query}"</em>
                </div>
                <div class="stat-pill">Sim. média <span>{avg_sim:.1f}%</span></div>
            </div>""", unsafe_allow_html=True)

            col_res, col_dist = st.columns([2, 1])
            with col_res:
                for _, row in resultados.iterrows():
                    stars     = "★" * int(row["review_score"]) + "☆" * (5 - int(row["review_score"]))
                    sim_pct   = row["similaridade"] * 100
                    bar_color = SENTIMENT_COLORS.get(row["sentimento"], C["accent"])
                    st.markdown(f"""<div class="result-card">
                        <div class="result-header">
                            <div>
                                <span class="result-stars" style="color:{bar_color}">{stars}</span>
                                <span class="result-sim">SIM {sim_pct:.1f}%</span>
                            </div>
                            <div class="result-badge">{row["sentimento"]}</div>
                        </div>
                        <div class="result-text">{row["review_text"][:320]}{"…" if len(row["review_text"]) > 320 else ""}</div>
                        <div class="sim-bar"><div class="sim-fill" style="width:{sim_pct:.1f}%;background:{bar_color};"></div></div>
                    </div>""", unsafe_allow_html=True)

            with col_dist:
                st.markdown(section("Notas encontradas"), unsafe_allow_html=True)
                dr = resultados["review_score"].value_counts().sort_index()
                fig_d = go.Figure(go.Bar(
                    x=[f"★{i}" for i in dr.index], y=dr.values,
                    marker=dict(color=[SENTIMENT_COLORS[SENTIMENT_MAP[i]] for i in dr.index], line=dict(width=0)),
                    text=dr.values, textposition="outside", textfont=dict(color=C["white"])
                ))
                apply_theme(fig_d, 280)
                fig_d.update_layout(showlegend=False, margin=dict(t=16))
                st.plotly_chart(fig_d, use_container_width=True)
                st.markdown(kpi("Similaridade Média", f"{avg_sim:.1f}%"), unsafe_allow_html=True)
        else:
            st.markdown("""<div class="empty-state">
                <div class="empty-icon">🔍</div>
                <div class="empty-title">Busca semântica de reviews</div>
                <div class="empty-sub">Digite qualquer texto para encontrar reviews semanticamente similares</div>
            </div>""", unsafe_allow_html=True)

    # ── TAB 3: ANÁLISE DE SENTIMENTO ──────────────────────────────
    with tab3:
        STOPWORDS = {
            'a','o','e','de','da','do','em','na','no','para','com','que','se','um','uma',
            'os','as','por','foi','mais','mas','muito','bem','já','nao','não','me','meu',
            'minha','tudo','eu','ele','ela','isso','como','ao','dos','das','seu','sua',
            'ate','aqui','essa','este','esta','ser','ter','tem','tinha','era','quando',
            'depois','antes','ainda','produto','compra','recebi','chegou','veio','loja',
        }

        def top_palavras(textos, n=12):
            palavras = []
            for t in textos:
                tokens = re.findall(r'\b[a-záéíóúãõâêîôûç]{3,}\b', str(t).lower())
                palavras.extend([w for w in tokens if w not in STOPWORDS])
            return Counter(palavras).most_common(n)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(section("Distribuição de Notas"), unsafe_allow_html=True)
            dist = df["review_score"].value_counts().sort_index()
            fig_dist = go.Figure(go.Bar(
                x=[f"Nota {i}" for i in dist.index], y=dist.values,
                marker=dict(color=[SENTIMENT_COLORS[SENTIMENT_MAP[i]] for i in dist.index], line=dict(width=0)),
                text=dist.values, textposition="outside", textfont=dict(color=C["white"], size=12),
                hovertemplate="<b>%{x}</b>: %{y:,} reviews<extra></extra>"
            ))
            apply_theme(fig_dist, 320)
            fig_dist.update_layout(showlegend=False)
            st.plotly_chart(fig_dist, use_container_width=True)

        with col2:
            st.markdown(section("Proporção por Sentimento"), unsafe_allow_html=True)
            sc = df["sentimento"].value_counts().reindex(
                [s for s in ["Muito Positivo","Positivo","Neutro","Negativo","Muito Negativo"] if s in df["sentimento"].unique()])
            fig_pie = go.Figure(go.Pie(
                labels=sc.index, values=sc.values, hole=0.62,
                marker=dict(colors=[SENTIMENT_COLORS[s] for s in sc.index], line=dict(color=C["bg"], width=3)),
                textinfo="percent",
                hovertemplate="<b>%{label}</b>: %{value:,} (%{percent})<extra></extra>"
            ))
            fig_pie.add_annotation(
                text=f"<b>{df['review_score'].mean():.2f}</b><br><span style='font-size:10px'>nota média</span>",
                x=0.5, y=0.5, showarrow=False, font=dict(size=22, color=C["white"])
            )
            apply_theme(fig_pie, 320)
            st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
        st.markdown(section("Palavras Frequentes por Sentimento"), unsafe_allow_html=True)

        opts = ["Nota 1","Nota 2","Nota 3","Nota 4","Nota 5"]
        sent_sel = st.select_slider("Comparar notas", options=opts, value=("Nota 1", "Nota 5"))
        nota_a = int(sent_sel[0].split()[1])
        nota_b = int(sent_sel[1].split()[1])

        top_a = top_palavras(df[df["review_score"] == nota_a]["review_text"])
        top_b = top_palavras(df[df["review_score"] == nota_b]["review_text"])

        fig_w = go.Figure()
        fig_w.add_trace(go.Bar(
            name=f"Nota {nota_a} — {SENTIMENT_MAP[nota_a]}",
            x=[w[0] for w in top_a], y=[w[1] for w in top_a],
            marker=dict(color=SENTIMENT_COLORS[SENTIMENT_MAP[nota_a]], line=dict(width=0)),
            hovertemplate="<b>%{x}</b>: %{y}<extra></extra>"
        ))
        fig_w.add_trace(go.Bar(
            name=f"Nota {nota_b} — {SENTIMENT_MAP[nota_b]}",
            x=[w[0] for w in top_b], y=[w[1] for w in top_b],
            marker=dict(color=SENTIMENT_COLORS[SENTIMENT_MAP[nota_b]], line=dict(width=0)),
            hovertemplate="<b>%{x}</b>: %{y}<extra></extra>"
        ))
        fig_w.update_layout(barmode="group")
        apply_theme(fig_w, 380)
        st.plotly_chart(fig_w, use_container_width=True)

        st.markdown("""<div class="insight-box">
            <strong>Insight semântico:</strong> Reviews de <strong>nota 5</strong> concentram termos de velocidade
            e qualidade — ótimo, rápido, recomendo. Reviews de <strong>nota 1</strong> revelam falhas operacionais —
            prazo, errado, cancelado. O modelo sentence-transformers captura essa polaridade automaticamente.
        </div>""", unsafe_allow_html=True)