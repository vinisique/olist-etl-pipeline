import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

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
# CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* Background */
.stApp {
    background-color: #0D0F14;
    color: #E8E8E8;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #13161E;
    border-right: 1px solid #1E2330;
}
[data-testid="stSidebar"] * {
    color: #C8CDD8 !important;
}

/* Hide default header */
header[data-testid="stHeader"] {
    background: transparent;
}

/* KPI Cards */
.kpi-card {
    background: linear-gradient(135deg, #13161E 0%, #1A1E2A 100%);
    border: 1px solid #1E2330;
    border-radius: 16px;
    padding: 24px 28px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.3s;
}
.kpi-card:hover {
    border-color: #3D5AFE;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #3D5AFE, #00E5FF);
}
.kpi-label {
    font-family: 'DM Sans', sans-serif;
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #6B7280;
    margin-bottom: 10px;
}
.kpi-value {
    font-family: 'Syne', sans-serif;
    font-size: 32px;
    font-weight: 800;
    color: #FFFFFF;
    line-height: 1;
    margin-bottom: 6px;
}
.kpi-delta {
    font-size: 12px;
    color: #10B981;
    font-weight: 500;
}
.kpi-delta.negative { color: #EF4444; }

/* Section headers */
.section-title {
    font-family: 'Syne', sans-serif;
    font-size: 20px;
    font-weight: 700;
    color: #FFFFFF;
    margin: 32px 0 16px 0;
    letter-spacing: -0.3px;
}
.section-subtitle {
    font-size: 13px;
    color: #6B7280;
    margin-top: -12px;
    margin-bottom: 20px;
}

/* Page title */
.page-header {
    padding: 32px 0 24px 0;
    border-bottom: 1px solid #1E2330;
    margin-bottom: 32px;
}
.page-title {
    font-family: 'Syne', sans-serif;
    font-size: 36px;
    font-weight: 800;
    color: #FFFFFF;
    letter-spacing: -1px;
    line-height: 1.1;
}
.page-tag {
    display: inline-block;
    background: rgba(61,90,254,0.15);
    border: 1px solid rgba(61,90,254,0.3);
    color: #7B8CFF;
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    padding: 4px 12px;
    border-radius: 20px;
    margin-bottom: 12px;
    font-weight: 500;
}

/* Divider */
.divider {
    height: 1px;
    background: linear-gradient(90deg, #1E2330, transparent);
    margin: 32px 0;
}

/* Insight box */
.insight-box {
    background: rgba(61,90,254,0.08);
    border: 1px solid rgba(61,90,254,0.2);
    border-left: 3px solid #3D5AFE;
    border-radius: 8px;
    padding: 16px 20px;
    margin: 16px 0;
    font-size: 13px;
    color: #A0A8C0;
    line-height: 1.6;
}
.insight-box strong { color: #7B8CFF; }

/* Plotly chart container */
.chart-container {
    background: #13161E;
    border: 1px solid #1E2330;
    border-radius: 16px;
    padding: 20px;
}

/* Scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #0D0F14; }
::-webkit-scrollbar-thumb { background: #1E2330; border-radius: 3px; }

/* Streamlit elements override */
.stSelectbox label, .stMultiSelect label, .stSlider label {
    color: #6B7280 !important;
    font-size: 12px !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
}
div[data-baseweb="select"] {
    background: #13161E !important;
    border-color: #1E2330 !important;
}
.stDataFrame { border: 1px solid #1E2330; border-radius: 12px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# PLOTLY THEME
# ─────────────────────────────────────────────
COLORS = {
    "blue":    "#3D5AFE",
    "cyan":    "#00E5FF",
    "green":   "#10B981",
    "orange":  "#F59E0B",
    "red":     "#EF4444",
    "purple":  "#8B5CF6",
    "bg":      "#13161E",
    "grid":    "#1E2330",
    "text":    "#9CA3AF",
    "white":   "#FFFFFF",
}

PALETTE = [
    "#3D5AFE","#00E5FF","#10B981","#F59E0B",
    "#8B5CF6","#EF4444","#06B6D4","#84CC16",
    "#F97316","#EC4899","#6366F1","#14B8A6",
]

def apply_theme(fig, height=420):
    fig.update_layout(
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(family="DM Sans", color=COLORS["text"], size=12),
        height=height,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=COLORS["grid"],
            font=dict(size=11)
        ),
        xaxis=dict(
            gridcolor=COLORS["grid"],
            linecolor=COLORS["grid"],
            tickfont=dict(size=11),
            title_font=dict(size=11)
        ),
        yaxis=dict(
            gridcolor=COLORS["grid"],
            linecolor=COLORS["grid"],
            tickfont=dict(size=11),
            title_font=dict(size=11)
        ),
    )
    return fig

# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

@st.cache_data
def load_data():
    receita     = pd.read_csv(f"{DATA_DIR}/receita_mensal.csv")
    categorias  = pd.read_csv(f"{DATA_DIR}/performance_categorias.csv")
    estados     = pd.read_csv(f"{DATA_DIR}/satisfacao_estados.csv")
    entrega     = pd.read_csv(f"{DATA_DIR}/tempo_entrega.csv")
    vendedores  = pd.read_csv(f"{DATA_DIR}/performance_vendedores.csv")

    receita["periodo"] = (
        receita["ano"].astype(str) + "-" +
        receita["mes"].astype(str).str.zfill(2)
    )
    entrega["periodo"] = (
        entrega["ano"].astype(str) + "-" +
        entrega["mes"].astype(str).str.zfill(2)
    )
    return receita, categorias, estados, entrega, vendedores

receita, categorias, estados, entrega, vendedores = load_data()

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding: 20px 0 32px 0;'>
        <div style='font-family: Syne, sans-serif; font-size: 22px;
                    font-weight: 800; color: white; letter-spacing: -0.5px;'>
            Olist Analytics
        </div>
        <div style='font-size: 11px; color: #4B5563; letter-spacing: 1px;
                    text-transform: uppercase; margin-top: 4px;'>
            Pipeline ETL Dashboard
        </div>
    </div>
    """, unsafe_allow_html=True)

    page = st.selectbox(
        "NAVEGAÇÃO",
        ["Visão Geral", "Análise Temporal", "Categorias & Produtos", "Logística & Entregas", "Vendedores"],
        label_visibility="visible"
    )

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    anos = sorted(receita["ano"].unique().tolist())
    anos_sel = st.multiselect("FILTRAR POR ANO", anos, default=anos)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    st.markdown("""
    <div style='font-size: 11px; color: #374151; line-height: 1.8;'>
        <div style='color: #4B5563; letter-spacing: 1px; text-transform: uppercase;
                    font-size: 10px; margin-bottom: 8px;'>Stack</div>
        Python · PySpark · SQLite<br>
        Airflow · LocalStack · Streamlit<br>
        <br>
        <div style='color: #4B5563; letter-spacing: 1px; text-transform: uppercase;
                    font-size: 10px; margin-bottom: 8px;'>Dataset</div>
        Olist Brazilian E-Commerce<br>
        99.441 pedidos · 2016–2018
    </div>
    """, unsafe_allow_html=True)

# Filter data by year
receita_f  = receita[receita["ano"].isin(anos_sel)]
entrega_f  = entrega[entrega["ano"].isin(anos_sel)]

# ─────────────────────────────────────────────
# GLOBAL KPIs
# ─────────────────────────────────────────────
total_receita  = receita_f["receita"].sum()
total_pedidos  = receita_f["pedidos"].sum()
ticket_medio   = receita_f["ticket_medio"].mean()
nota_media     = 4.01
taxa_atraso    = 7.8
prazo_medio    = entrega_f["prazo_medio"].mean() if len(entrega_f) > 0 else 0

def kpi(label, value, delta=None, delta_neg=False):
    delta_html = ""
    if delta:
        cls = "negative" if delta_neg else ""
        icon = "↓" if delta_neg else "↑"
        delta_html = f'<div class="kpi-delta {cls}">{icon} {delta}</div>'
    return f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """

# ─────────────────────────────────────────────
# PAGE: VISÃO GERAL
# ─────────────────────────────────────────────
if page == "Visão Geral":
    st.markdown("""
    <div class="page-header">
        <div class="page-tag">Overview</div>
        <div class="page-title">Visão Geral do Negócio</div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("Receita Total", f"R$ {total_receita/1e6:.1f}M", "2016–2018"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("Pedidos Entregues", f"{total_pedidos:,.0f}".replace(",", "."), "período selecionado"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("Ticket Médio", f"R$ {ticket_medio:.0f}", "por item"), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi("Nota Média", f"{nota_media:.2f} / 5", "satisfação clientes"), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("<div class='section-title'>Receita Acumulada por Mês</div>", unsafe_allow_html=True)
        receita_sorted = receita_f.sort_values("periodo")
        receita_sorted["receita_acum"] = receita_sorted["receita"].cumsum()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=receita_sorted["periodo"],
            y=receita_sorted["receita_acum"],
            fill="tozeroy",
            fillcolor="rgba(61,90,254,0.12)",
            line=dict(color=COLORS["blue"], width=2.5),
            mode="lines",
            name="Receita Acumulada",
            hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"
        ))
        fig.add_trace(go.Bar(
            x=receita_sorted["periodo"],
            y=receita_sorted["receita"],
            marker_color="rgba(0,229,255,0.25)",
            marker_line_width=0,
            name="Receita Mensal",
            hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>",
            yaxis="y2"
        ))
        fig.update_layout(
            yaxis2=dict(overlaying="y", side="right",
                        gridcolor="transparent", showgrid=False),
            hovermode="x unified"
        )
        apply_theme(fig, height=380)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("<div class='section-title'>Volume por Ano</div>", unsafe_allow_html=True)
        por_ano = receita_f.groupby("ano").agg(
            pedidos=("pedidos","sum"),
            receita=("receita","sum")
        ).reset_index()

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=por_ano["ano"].astype(str),
            y=por_ano["receita"],
            marker=dict(
                color=[COLORS["blue"], COLORS["cyan"], COLORS["purple"]],
                line=dict(width=0)
            ),
            text=[f"R$ {v/1e6:.1f}M" for v in por_ano["receita"]],
            textposition="outside",
            textfont=dict(color=COLORS["white"], size=12),
            hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"
        ))
        apply_theme(fig2, height=380)
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    col3, col4, col5 = st.columns(3)

    with col3:
        st.markdown("<div class='section-title'>Taxa de Atraso</div>", unsafe_allow_html=True)
        fig3 = go.Figure(go.Pie(
            values=[100 - taxa_atraso, taxa_atraso],
            labels=["No Prazo", "Atrasado"],
            hole=0.72,
            marker=dict(colors=[COLORS["green"], COLORS["red"]],
                        line=dict(color=COLORS["bg"], width=3)),
            textinfo="none",
            hovertemplate="<b>%{label}</b>: %{percent}<extra></extra>"
        ))
        fig3.add_annotation(
            text=f"<b>{100-taxa_atraso:.1f}%</b><br><span style='font-size:10px'>No Prazo</span>",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=18, color=COLORS["white"])
        )
        apply_theme(fig3, height=280)
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.markdown("<div class='section-title'>Top 5 Estados</div>", unsafe_allow_html=True)
        top5 = estados.head(5)
        fig4 = go.Figure(go.Bar(
            x=top5["pedidos"],
            y=top5["estado"],
            orientation="h",
            marker=dict(
                color=PALETTE[:5],
                line=dict(width=0)
            ),
            text=[f"{v:,}".replace(",",".") for v in top5["pedidos"]],
            textposition="outside",
            textfont=dict(color=COLORS["white"], size=11),
            hovertemplate="<b>%{y}</b>: %{x:,} pedidos<extra></extra>"
        ))
        apply_theme(fig4, height=280)
        fig4.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig4, use_container_width=True)

    with col5:
        st.markdown("<div class='section-title'>Avaliações</div>", unsafe_allow_html=True)
        notas = [1,2,3,4,5]
        pcts  = [2.8, 3.0, 8.7, 19.9, 65.6]
        cores = [COLORS["red"], COLORS["orange"], COLORS["orange"],
                 COLORS["cyan"], COLORS["green"]]
        fig5 = go.Figure(go.Bar(
            x=[f"{'★'*n}" for n in notas],
            y=pcts,
            marker=dict(color=cores, line=dict(width=0)),
            text=[f"{p}%" for p in pcts],
            textposition="outside",
            textfont=dict(color=COLORS["white"], size=11),
            hovertemplate="<b>%{x}</b>: %{y}%<extra></extra>"
        ))
        apply_theme(fig5, height=280)
        fig5.update_layout(showlegend=False)
        st.plotly_chart(fig5, use_container_width=True)

# ─────────────────────────────────────────────
# PAGE: ANÁLISE TEMPORAL
# ─────────────────────────────────────────────
elif page == "Análise Temporal":
    st.markdown("""
    <div class="page-header">
        <div class="page-tag">Temporal</div>
        <div class="page-title">Evolução ao Longo do Tempo</div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(kpi("Receita Total", f"R$ {total_receita/1e6:.1f}M"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("Melhor Mês", receita_f.loc[receita_f["receita"].idxmax(), "periodo"] if len(receita_f) > 0 else "—"), unsafe_allow_html=True)
    with c3:
        crescimento = ((receita_f[receita_f["ano"]==2018]["receita"].sum() /
                        receita_f[receita_f["ano"]==2017]["receita"].sum()) - 1) * 100 if 2017 in anos_sel and 2018 in anos_sel else 0
        st.markdown(kpi("Crescimento 2017→2018", f"+{crescimento:.0f}%"), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    st.markdown("<div class='section-title'>Receita e Volume de Pedidos por Mês</div>", unsafe_allow_html=True)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    receita_s = receita_f.sort_values("periodo")

    fig.add_trace(go.Bar(
        x=receita_s["periodo"], y=receita_s["receita"],
        name="Receita (R$)",
        marker=dict(color=COLORS["blue"], opacity=0.85, line=dict(width=0)),
        hovertemplate="<b>%{x}</b><br>Receita: R$ %{y:,.0f}<extra></extra>"
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=receita_s["periodo"], y=receita_s["pedidos"],
        name="Pedidos",
        line=dict(color=COLORS["cyan"], width=2.5),
        mode="lines+markers",
        marker=dict(size=5),
        hovertemplate="<b>%{x}</b><br>Pedidos: %{y:,}<extra></extra>"
    ), secondary_y=True)

    fig.update_layout(
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
        font=dict(family="DM Sans", color=COLORS["text"]),
        height=400, hovermode="x unified",
        margin=dict(l=20, r=20, t=20, b=20),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(gridcolor=COLORS["grid"], tickangle=45),
        yaxis=dict(gridcolor=COLORS["grid"], title="Receita (R$)"),
        yaxis2=dict(gridcolor="transparent", title="Pedidos")
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-title'>Ticket Médio por Mês</div>", unsafe_allow_html=True)
        fig2 = go.Figure(go.Scatter(
            x=receita_s["periodo"], y=receita_s["ticket_medio"],
            fill="tozeroy",
            fillcolor="rgba(139,92,246,0.1)",
            line=dict(color=COLORS["purple"], width=2),
            mode="lines+markers",
            marker=dict(size=5, color=COLORS["purple"]),
            hovertemplate="<b>%{x}</b><br>Ticket: R$ %{y:.2f}<extra></extra>"
        ))
        apply_theme(fig2, height=300)
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        st.markdown("<div class='section-title'>Frete Médio por Mês</div>", unsafe_allow_html=True)
        fig3 = go.Figure(go.Scatter(
            x=receita_s["periodo"], y=receita_s["frete_medio"],
            fill="tozeroy",
            fillcolor="rgba(245,158,11,0.1)",
            line=dict(color=COLORS["orange"], width=2),
            mode="lines+markers",
            marker=dict(size=5, color=COLORS["orange"]),
            hovertemplate="<b>%{x}</b><br>Frete: R$ %{y:.2f}<extra></extra>"
        ))
        apply_theme(fig3, height=300)
        st.plotly_chart(fig3, use_container_width=True)

    st.markdown(f"""
    <div class="insight-box">
        <strong>Insight:</strong> O volume de pedidos cresceu de forma consistente entre 2017 e 2018,
        com pico em novembro 2017 (Black Friday). O ticket médio se manteve estável em torno de
        <strong>R$ {receita_f['ticket_medio'].mean():.0f}</strong>, indicando crescimento orgânico
        por volume e não por aumento de preço.
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
# PAGE: CATEGORIAS & PRODUTOS
# ─────────────────────────────────────────────
elif page == "Categorias & Produtos":
    st.markdown("""
    <div class="page-header">
        <div class="page-tag">Produtos</div>
        <div class="page-title">Categorias & Produtos</div>
    </div>
    """, unsafe_allow_html=True)

    top_n = st.slider("Número de categorias", 5, 20, 10)
    cat_top = categorias.head(top_n)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-title'>Receita por Categoria</div>", unsafe_allow_html=True)
        cat_sorted = cat_top.sort_values("receita")
        fig = go.Figure(go.Bar(
            x=cat_sorted["receita"],
            y=cat_sorted["categoria"],
            orientation="h",
            marker=dict(
                color=cat_sorted["receita"],
                colorscale=[[0,"#1E2330"],[0.5,COLORS["blue"]],[1,COLORS["cyan"]]],
                line=dict(width=0)
            ),
            text=[f"R$ {v/1e3:.0f}k" for v in cat_sorted["receita"]],
            textposition="outside",
            textfont=dict(color=COLORS["white"], size=10),
            hovertemplate="<b>%{y}</b><br>Receita: R$ %{x:,.0f}<extra></extra>"
        ))
        apply_theme(fig, height=420)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("<div class='section-title'>Satisfação vs Receita</div>", unsafe_allow_html=True)
        fig2 = px.scatter(
            cat_top,
            x="receita", y="nota_media",
            size="pedidos", color="ticket_medio",
            hover_name="categoria",
            color_continuous_scale=[[0,COLORS["blue"]],[1,COLORS["cyan"]]],
            size_max=40,
            labels={"receita": "Receita (R$)", "nota_media": "Nota Média",
                    "ticket_medio": "Ticket Médio"}
        )
        apply_theme(fig2, height=420)
        fig2.update_traces(
            hovertemplate="<b>%{hovertext}</b><br>Receita: R$ %{x:,.0f}<br>Nota: %{y:.2f}<extra></extra>"
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    st.markdown("<div class='section-title'>Tabela Completa de Categorias</div>", unsafe_allow_html=True)
    cat_display = categorias.copy()
    cat_display["receita"] = cat_display["receita"].apply(lambda x: f"R$ {x:,.0f}")
    cat_display["ticket_medio"] = cat_display["ticket_medio"].apply(lambda x: f"R$ {x:.2f}")
    cat_display["nota_media"] = cat_display["nota_media"].apply(lambda x: f"{x:.2f} ★")
    cat_display.columns = ["Categoria","Pedidos","Receita","Ticket Médio","Nota Média"]
    st.dataframe(cat_display, use_container_width=True, height=400)

# ─────────────────────────────────────────────
# PAGE: LOGÍSTICA & ENTREGAS
# ─────────────────────────────────────────────
elif page == "Logística & Entregas":
    st.markdown("""
    <div class="page-header">
        <div class="page-tag">Logística</div>
        <div class="page-title">Logística & Entregas</div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(kpi("Prazo Médio", f"{prazo_medio:.1f} dias"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("Taxa de Atraso", f"{taxa_atraso}%", delta_neg=True), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("Entregas Atrasadas", "9.067", delta_neg=True), unsafe_allow_html=True)
    with c4:
        st.markdown(kpi("No Prazo", "106.652", "97,0% do total"), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-title'>Prazo Médio de Entrega por Mês</div>", unsafe_allow_html=True)
        entrega_s = entrega_f.sort_values("periodo")
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=entrega_s["periodo"], y=entrega_s["prazo_medio"],
            fill="tozeroy",
            fillcolor="rgba(16,185,129,0.08)",
            line=dict(color=COLORS["green"], width=2.5),
            mode="lines+markers",
            marker=dict(size=5),
            name="Prazo Médio",
            hovertemplate="<b>%{x}</b><br>%{y:.1f} dias<extra></extra>"
        ))
        fig.add_hline(
            y=entrega_s["prazo_medio"].mean(),
            line_dash="dash", line_color=COLORS["orange"],
            annotation_text=f"Média: {entrega_s['prazo_medio'].mean():.1f}d",
            annotation_font_color=COLORS["orange"]
        )
        apply_theme(fig, height=340)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("<div class='section-title'>Taxa de Atraso por Mês (%)</div>", unsafe_allow_html=True)
        fig2 = go.Figure(go.Bar(
            x=entrega_s["periodo"],
            y=entrega_s["pct_atraso"],
            marker=dict(
                color=entrega_s["pct_atraso"],
                colorscale=[[0,COLORS["green"]],[0.5,COLORS["orange"]],[1,COLORS["red"]]],
                line=dict(width=0)
            ),
            hovertemplate="<b>%{x}</b><br>Atraso: %{y:.1f}%<extra></extra>"
        ))
        apply_theme(fig2, height=340)
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    st.markdown("<div class='section-title'>Performance por Estado</div>", unsafe_allow_html=True)

    metric = st.selectbox(
        "Métrica",
        ["pct_atraso", "prazo_medio_dias", "nota_media"],
        format_func=lambda x: {
            "pct_atraso": "Taxa de Atraso (%)",
            "prazo_medio_dias": "Prazo Médio (dias)",
            "nota_media": "Nota Média"
        }[x]
    )

    col3, col4 = st.columns([2, 1])

    with col3:
        estados_s = estados.sort_values(metric, ascending=False)
        fig3 = go.Figure(go.Bar(
            x=estados_s["estado"],
            y=estados_s[metric],
            marker=dict(
                color=estados_s[metric],
                colorscale=[[0,COLORS["green"]],[0.5,COLORS["orange"]],[1,COLORS["red"]]],
                line=dict(width=0)
            ),
            hovertemplate="<b>%{x}</b><br>%{y:.1f}<extra></extra>"
        ))
        apply_theme(fig3, height=320)
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.markdown("<div class='section-title'>Ranking</div>", unsafe_allow_html=True)
        rank = estados[["estado", metric, "pedidos"]].sort_values(metric).head(8)
        rank.columns = ["Estado", metric, "Pedidos"]
        st.dataframe(rank, use_container_width=True, height=320)

# ─────────────────────────────────────────────
# PAGE: VENDEDORES
# ─────────────────────────────────────────────
elif page == "Vendedores":
    st.markdown("""
    <div class="page-header">
        <div class="page-tag">Sellers</div>
        <div class="page-title">Performance dos Vendedores</div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(kpi("Vendedores Ativos", "3.095", "com ≥ 10 pedidos"), unsafe_allow_html=True)
    with c2:
        st.markdown(kpi("Receita Média/Seller", f"R$ {vendedores['receita'].mean():,.0f}"), unsafe_allow_html=True)
    with c3:
        st.markdown(kpi("Top Estado", vendedores.groupby('estado')['receita'].sum().idxmax()), unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<div class='section-title'>Receita vs Satisfação</div>", unsafe_allow_html=True)
        fig = px.scatter(
            vendedores,
            x="receita", y="nota_media",
            size="pedidos", color="pct_atraso",
            hover_name="seller_id",
            color_continuous_scale=[[0,COLORS["green"]],[0.5,COLORS["orange"]],[1,COLORS["red"]]],
            size_max=30,
            labels={"receita":"Receita (R$)","nota_media":"Nota Média","pct_atraso":"% Atraso"}
        )
        apply_theme(fig, height=380)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("<div class='section-title'>Receita por Estado</div>", unsafe_allow_html=True)
        por_estado = vendedores.groupby("estado").agg(
            receita=("receita","sum"),
            vendedores=("seller_id","count")
        ).reset_index().sort_values("receita", ascending=False).head(10)

        fig2 = go.Figure(go.Bar(
            x=por_estado["estado"],
            y=por_estado["receita"],
            marker=dict(
                color=PALETTE[:len(por_estado)],
                line=dict(width=0)
            ),
            text=[f"R$ {v/1e3:.0f}k" for v in por_estado["receita"]],
            textposition="outside",
            textfont=dict(color=COLORS["white"], size=10),
            hovertemplate="<b>%{x}</b><br>R$ %{y:,.0f}<extra></extra>"
        ))
        apply_theme(fig2, height=380)
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown("<div class='section-title'>Top 50 Vendedores</div>", unsafe_allow_html=True)

    v_display = vendedores.copy()
    v_display["receita"]      = v_display["receita"].apply(lambda x: f"R$ {x:,.0f}")
    v_display["ticket_medio"] = v_display["ticket_medio"].apply(lambda x: f"R$ {x:.2f}")
    v_display["nota_media"]   = v_display["nota_media"].apply(lambda x: f"{x:.2f} ★")
    v_display["pct_atraso"]   = v_display["pct_atraso"].apply(lambda x: f"{x:.1f}%")
    v_display["seller_id"]    = v_display["seller_id"].str[:12] + "..."
    v_display.columns = ["Seller ID","Estado","Cidade","Pedidos","Receita","Ticket Médio","Nota Média","% Atraso"]
    st.dataframe(v_display, use_container_width=True, height=400)
