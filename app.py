import os
import requests
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import threading
import time
import logging
from flask import Flask, jsonify

# Import custom modules
from libs import KommoAPI, SupabaseClient, SyncManager
from data_processor import process_data
from visualizations import create_heatmap, create_conversion_funnel, create_points_breakdown_chart

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

st.set_page_config(page_title="Dicasa - Dashboard de Desempenho",
                   layout="wide")


@st.cache_resource
def init_supabase_client():
    return SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                          key=os.getenv("VITE_SUPABASE_ANON_KEY"))


supabase = init_supabase_client()


def background_data_loader():
    """
    This function runs in the background to continuously monitor Kommo API
    for changes and update the database accordingly
    """
    try:
        kommo_api = KommoAPI(api_url=os.getenv(
            "KOMMO_API_URL", "https://dicasaindaial.kommo.com/api/v4"),
                             access_token=os.getenv("ACCESS_TOKEN_KOMMO"))

        supabase = SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                                  key=os.getenv("VITE_SUPABASE_ANON_KEY"))

        sync_manager = SyncManager(kommo_api, supabase)
        last_sync_time = None

        # Initial check for broker_points data
        existing = supabase.client.table("broker_points").select("*").limit(
            1).execute()
        if not existing.data:
            logger.info("Inicializando broker_points...")
            supabase.initialize_broker_points()

        while True:
            try:
                brokers = kommo_api.get_users()
                leads = kommo_api.get_leads()
                activities = kommo_api.get_activities()

                current_time = datetime.now()

                if not last_sync_time or (
                        current_time - last_sync_time).total_seconds() > 300:
                    logger.info(
                        "Iniciando sincronização e atualização de pontos...")

                    if not brokers.empty and not leads.empty and not activities.empty:
                        sync_manager.sync_from_cache(brokers, leads,
                                                     activities)
                        auto_update_broker_points()

                    last_sync_time = current_time

            except Exception as e:
                logger.error(f"Error in background sync: {str(e)}")
                time.sleep(5)

    except Exception as e:
        logger.error(f"Failed to initialize background sync: {str(e)}")


# # Start the background thread when app starts
@st.cache_resource
def start_background_thread():
    thread = threading.Thread(target=background_data_loader, daemon=True)
    thread.start()
    return "Background thread started"


# Start the background thread
thread_status = start_background_thread()

# Custom CSS
st.markdown("""
<style>
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    h1 {
        font-size: 2.2rem !important;
        font-weight: 600 !important;
        color: #1E3A8A !important;
    }
    h2 {
        font-size: 1.8rem !important;
        font-weight: 500 !important;
        color: #2563EB !important;
    }
    .stMetric {
        background-color: #F3F4F6;
        padding: 10px 15px;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
    }
    .stMetric label {
        font-weight: 600 !important;
        color: #4B5563 !important;
    }
    .stMetric .css-1xarl3l {
        font-size: 2.2rem !important;
        font-weight: 700 !important;
        color: #1E40AF !important;
    }
    .streamlit-card {
        border-radius: 12px !important;
        border: 1px solid #E5E7EB !important;
    }
    .block-container {
        max-width: 1200px;
        padding-top: 1rem;
        padding-right: 1.5rem;
        padding-left: 1.5rem;
        padding-bottom: 1rem;
    }
    .ranking-card {
        background-color: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
        transition: all 0.3s ease;
    }
    .ranking-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.08);
    }
    .rank-number {
        font-size: 24px;
        font-weight: bold;
        color: #1E3A8A;
    }
    .broker-name {
        font-size: 18px;
        font-weight: 500;
        color: #1F2937;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 150px;
    }
    .points-label {
        font-size: 14px;
        color: #6B7280;
    }
    .points-value {
        font-size: 20px;
        font-weight: 600;
        color: #2563EB;
    }
    .card-title {
        font-weight: 600;
        color: #1F2937;
        font-size: 18px;
        margin-bottom: 10px;
    }
    .stat-card {
        background-color: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
        margin-bottom: 20px;
    }
    .tabs-container .stTabs [data-baseweb="tab-list"] {
        background-color: #F3F4F6;
        border-radius: 8px;
        padding: 5px;
    }
    .tabs-container .stTabs [data-baseweb="tab"] {
        padding: 10px 16px;
        border-radius: 8px;
    }
    .tabs-container .stTabs [aria-selected="true"] {
        background-color: #3B82F6;
        color: white;
    }
</style>
""",
            unsafe_allow_html=True)


# Function to fetch data from Supabase
@st.cache_data(ttl=10, max_entries=1)  # Cache for 5 minutes, limit cache size
def get_data_from_supabase():
    """Fetch data from Supabase tables"""
    try:
        # Get data from Supabase
        brokers = supabase.client.table("brokers").select("*").execute()
        leads = supabase.client.table("leads").select("*").execute()
        activities = supabase.client.table("activities").select("*").execute()
        broker_points = supabase.client.table("broker_points").select(
            "*").execute()

        # Convert to DataFrames
        brokers_df = pd.DataFrame(
            brokers.data) if brokers.data else pd.DataFrame()
        leads_df = pd.DataFrame(leads.data) if leads.data else pd.DataFrame()
        activities_df = pd.DataFrame(
            activities.data) if activities.data else pd.DataFrame()
        broker_points_df = pd.DataFrame(
            broker_points.data) if broker_points.data else pd.DataFrame()

        return {
            'brokers': brokers_df,
            'leads': leads_df,
            'activities': activities_df,
            'ranking': broker_points_df
        }
    except Exception as e:
        st.error(f"Erro ao carregar dados do Supabase: {str(e)}")
        return None


# Function to display the ranking cards in the exact format shown in the image
def get_rank_style(points):
    """Define estilos baseados na pontuação"""
    if points < 0:
        return {
            'color': '#DC2626',
            'icon': '⚠️',
            'gradient': 'linear-gradient(135deg, white, #FEE2E2)',
            'border': '#DC2626',
            'badge_bg': '#FEE2E2',
            'status': 'Precisa Atenção'
        }
    elif points == 0:
        return {
            'color': '#6B7280',
            'icon': '🔄',
            'gradient': 'linear-gradient(135deg, white, #F3F4F6)',
            'border': '#6B7280',
            'badge_bg': '#F3F4F6',
            'status': 'Iniciando'
        }
    elif points < 100:
        return {
            'color': '#FB923C',
            'icon': '📈',
            'gradient': 'linear-gradient(135deg, white, #FFEDD5)',
            'border': '#FB923C',
            'badge_bg': '#FFEDD5',
            'status': 'Em Evolução'
        }
    elif points < 300:
        return {
            'color': '#EAB308',
            'icon': '🌟',
            'gradient': 'linear-gradient(135deg, white, #FEF9C3)',
            'border': '#EAB308',
            'badge_bg': '#FEF9C3',
            'status': 'Progredindo'
        }
    elif points < 500:
        return {
            'color': '#3B82F6',
            'icon': '⭐',
            'gradient': 'linear-gradient(135deg, white, #DBEAFE)',
            'border': '#3B82F6',
            'badge_bg': '#DBEAFE',
            'status': 'Avançado'
        }
    else:
        return {
            'color': '#22C55E',
            'icon': '🏆',
            'gradient': 'linear-gradient(135deg, white, #DCFCE7)',
            'border': '#22C55E',
            'badge_bg': '#DCFCE7',
            'status': 'Expert'
        }


# Function to display the ranking cards in the exact format shown in the image
def display_ranking_cards(ranking_data):
    """Display ranking cards in a grid layout"""
    if ranking_data.empty:
        st.info(
            "📭 Nenhum dado de pontuação ainda. Acompanhe em breve o desempenho dos corretores."
        )
        return

    # Sort brokers by points in descending order
    sorted_brokers = ranking_data.sort_values(
        by='pontos', ascending=False).reset_index(drop=True)
    sorted_brokers.index = sorted_brokers.index + 1  # Start index from 1

    # Create a grid of cards - 3 per row as shown in the reference image
    cols = st.columns(3)

    # Display only top 9 brokers (or fewer if less available)
    for i, row in enumerate(sorted_brokers.head(9).itertuples()):
        col_idx = i % 3  # Determine which column to place the card
        rank_style = get_rank_style(row.pontos)

        with cols[col_idx]:
            st.markdown(f"""
            <div class="ranking-card" style="background: {rank_style['gradient']}; border-left: 4px solid {rank_style['border']};">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <div style="display: flex; align-items: center;">
                        <div class="rank-number" style="
                            background-color: {rank_style['color']}; 
                            color: white;
                            width: 30px;
                            height: 30px;
                            border-radius: 50%;
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            margin-right: 12px;
                            font-weight: bold;
                            font-size: 16px;">
                            {row.Index}
                        </div>
                        <div style="display: flex; flex-direction: column;">
                            <div class="broker-name" style="font-weight: 600; color: #1F2937;">{row.nome}</div>
                            <div style="font-size: 12px; color: {rank_style['color']}; margin-top: 2px;">
                                {rank_style['icon']} {rank_style['status']}
                            </div>
                        </div>
                    </div>
                    <div style="
                        background-color: {rank_style['badge_bg']};
                        padding: 8px 12px;
                        border-radius: 12px;
                        display: flex;
                        flex-direction: column;
                        align-items: center;
                    ">
                        <div style="font-size: 11px; color: {rank_style['color']}; font-weight: 500;">PONTOS</div>
                        <div style="font-size: 18px; color: {rank_style['color']}; font-weight: 700;">{int(row.pontos)}</div>
                    </div>
                </div>
                <div style="
                    display: flex;
                    justify-content: space-between;
                    background-color: {rank_style['color']}08;
                    padding: 8px;
                    border-radius: 8px;
                    margin-top: 8px;
                ">
                    <div style="text-align: center; flex: 1;">
                        <div style="font-size: 11px; color: #6B7280;">Leads</div>
                        <div style="font-size: 14px; color: #374151; font-weight: 600;">{int(row.leads_visitados or 0)}</div>
                    </div>
                    <div style="text-align: center; flex: 1; border-left: 1px solid {rank_style['color']}20; border-right: 1px solid {rank_style['color']}20;">
                        <div style="font-size: 11px; color: #6B7280;">Propostas</div>
                        <div style="font-size: 14px; color: #374151; font-weight: 600;">{int(row.propostas_enviadas or 0)}</div>
                    </div>
                    <div style="text-align: center; flex: 1;">
                        <div style="font-size: 11px; color: #6B7280;">Vendas</div>
                        <div style="font-size: 14px; color: #374151; font-weight: 600;">{int(row.vendas_realizadas or 0)}</div>
                    </div>
                </div>
            </div>
            """,
                        unsafe_allow_html=True)


# Function to fetch data from Supabase
@st.cache_data(ttl=300, max_entries=10)
def calculate_broker_metrics(broker_id, data):
    """Calculate broker metrics for caching"""
    broker_points = data.get('ranking', pd.DataFrame())

    if not broker_points.empty:
        broker_row = broker_points[broker_points['id'] == broker_id]
        if not broker_row.empty:
            return {
                'leads_respondidos_1h':
                int(broker_row['leads_respondidos_1h'].values[0])
                if 'leads_respondidos_1h' in broker_row.columns else 0,
                'leads_visitados':
                int(broker_row['leads_visitados'].values[0])
                if 'leads_visitados' in broker_row.columns else 0,
                'propostas_enviadas':
                int(broker_row['propostas_enviadas'].values[0])
                if 'propostas_enviadas' in broker_row.columns else 0,
                'vendas_realizadas':
                int(broker_row['vendas_realizadas'].values[0])
                if 'vendas_realizadas' in broker_row.columns else 0
            }
    return None


def display_broker_metrics(broker_id, data):
    """Display broker metrics in the dashboard"""
    metrics = calculate_broker_metrics(broker_id, data)

    if metrics is None:
        st.info("Dados do corretor não disponíveis.")
        return

    cols = st.columns(4)

    with cols[0]:
        st.metric("Leads Respondidos em 1h", metrics['leads_respondidos_1h'])
    with cols[1]:
        st.metric("Leads Visitados", metrics['leads_visitados'])
    with cols[2]:
        st.metric("Propostas Enviadas", metrics['propostas_enviadas'])
    with cols[3]:
        st.metric("Vendas Realizadas", metrics['vendas_realizadas'])


# Function to display the activity heatmap with advanced filtering options
@st.cache_data(ttl=300, max_entries=10)
def process_heatmap_data(broker_id, data, activity_type, date_range,
                         lead_status):
    """Process and cache heatmap data"""
    if 'activities' not in data or data['activities'].empty:
        return None

    broker_activities = data['activities'][data['activities']['user_id'] ==
                                           broker_id]
    if broker_activities.empty:
        return None

    filtered_activities = broker_activities.copy()

    # Apply filters
    if activity_type != 'Todos':
        filtered_activities = filtered_activities[filtered_activities['tipo']
                                                  == activity_type]

    if date_range != 'Todos os períodos':
        today = datetime.now()
        if date_range == 'Últimos 7 dias':
            date_filter = today - timedelta(days=7)
        elif date_range == 'Últimos 30 dias':
            date_filter = today - timedelta(days=30)
        else:  # Últimos 90 dias
            date_filter = today - timedelta(days=90)
        filtered_activities = filtered_activities[
            filtered_activities['criado_em'] >= date_filter]

    return filtered_activities


def display_activity_heatmap(broker_id, data):
    """Display heatmap apenas com mensagem_enviada entre 08h e 22h"""

    if 'activities' not in data or data['activities'].empty:
        st.info("Sem dados de atividades.")
        return

    # Filtrar por corretor, tipo, e horário
    broker_activities = data['activities']
    filtered_activities = broker_activities[
        (broker_activities['user_id'] == broker_id)
        & (broker_activities['tipo'] == 'mensagem_enviada') &
        (broker_activities['hora'] >= 8) &
        (broker_activities['hora'] < 22)].copy()

    logger.info(
        f"[HEATMAP] Corretor {broker_id} - Atividades filtradas: {len(filtered_activities)} registros encontrados"
    )
    logger.info(
        f"[HEATMAP] Preview das atividades:\n{filtered_activities.head().to_string(index=False)}"
    )

    if filtered_activities.empty:
        st.info("Nenhuma atividade de mensagem enviada entre 08h e 22h.")
        return

    # Gerar o heatmap com filtro já aplicado
    heatmap_fig = create_heatmap(filtered_activities,
                                 activity_type="mensagem_enviada")
    st.plotly_chart(heatmap_fig, use_container_width=True)


# Function to display the points breakdown chart
def display_points_breakdown(broker_id, data):
    """Display points breakdown chart for the broker"""
    broker_row = data['ranking'][data['ranking']['id'] == broker_id]

    if broker_row.empty:
        st.info("Dados de pontuação não disponíveis.")
        return

    # Create points breakdown chart
    points_fig = create_points_breakdown_chart(broker_row.iloc[0])
    st.plotly_chart(points_fig, use_container_width=True)


#Function to display the alerts for the broker
def display_broker_alerts(broker_id, data):
    """Display alerts for the broker"""
    broker_row = data['ranking'][data['ranking']['id'] == broker_id]

    if broker_row.empty:
        st.info("Dados de alertas não disponíveis.")
        return

    # Get alerts from ranking data
    alerts = []

    if 'leads_sem_interacao_24h' in broker_row.columns and broker_row[
            'leads_sem_interacao_24h'].values[0] > 0:
        alerts.append(
            f"⚠️ {int(broker_row['leads_sem_interacao_24h'].values[0])} leads sem interação há mais de 24h"
        )

    if 'leads_respondidos_apos_18h' in broker_row.columns and broker_row[
            'leads_respondidos_apos_18h'].values[0] > 0:
        alerts.append(
            f"⚠️ {int(broker_row['leads_respondidos_apos_18h'].values[0])} leads respondidos após 18h"
        )

    if 'leads_tempo_resposta_acima_12h' in broker_row.columns and broker_row[
            'leads_tempo_resposta_acima_12h'].values[0] > 0:
        alerts.append(
            f"⚠️ {int(broker_row['leads_tempo_resposta_acima_12h'].values[0])} leads com tempo médio de resposta acima de 12h"
        )

    if 'leads_5_dias_sem_mudanca' in broker_row.columns and broker_row[
            'leads_5_dias_sem_mudanca'].values[0] > 0:
        alerts.append(
            f"⚠️ {int(broker_row['leads_5_dias_sem_mudanca'].values[0])} leads com mais de 5 dias sem mudança de etapa"
        )

    if alerts:
        for alert in alerts:
            st.warning(alert)
    else:
        st.success("Não há alertas para este corretor.")


# Function to create conversion funnel with consistent styling
def create_styled_conversion_funnel(lead_data):
    """Create a styled conversion funnel visualization"""
    # Define the pipeline stages in order
    stages = [
        "Novo Lead", "Qualificação", "Apresentação", "Proposta", "Negociação",
        "Fechado"
    ]

    # Count leads in each stage
    stage_counts = []
    for stage in stages:
        count = len(lead_data[lead_data['etapa'] ==
                              stage]) if 'etapa' in lead_data.columns else 0
        stage_counts.append(count)

    # Create funnel chart with specific styling to match the reference image
    fig = go.Figure(
        go.Funnel(y=stages,
                  x=stage_counts,
                  textposition="inside",
                  textinfo="value",
                  marker=dict(color=[
                      "#3B82F6", "#60A5FA", "#93C5FD", "#BFDBFE", "#DBEAFE",
                      "#93C5FD"
                  ],
                              line=dict(width=1,
                                        color=[
                                            "#2563EB", "#2563EB", "#2563EB",
                                            "#2563EB", "#2563EB", "#2563EB"
                                        ])),
                  connector=dict(line=dict(width=1))))

    fig.update_layout(height=300,
                      margin=dict(t=0, l=5, r=5, b=0),
                      font=dict(size=14),
                      paper_bgcolor='rgba(0,0,0,0)',
                      plot_bgcolor='rgba(0,0,0,0)')

    return fig


# Main application
def display_broker_dashboard(broker_id, data):
    """Display individual broker dashboard"""
    broker = data['brokers'][data['brokers']['id'] == broker_id].iloc[0]

    # Get broker details
    if 'responsavel_id' in data['leads'].columns:
        broker_leads = data['leads'][data['leads']['responsavel_id'] ==
                                     broker_id]
    else:
        broker_leads = None

    # Display broker header with ranking position
    broker_rank = data['ranking'][data['ranking']['id'] == broker_id].index[
        0] + 1 if not data['ranking'].empty and broker_id in data['ranking'][
            'id'].values else "N/A"
    broker_points = int(
        data['ranking'][data['ranking']['id'] == broker_id]['pontos'].values[0]
    ) if not data['ranking'].empty and broker_id in data['ranking'][
        'id'].values else 0

    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
        <h2>{broker['nome']}</h2>
        <div style="display: flex; align-items: center;">
            <div style="margin-right: 20px; text-align: center;">
                <div style="font-size: 14px; color: #6B7280;">Ranking</div>
                <div style="font-size: 24px; font-weight: bold; color: #1E3A8A;">#{broker_rank}</div>
            </div>
            <div style="text-align: center;">
                <div style="font-size: 14px; color: #6B7280;">Pontuação</div>
                <div style="font-size: 24px; font-weight: bold; color: #2563EB;">{broker_points}</div>
            </div>
        </div>
    </div>
    """,
                unsafe_allow_html=True)

    # Create tabs
    tab1, tab2 = st.tabs(["Métricas", "Detalhes"])

    with tab1:
        # Display metrics and charts
        display_broker_metrics(broker_id, data)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="card-title">Funil de Conversão</div>',
                        unsafe_allow_html=True)
            funnel_fig = create_styled_conversion_funnel(broker_leads)
            st.plotly_chart(funnel_fig, use_container_width=True)

            st.markdown('<div class="card-title">Alertas</div>',
                        unsafe_allow_html=True)
            display_broker_alerts(broker_id, data)

        with col2:
            st.markdown(
                '<div class="card-title">Mapa de Calor - Atividades</div>',
                unsafe_allow_html=True)
            display_activity_heatmap(broker_id, data)

            st.markdown('<div class="card-title">Distribuição de Pontos</div>',
                        unsafe_allow_html=True)
            display_points_breakdown(broker_id, data)

    with tab2:
        # Filter only active brokers (those with points in ranking)
        active_broker_ids = data['ranking']['id'].tolist(
        ) if not data['ranking'].empty else []
        broker_options = data['brokers'][data['brokers']['id'].isin(
            active_broker_ids)][['id', 'nome']].copy()
        broker_options['display_name'] = broker_options['nome']

        selected_broker = st.selectbox(
            "Selecione um corretor",
            options=broker_options['id'].tolist(),
            format_func=lambda x: broker_options.loc[broker_options['id'] == x,
                                                     'display_name'].iloc[0],
            key="broker_selector")

        if selected_broker:
            display_broker_metrics(selected_broker, data)

            # Create two columns for the charts
            col1, col2 = st.columns(2)

            with col1:
                st.markdown('<div class="card-title">Funil de Conversão</div>',
                            unsafe_allow_html=True)
                funnel_fig = create_styled_conversion_funnel(broker_leads)
                st.plotly_chart(funnel_fig, use_container_width=True)

                st.markdown('<div class="card-title">Alertas</div>',
                            unsafe_allow_html=True)
                display_broker_alerts(selected_broker, data)

            with col2:
                st.markdown(
                    '<div class="card-title">Mapa de Calor - Atividades</div>',
                    unsafe_allow_html=True)
                display_activity_heatmap(selected_broker, data)

                st.markdown(
                    '<div class="card-title">Distribuição de Pontos</div>',
                    unsafe_allow_html=True)
                display_points_breakdown(selected_broker, data)


def display_general_ranking(data):
    """Display general ranking dashboard"""
    st.markdown("### Ranking de Corretores")
    st.markdown("Classificação baseada em pontos acumulados por produtividade")
    display_ranking_cards(data['ranking'])

    st.markdown("### Estatísticas Gerais")
    col1, col2, col3, col4 = st.columns(4)

    total_leads = len(data['leads']) if not data['leads'].empty else 0
    active_brokers = len(data['ranking']) if not data['ranking'].empty else 0
    avg_points = int(
        data['ranking']['pontos'].mean()) if not data['ranking'].empty else 0
    total_sales = int(data['ranking']['vendas_realizadas'].sum(
    )) if not data['ranking'].empty and 'vendas_realizadas' in data[
        'ranking'].columns else 0

    with col1:
        st.metric("Total de Leads", total_leads)
    with col2:
        st.metric("Corretores Ativos", active_brokers)
    with col3:
        st.metric("Pontuação Média", avg_points)
    with col4:
        st.metric("Vendas Realizadas", total_sales)


# Lock para sincronização do state
state_lock = threading.Lock()


def auto_update_broker_points():
    while True:
        try:
            logger.info("[Auto Update] Atualizando pontos dos corretores")
            supabase.update_broker_points()
            logger.info("[Auto Update] Pontos atualizados com sucesso")
            st.rerun()
            logger.info(
                "[Auto Update] Aguardando 5 minutos para a próxima atualização"
            )
            time.sleep(300)
        except Exception as e:
            logger.error(f"[Auto Update] Erro ao atualizar pontos: {str(e)}")
        time.sleep(10)



def main():
    # Inicializar session state se necessário
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "ranking"

    # Título da página
    st.markdown(
        "<h1 style='text-align: center;'>Dashboard de Desempenho - Corretores</h1>",
        unsafe_allow_html=True)

    # Fetch data from Supabase
    data = get_data_from_supabase()

    if data is None:
        st.error("Erro ao conectar com o banco de dados")
        return

    # Iniciar threads de background se ainda não iniciados
    if not st.session_state.get("background_started"):
        data_thread = threading.Thread(target=background_data_loader,
                                       daemon=True)

        # Atualizar lista de corretores ativos
        st.session_state["active_brokers"] = data['brokers'][
            data['brokers']['cargo'] == 'Corretor']['id'].tolist()
        
        # Rotação de views integrada à função main()
        time.sleep(5) # Aguarda 5 segundos para garantir que os dados foram carregados

        while True:
            try:
                time.sleep(10) # Aguarda 10 segundos antes de cada rotação

                with state_lock:
                    if "current_page" not in st.session_state:
                        st.session_state["current_page"] = "ranking"
                        logger.info(
                            "[View Rotator] Iniciando na página de ranking")

                    if "active_brokers" not in st.session_state:
                        logger.info(
                            "[View Rotator] Aguardando lista de corretores...")
                        continue

                    current_page = st.session_state["current_page"]
                    next_page = current_page

                    if current_page == "ranking":
                        if st.session_state["active_brokers"]:
                            next_page = f"broker/{st.session_state['active_brokers'][0]}"
                    else:
                        try:
                            current_broker = int(current_page.split('/')[1])
                            broker_ids = st.session_state["active_brokers"]
                            current_index = broker_ids.index(current_broker)

                            if current_index == len(broker_ids) - 1:
                                next_page = "ranking"
                            else:
                                next_page = f"broker/{broker_ids[current_index + 1]}"
                        except (ValueError, IndexError):
                            next_page = "ranking"

                    logger.info(
                        f"[View Rotator] Rotacionando de '{current_page}' para '{next_page}'"
                    )
                    st.session_state["current_page"] = next_page

                st.rerun()
            except Exception as e:
                logger.error(f"Erro na rotação de views: {str(e)}")
                time.sleep(1)


        data_thread.start()

        st.session_state["background_started"] = True

    if data['brokers'].empty:
        st.warning("Nenhum corretor cadastrado no sistema.")
        return

    if data['ranking'].empty:
        st.info(
            "🔄 Os pontos ainda estão sendo calculados. Assim que estiverem prontos, o ranking será exibido aqui."
        )
        st.rerun()

    # Get active brokers
    active_brokers = data['brokers'][data['brokers']['cargo'] == 'Corretor']

    # Use session state to control which view to show
    current_page = st.session_state["current_page"]
    broker_id = None

    if current_page != "ranking":
        try:
            broker_id = int(current_page.split('/')[1])
        except (IndexError, ValueError):
            st.warning("Página inválida. Retornando ao ranking geral.")
            st.session_state["current_page"] = "ranking"
            current_page = "ranking"

    if current_page == "ranking":
        display_general_ranking(data)
    elif current_page.startswith("broker") and broker_id:
        display_broker_dashboard(broker_id, data)

    # Add JavaScript for auto-rotation
    broker_ids = active_brokers['id'].tolist()
    st.markdown(f"""
    <script type="text/javascript">
        window.addEventListener('load', function() {{
            const brokerIds = {broker_ids};

            function rotateViews() {{
                const urlParams = new URLSearchParams(window.location.search);
                let currentPage = urlParams.get('page') || 'ranking';
                let newUrl = new URL(window.location.href);

                if (currentPage === 'ranking') {{
                    newUrl.searchParams.set('page', 'broker/' + brokerIds[0]);
                }} else {{
                    let currentBrokerId = parseInt(currentPage.split('/')[1]);
                    let currentIndex = brokerIds.indexOf(currentBrokerId);

                    if (currentIndex === -1 || currentIndex === brokerIds.length - 1) {{
                        newUrl.searchParams.set('page', 'ranking');
                    }} else {{
                        newUrl.searchParams.set('page', 'broker/' + brokerIds[currentIndex + 1]);
                    }}
                }}

                window.location.href = newUrl.toString();
            }}

            // Iniciar rotação após 10 segundos
            setTimeout(() => {{
                setInterval(rotateViews, 10000);
            }}, 10000);
        }});
    </script>
    """,
                unsafe_allow_html=True)


if __name__ == "__main__":
    main()