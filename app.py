import os
import time
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import threading
import logging
from flask import Flask, jsonify
from PIL import Image

# Import custom modules
from libs import KommoAPI, SupabaseClient, SyncManager
from data_processor import process_data
from visualizations import create_heatmap, create_conversion_funnel, create_points_breakdown_chart
from view_manager import ViewManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

hide_streamlit_style = """
            <style>
                /* Hide the Streamlit header and menu */
                header {visibility: hidden;}
            </style>
            """

st.set_page_config(page_title="Dicasa - Dashboard de Desempenho",
                   layout="wide")

st.markdown(hide_streamlit_style, unsafe_allow_html=True)


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


# Custom CSS
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #f6f9fc 0%, #eef2f7 100%);
        min-height: 100vh;
        font-family: 'Inter', sans-serif;
    }
    .main .block-container {
        padding: 2.5rem;
        max-width: 1400px;
        margin: 1.5rem auto;
        background: rgba(255, 255, 255, 0.98);
        backdrop-filter: blur(10px);
        border-radius: 24px;
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.9);
    }
    .stApp > header {
        background-color: transparent !important;
    }
    .stButton > button {
        border-radius: 12px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.2s ease;
        background: linear-gradient(135deg, #3B82F6, #2563EB);
        border: none;
        color: white;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 15px rgba(37, 99, 235, 0.2);
    }
    h1 {
        font-size: 2.4rem !important;
        font-weight: 700 !important;
        color: #1E3A8A !important;
        text-align: center !important;
        margin-bottom: 2rem !important;
        text-shadow: 1px 1px 2px rgba(0,0,0,0.1);
    }
    h2 {
        font-size: 2rem !important;
        font-weight: 600 !important;
        color: #2563EB !important;
        margin: 1.5rem 0 !important;
        border-bottom: 2px solid #E2E8F0;
        padding-bottom: 0.5rem;
    }
    h3 {
        font-size: 1.5rem !important;
        font-weight: 600 !important;
        color: #334155 !important;
        margin: 1rem 0 !important;
    }
    .stMetric {
        background: linear-gradient(145deg, #ffffff, #f8fafc);
        padding: 1.8rem;
        border-radius: 16px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
        border: 1px solid rgba(226, 232, 240, 0.8);
    }
    .stMetric:hover {
        transform: translateY(-3px);
        box-shadow: 0 12px 25px rgba(0,0,0,0.07);
        border-color: rgba(59, 130, 246, 0.3);
    }
    .stMetric label {
        font-size: 0.875rem !important;
        font-weight: 600 !important;
        color: #64748B !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.5rem;
    }
    .stMetric [data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: 700 !important;
        background: linear-gradient(45deg, #1E40AF, #3B82F6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-top: 0.25rem;
    }
    .stMetric label {
        font-size: 1rem !important;
        font-weight: 600 !important;
        color: #4B5563 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .stMetric .css-1xarl3l {
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        background: linear-gradient(45deg, #1E40AF, #3B82F6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
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
        background: linear-gradient(145deg, #ffffff, #f8fafc);
        padding: 1.8rem;
        border-radius: 20px;
        box-shadow: 0 8px 20px rgba(0,0,0,0.06);
        margin-bottom: 2rem;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        border: 1px solid rgba(226, 232, 240, 0.9);
        position: relative;
        overflow: hidden;
    }
    .ranking-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #3B82F6, #2563EB);
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    .ranking-card:hover::before {
        opacity: 1;
    }
    .ranking-card:hover {
        transform: translateY(-4px) scale(1.02);
        box-shadow: 0 20px 30px rgba(0,0,0,0.1);
        border-color: rgba(59, 130, 246, 0.2);
    }
    .ranking-card:hover {
        transform: translateY(-4px) scale(1.01);
        box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04);
        border-color: #CBD5E1;
    }
    .ranking-card::after {
        content: '';
        position: absolute;
        inset: 0;
        z-index: -1;
        background: linear-gradient(135deg, rgba(59,130,246,0.1), rgba(30,64,175,0.1));
        border-radius: 16px;
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    .ranking-card:hover::after {
        opacity: 1;
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
    """Exibe o mapa de calor de atividades (mensagem_enviada) das 08h às 22h para o corretor atual"""

    if 'activities' not in data or data['activities'].empty:
        st.info("Sem dados de atividades.")
        return

    activities = data['activities']

    # Filtro por corretor, tipo e horário
    filtered = activities[(activities['user_id'] == broker_id)
                          & (activities['tipo'] == 'mensagem_enviada') &
                          (activities['hora'] >= 8) &
                          (activities['hora'] <= 21)].copy()

    # Se estiver vazio, avisa
    if filtered.empty:
        st.info("Nenhuma atividade de mensagem enviada entre 08h e 22h.")
        return

    # Força dia_semana como categoria ordenada
    day_order = [
        'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'
    ]
    filtered['dia_semana'] = pd.Categorical(filtered['dia_semana'],
                                            categories=day_order,
                                            ordered=True)

    # Gerar o heatmap com os dados filtrados
    fig = create_heatmap(filtered, activity_type="mensagem_enviada")
    st.plotly_chart(fig, use_container_width=True, key=f"heatmap_{broker_id}")


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
                <div<div style="font-size: 24px; font-weight: bold; color: #2563EB;">{broker_points}</div>
            </div>
        </div>
    </div>
    """,
                unsafe_allow_html=True)

    # Create tabs
    tabs = st.tabs(["Métricas"])
    metrics_tab = tabs[0]

    with metrics_tab:
        # Display metrics and charts
        display_broker_metrics(broker_id, data)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown('<div class="card-title">Funil de Conversão</div>',
                        unsafe_allow_html=True)
            funnel_fig = create_styled_conversion_funnel(broker_leads)
            st.plotly_chart(funnel_fig,
                            use_container_width=True,
                            key=f"funnel_{broker_id}")

            st.markdown('<div class="card-title">Alertas</div>',
                        unsafe_allow_html=True)
            display_broker_alerts(broker_id, data)

        with col2:
            st.markdown(
                '<div class="card-title">Mapa de Calor - Mensagens Enviadas</div>',
                unsafe_allow_html=True)
            display_activity_heatmap(broker_id, data)

            st.markdown('<div class="card-title">Distribuição de Pontos</div>',
                        unsafe_allow_html=True)
            display_points_breakdown(broker_id, data)


def display_general_ranking(data):
    """Display general ranking dashboard"""
    st.markdown("### Ranking de Corretores")
    st.markdown("Classificação baseada em pontos acumulados por produtividade")
    display_ranking_cards(data['ranking'])

    st.markdown("### Estatísticas Gerais")
    col1, col2, col3, col4 = st.columns(4)

    total_leads = len(data['leads'][data['leads']['pipeline_id'] == 8865067]) \
        if not data['leads'].empty else 0
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
            logger.info(
                "[Auto Update] Aguardando 5 minutos para a próxima atualização"
            )
            time.sleep(300)

            st.rerun()
        except Exception as e:
            logger.error(f"[Auto Update] Erro ao atualizar pontos: {str(e)}")
        time.sleep(10)


# Inicializar ViewManager como recurso global
@st.cache_resource
def get_view_manager():
    return ViewManager(rotation_interval=5)


def display_login_page():
    st.markdown("""
    <style>
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(135deg, #f6f9fc 0%, #eef2f7 100%);
        }
        
        [data-testid="stVerticalBlock"] {
            padding-top: 10vh;
            margin: 0 auto;
            max-width: 400px;
        }

        [data-testid="stImage"] {
            margin: 0 auto 2rem;
            display: block;
        }

        .login-card {
            background: white;
            padding: 2rem;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-top: 2rem;
        }

        .stButton > button {
            width: 100%;
            background: linear-gradient(135deg, #3B82F6, #2563EB);
            color: white;
            padding: 0.75rem;
            border-radius: 8px;
            border: none;
            font-weight: 600;
            margin-top: 1rem;
        }

        .stTextInput > div > input {
            background: #F3F4F6;
            border-radius: 8px;
            padding: 0.75rem;
            margin-bottom: 0.5rem;
        }
    </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        imagem = Image.open("logo_dicasa.png")
        st.image(imagem, width=200)
        
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        
        email = st.text_input("Email", placeholder="Digite seu email")
        password = st.text_input("Senha", type="password", placeholder="Digite sua senha")

        if st.button("Entrar"):
            try:
                response = supabase.client.auth.sign_in_with_password({
                    "email": email,
                    "password": password
                })
                if response.user:
                    st.session_state["authenticated"] = True
                    st.query_params["page"] = "ranking"
                    st.rerun()
                else:
                    st.error("Login falhou. Verifique suas credenciais.")
            except Exception as e:
                st.error(f"Erro no login: {str(e)}")
        
        st.markdown('</div>', unsafe_allow_html=True)


def main():
    # Inicializar estado de autenticação se necessário
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    # Verificar se é a página de login
    current_page = st.query_params.get("page", "ranking")
    if current_page == "login":
        display_login_page()
        return

    def rotate_views_on_reload():
        view_manager = get_view_manager()

        if "active_brokers" not in st.session_state:
            st.session_state["active_brokers"] = []

        if "last_rotation_time" not in st.session_state:
            st.session_state["last_rotation_time"] = time.time()

        elapsed = time.time() - st.session_state["last_rotation_time"]

        if elapsed >= 10:  # só rotaciona a cada 5 segundos
            view_manager.set_active_brokers(st.session_state["active_brokers"])
            next_page = view_manager.get_next_page()

            if next_page and next_page != st.session_state.get("current_page"):
                st.session_state["current_page"] = next_page
                st.session_state["last_rotation_time"] = time.time()
                st.query_params["page"] = next_page
                st.rerun()

    # Gerenciar rotação de páginas
    # handle_page_rotation()

    # # Verificar se houve mudança de página
    # url_page = st.query_params.get("page", "ranking")
    # if url_page != st.session_state.get("current_page"):
    #     st.session_state["current_page"] = url_page
    #     st.rerun()

    if "current_page" not in st.session_state:
        st.session_state["current_page"] = "ranking"

    if "page" not in st.query_params:
        st.query_params.update({"page": "ranking"})

    # Header with logout button
    col1, col2, col3 = st.columns([8, 2, 2])
    with col1:
        st.markdown(
            "<h1 style='text-align: center !important;'>Dashboard de Desempenho - Corretores</h1>",
            unsafe_allow_html=True)

    # Fetch data from Supabase
    data = get_data_from_supabase()

    if data is None:
        st.error("Erro ao conectar com o banco de dados")
        return

    # Iniciar threads de background se ainda não iniciados
    if not st.session_state.get("background_started"):
        brokers_df = data['brokers']
        if 'cargo' in brokers_df.columns:
            active_brokers = brokers_df[brokers_df['cargo'] ==
                                        'Corretor']['id'].tolist()
        else:
            # Se não houver coluna cargo, considera todos os brokers como ativos
            active_brokers = brokers_df['id'].tolist()
        st.session_state["active_brokers"] = active_brokers

        # st.session_state["rotation_thread"] = threading.Thread(
        #     target=rotate_views_loop, daemon=True)
        # st.session_state["rotation_thread"].start()

        st.session_state["data_thread"] = threading.Thread(
            target=background_data_loader, daemon=True)
        st.session_state["data_thread"].start()

        st.session_state["background_started"] = True

    if data['brokers'].empty:
        st.warning("Nenhum corretor cadastrado no sistema.")
        return

    if data['ranking'].empty:
        st.info(
            "🔄 Os pontos ainda estão sendo calculados. Assim que estiverem prontos, o ranking será exibido aqui."
        )
        return

    # Get active brokers
    active_brokers = data['brokers'][data['brokers']['cargo'] == 'Corretor']

    # Ler parâmetros da URL e sincronizar com session state
    url_page = st.query_params.get("page", "ranking")

    if url_page != st.session_state["current_page"]:
        st.session_state["current_page"] = url_page

    current_page = st.session_state["current_page"]
    broker_id = None

    logger.info(f"[MAIN]Current page: {current_page}")
    logger.info(f"[MAIN] Active brokers: {active_brokers}")

    if current_page != "ranking":
        try:
            broker_id = int(current_page.split('/')[1])
            broker_exists = data['brokers'][data['brokers']['id'] == broker_id]
            if broker_exists.empty:
                raise ValueError("Corretor inválido")
        except (IndexError, ValueError):
            st.warning("Página inválida. Retornando ao ranking geral.")
            st.session_state["current_page"] = "ranking"
            st.query_params["page"] = "ranking"
            current_page = "ranking"

    rotate_views_on_reload()

    if current_page == "ranking":
        display_general_ranking(data)
    elif current_page.startswith("broker") and broker_id:
        display_broker_dashboard(broker_id, data)

    # # Add JavaScript for auto-rotation
    # # broker_ids = active_brokers['id'].tolist()
    # # Exibir JS de rotação de views se necessário
    # if "next_page" in st.session_state and st.session_state["next_page"]:
    #     next_page = st.session_state["next_page"]
    #     st.session_state["next_page"] = None  # limpa para não repetir

    #     st.markdown(f"""
    #     <script>
    #         setTimeout(function() {{
    #             window.location.href = window.location.pathname + "?page={next_page}";
    #         }}, 1000);
    #     </script>
    #     """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
