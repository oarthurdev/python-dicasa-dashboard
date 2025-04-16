import os
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
from flask import Flask

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

# def background_data_loader_once():
#     """
#     Runs the sync process once to fetch initial data from Kommo and update Supabase.
#     """
#     try:
#         # Initialize clients
#         kommo_api = KommoAPI(api_url=os.getenv(
#             "KOMMO_API_URL", "https://dicasaindaial.kommo.com/api/v4"),
#                              access_token=os.getenv("ACCESS_TOKEN_KOMMO"))

#         supabase = SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
#                                   key=os.getenv("VITE_SUPABASE_ANON_KEY"))

#         sync_manager = SyncManager(kommo_api, supabase)

#         logger.info("Running initial data sync with Kommo API")
#         sync_manager.sync_data()

#     except Exception as e:
#         logger.error(f"Failed to run initial data sync: {str(e)}")


# Initialize API and database clients
@st.cache_resource
def init_supabase_client():
    return SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                          key=os.getenv("VITE_SUPABASE_ANON_KEY"))


supabase = init_supabase_client()

supabase.initialize_broker_points()

def background_data_loader():
    """
    This function runs in the background to continuously monitor Kommo API
    for changes and update the database accordingly
    """
    try:
        kommo_api = KommoAPI(api_url=os.getenv("KOMMO_API_URL", "https://dicasaindaial.kommo.com/api/v4"),
                            access_token=os.getenv("ACCESS_TOKEN_KOMMO"))
        
        supabase = SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                                key=os.getenv("VITE_SUPABASE_ANON_KEY"))
        
        sync_manager = SyncManager(kommo_api, supabase)
        last_sync_time = None

        # Initial check for broker_points data
        existing = supabase.client.table("broker_points").select("*").limit(1).execute()
        if not existing.data:
            logger.info("Inicializando broker_points...")
            supabase.initialize_broker_points()

        while True:
            try:
                current_time = datetime.now()
                
                # Only sync if more than 5 minutes have passed since last sync
                if not last_sync_time or (current_time - last_sync_time).total_seconds() > 300:
                    logger.info("Checking for updates from Kommo API")
                    sync_manager.sync_data()
                    
                    # Calculate points after syncing data
                    brokers = kommo_api.get_users()
                    leads = kommo_api.get_leads()
                    activities = kommo_api.get_activities()
                    
                    if not brokers.empty and not leads.empty and not activities.empty:
                        points_df = calculate_broker_points(brokers, leads, activities)
                        supabase.upsert_broker_points(points_df)
                        logger.info("Broker points updated successfully")
                    
                    last_sync_time = current_time
                
                time.sleep(60)  # Check every minute for sync timing

            except Exception as e:
                logger.error(f"Error in background sync: {str(e)}")
                time.sleep(5)  # Wait before retrying on error

    except Exception as e:
        logger.error(f"Failed to initialize background sync: {str(e)}")


# Start the background thread when app starts
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
@st.cache_data(ttl=300, max_entries=1)  # Cache for 5 minutes, limit cache size
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
def get_rank_color(points):
    """Define cores baseadas na pontuação"""
    if points >= 500:
        return "#22C55E", "🏆"  # Verde para pontuação alta
    elif points >= 300:
        return "#3B82F6", "⭐"  # Azul para pontuação média-alta
    elif points >= 100:
        return "#EAB308", "🌟"  # Amarelo para pontuação média
    else:
        return "#6B7280", "🔄"  # Cinza para pontuação baixa


# Function to display the ranking cards in the exact format shown in the image
def display_ranking_cards(ranking_data):
    """Display ranking cards in a grid layout"""
    if ranking_data.empty:
        st.info("Dados de ranking não disponíveis.")
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
        rank_color, rank_icon = get_rank_color(row.pontos)

        with cols[col_idx]:
            st.markdown(f"""
            <div class="ranking-card" style="background: linear-gradient(135deg, white, {rank_color}10); border-left: 4px solid {rank_color};">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <div style="display: flex; align-items: center;">
                        <div class="rank-number" style="
                            background-color: {rank_color}; 
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
                            <div style="font-size: 12px; color: {rank_color}; margin-top: 2px;">
                                {rank_icon} Nível {5-((row.Index-1)//2 if row.Index <= 9 else 1)}
                            </div>
                        </div>
                    </div>
                    <div style="
                        background-color: {rank_color}15;
                        padding: 8px 12px;
                        border-radius: 12px;
                        display: flex;
                        flex-direction: column;
                        align-items: center;
                    ">
                        <div style="font-size: 11px; color: {rank_color}; font-weight: 500;">PONTOS</div>
                        <div style="font-size: 18px; color: {rank_color}; font-weight: 700;">{int(row.pontos)}</div>
                    </div>
                </div>
                <div style="
                    display: flex;
                    justify-content: space-between;
                    background-color: {rank_color}08;
                    padding: 8px;
                    border-radius: 8px;
                    margin-top: 8px;
                ">
                    <div style="text-align: center; flex: 1;">
                        <div style="font-size: 11px; color: #6B7280;">Leads</div>
                        <div style="font-size: 14px; color: #374151; font-weight: 600;">{int(row.leads_visitados or 0)}</div>
                    </div>
                    <div style="text-align: center; flex: 1; border-left: 1px solid {rank_color}20; border-right: 1px solid {rank_color}20;">
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


# Function to display the broker performance breakdown
@st.cache_data(ttl=300, max_entries=10)
def calculate_broker_metrics(broker_id, data):
    """Calculate broker metrics for caching"""
    broker_points = data.get('ranking', pd.DataFrame())

    if not broker_points.empty:
        broker_row = broker_points[broker_points.index == broker_id]
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
    # Find the broker in the data
    broker_points = data.get('ranking', pd.DataFrame())

    if not broker_points.empty:
        broker_row = broker_points[broker_points.index == broker_id]
    else:
        broker_row = pd.DataFrame()

    if broker_row.empty:
        st.info("Dados do corretor não disponíveis.")
        return

    # Create metrics row
    cols = st.columns(4)

    # Extract performance metrics
    leads_respondidos_1h = int(
        broker_row['leads_respondidos_1h'].values[0]
    ) if 'leads_respondidos_1h' in broker_row.columns else 0
    leads_visitados = int(broker_row['leads_visitados'].values[0]
                          ) if 'leads_visitados' in broker_row.columns else 0
    propostas_enviadas = int(
        broker_row['propostas_enviadas'].values[0]
    ) if 'propostas_enviadas' in broker_row.columns else 0
    vendas_realizadas = int(
        broker_row['vendas_realizadas'].values[0]
    ) if 'vendas_realizadas' in broker_row.columns else 0

    # Display metrics
    with cols[0]:
        st.metric("Leads Respondidos em 1h", leads_respondidos_1h)
    with cols[1]:
        st.metric("Leads Visitados", leads_visitados)
    with cols[2]:
        st.metric("Propostas Enviadas", propostas_enviadas)
    with cols[3]:
        st.metric("Vendas Realizadas", vendas_realizadas)


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
    """Display enhanced activity heatmap for the broker with filtering options"""

    if 'activities' in data and not data['activities'].empty:
        broker_activities = data['activities'][data['activities']['user_id'] ==
                                               broker_id]
    else:
        st.info("Não há dados de atividades disponíveis para este corretor.")
        return

    if broker_activities.empty:
        st.info(
            "Não há dados de atividades suficientes para gerar o mapa de calor."
        )
        return

    # Create filter options for the heatmap
    st.markdown("##### Filtros para o Mapa de Calor")

    filter_cols = st.columns([1, 1, 1])

    with filter_cols[0]:
        # Filter by activity type
        activity_types = ['Todos'] + sorted(
            list(broker_activities['tipo'].unique()))
        selected_activity_type = st.selectbox("Tipo de Atividade",
                                              options=activity_types,
                                              key=f"activity_type_{broker_id}")

    with filter_cols[1]:
        # Filter by date range
        date_options = [
            "Todos os períodos", "Últimos 7 dias", "Últimos 30 dias",
            "Últimos 90 dias"
        ]
        selected_date_range = st.selectbox("Período",
                                           options=date_options,
                                           key=f"date_range_{broker_id}")

    with filter_cols[2]:
        # Filter by lead status
        lead_status_options = [
            "Todos os leads", "Leads ativos", "Leads convertidos",
            "Leads perdidos"
        ]
        selected_lead_status = st.selectbox("Status do Lead",
                                            options=lead_status_options,
                                            key=f"lead_status_{broker_id}")

    # Apply filters
    filtered_activities = broker_activities.copy()

    # Filter by activity type
    if selected_activity_type != 'Todos':
        filtered_activities = filtered_activities[filtered_activities['tipo']
                                                  == selected_activity_type]

    # Filter by date range
    if selected_date_range != 'Todos os períodos':
        today = datetime.now()
        if selected_date_range == 'Últimos 7 dias':
            date_filter = today - timedelta(days=7)
        elif selected_date_range == 'Últimos 30 dias':
            date_filter = today - timedelta(days=30)
        else:  # Últimos 90 dias
            date_filter = today - timedelta(days=90)

        filtered_activities = filtered_activities[
            filtered_activities['criado_em'] >= date_filter]

    # Filter by lead status (requires joining with leads data)
    if selected_lead_status != 'Todos os leads' and 'lead_id' in filtered_activities.columns:
        broker_leads = data['leads'][data['leads']['responsavel_id'] ==
                                     broker_id]

        if selected_lead_status == 'Leads ativos':
            active_lead_ids = broker_leads[~broker_leads['fechado']][
                'id'].tolist()
            filtered_activities = filtered_activities[
                filtered_activities['lead_id'].isin(active_lead_ids)]
        elif selected_lead_status == 'Leads convertidos':
            converted_lead_ids = broker_leads[broker_leads['status'] ==
                                              'Ganho']['id'].tolist()
            filtered_activities = filtered_activities[
                filtered_activities['lead_id'].isin(converted_lead_ids)]
        elif selected_lead_status == 'Leads perdidos':
            lost_lead_ids = broker_leads[broker_leads['status'] ==
                                         'Perdido']['id'].tolist()
            filtered_activities = filtered_activities[
                filtered_activities['lead_id'].isin(lost_lead_ids)]

    # Display analysis tips based on the heat map
    with st.expander("💡 Como interpretar o mapa de calor", expanded=False):
        st.markdown("""
        **Cores mais escuras** indicam maior atividade em determinado horário e dia.

        **Este mapa ajuda a identificar:**
        - **Gargalos operacionais:** Períodos sem atividades durante o horário comercial
        - **Oportunidades:** Horários sub-aproveitados que poderiam ter melhor rendimento
        - **Padrões de comportamento:** Horários de maior produtividade para otimizar agendas

        **Ações recomendadas:**
        - Áreas em **vermelho** (Atenção) indicam períodos comerciais sem atividade - reorganize a agenda
        - Áreas marcadas como **Oportunidade** mostram horários pouco explorados com potencial
        - Mais de 30% das atividades após as 18h indica necessidade de redistribuição de horários
        """)

    # Create heatmap with the filtered data
    heatmap_fig = create_heatmap(filtered_activities,
                                 activity_type=selected_activity_type if
                                 selected_activity_type != 'Todos' else None)

    # Show the heatmap
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


# Function to display alerts for the broker
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
def main():
    # Título da página
    st.markdown(
        "<h1 style='text-align: center;'>Dashboard de Desempenho - Corretores</h1>",
        unsafe_allow_html=True)

    # Fetch data from Supabase only
    data = get_data_from_supabase()

    if data is None:
        st.error("Erro ao conectar com o banco de dados")
        return

    # Verifica se pelo menos brokers e ranking têm dados
    if data['brokers'].empty or data['ranking'].empty:
        st.info(
            "Carregando dados do banco... Por favor, aguarde alguns instantes."
        )
        st.info(
            "Se o problema persistir, verifique a conexão com o banco de dados."
        )
        return

    # Create tabs container with custom styling
    st.markdown('<div class="tabs-container">', unsafe_allow_html=True)
    tab1, tab2 = st.tabs(["📊 Ranking Geral", "👤 Dashboard Individual"])
    st.markdown('</div>', unsafe_allow_html=True)

    with tab1:
        st.markdown("### Ranking de Corretores")
        st.markdown(
            "Classificação baseada em pontos acumulados por produtividade")

        # Display ranking cards
        display_ranking_cards(data['ranking'])

        # Additional section for summary statistics
        st.markdown("### Estatísticas Gerais")

        # Create summary metrics
        col1, col2, col3, col4 = st.columns(4)

        # Calculate summary statistics
        total_leads = len(data['leads']) if not data['leads'].empty else 0
        active_brokers = len(
            data['ranking']) if not data['ranking'].empty else 0

        # Calculate average points and total sales from broker_points table
        if not data['ranking'].empty:
            avg_points = int(data['ranking']['pontos'].mean())
            total_sales = int(data['ranking']['vendas_realizadas'].sum(
            )) if 'vendas_realizadas' in data['ranking'].columns else 0
        else:
            avg_points = 0
            total_sales = 0

        with col1:
            st.metric("Total de Leads", total_leads)
        with col2:
            st.metric("Corretores Ativos", active_brokers)
        with col3:
            st.metric("Pontuação Média", avg_points)
        with col4:
            st.metric("Vendas Realizadas", total_sales)

    with tab2:
        # Select a broker
        if data['brokers'].empty:
            st.info("Nenhum corretor disponível no momento.")
            return

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
                                                     'display_name'].iloc[0])

        if selected_broker:
            # Get broker details
            broker = data['brokers'][data['brokers']['id'] ==
                                     selected_broker].iloc[0]
            broker_leads = data['leads'][data['leads']['responsavel_id'] ==
                                         selected_broker]

            # Display broker header with ranking position
            broker_rank = data['ranking'][
                data['ranking']['id'] ==
                selected_broker].index[0] + 1 if not data[
                    'ranking'].empty and selected_broker in data['ranking'][
                        'id'].values else "N/A"
            broker_points = int(
                data['ranking'][data['ranking']['id'] ==
                                selected_broker]['pontos'].values[0]
            ) if not data['ranking'].empty and selected_broker in data[
                'ranking']['id'].values else 0

            # Header with rank and points
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

            # Display performance metrics
            st.markdown('<div class="card-title">Métricas de Desempenho</div>',
                        unsafe_allow_html=True)
            display_broker_metrics(selected_broker, data)

            # Create two columns for the charts
            col1, col2 = st.columns(2)

            with col1:
                # Display conversion funnel
                st.markdown('<div class="card-title">Funil de Conversão</div>',
                            unsafe_allow_html=True)
                funnel_fig = create_styled_conversion_funnel(broker_leads)
                st.plotly_chart(funnel_fig, use_container_width=True)

                # Display alerts
                st.markdown('<div class="card-title">Alertas</div>',
                            unsafe_allow_html=True)
                display_broker_alerts(selected_broker, data)

            with col2:
                # Display activity heatmap
                st.markdown(
                    '<div class="card-title">Mapa de Calor - Atividades</div>',
                    unsafe_allow_html=True)
                display_activity_heatmap(selected_broker, data)

                # Display points breakdown
                st.markdown(
                    '<div class="card-title">Distribuição de Pontos</div>',
                    unsafe_allow_html=True)
                display_points_breakdown(selected_broker, data)


if __name__ == "__main__":
    # background_data_loader_once()
    main()
