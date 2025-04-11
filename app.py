import os
import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
from dotenv import load_dotenv

# Import custom modules
from kommo_api import KommoAPI
from supabase_db import SupabaseClient
from gamification import calculate_broker_points
from data_processor import process_data
from visualizations import create_heatmap, create_conversion_funnel

# Load environment variables
load_dotenv()

# Initialize Streamlit page configuration
st.set_page_config(
    page_title="Dashboard de Corretores Imobiliários",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize API and database clients
@st.cache_resource
def init_clients():
    kommo_api = KommoAPI(
        api_url=os.getenv("KOMMO_API_URL"),
        access_token=os.getenv("ACCESS_TOKEN_KOMMO")
    )
    
    supabase = SupabaseClient(
        url=os.getenv("VITE_SUPABASE_URL"),
        key=os.getenv("VITE_SUPABASE_ANON_KEY")
    )
    
    return kommo_api, supabase

kommo_api, supabase = init_clients()

# Function to load data with caching
@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_data():
    try:
        with st.spinner("Carregando dados do Kommo CRM..."):
            # Set a timeout for loading data (120 seconds)
            start_time = datetime.now()
            timeout_seconds = 120
            
            # Fetch data from Kommo API
            st.text("Buscando usuários...")
            brokers = kommo_api.get_users()
            
            # Check for timeout
            if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                st.error("Tempo limite excedido ao carregar dados. Tente novamente mais tarde.")
                return None
                
            st.text("Buscando leads...")
            leads = kommo_api.get_leads()
            
            # Check for timeout
            if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                st.error("Tempo limite excedido ao carregar dados. Tente novamente mais tarde.")
                return None
                
            st.text("Buscando atividades...")
            activities = kommo_api.get_activities()
            
            # Check for timeout
            if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                st.error("Tempo limite excedido ao carregar dados. Tente novamente mais tarde.")
                return None
            
            # Store data in Supabase (optional - only if time permits)
            if (datetime.now() - start_time).total_seconds() < timeout_seconds - 30:
                st.text("Armazenando dados no banco...")
                try:
                    supabase.upsert_brokers(brokers)
                    supabase.upsert_leads(leads)
                    supabase.upsert_activities(activities)
                except Exception as e:
                    st.warning(f"Alerta: Falha ao salvar dados no banco: {str(e)}")
                    # Continue with processing even if database storage fails
            
            # Process data for dashboard
            st.text("Processando dados...")
            broker_data, lead_data, activity_data = process_data(brokers, leads, activities)
            
            # Calculate broker points based on gamification rules
            st.text("Calculando pontuação...")
            ranking_data = calculate_broker_points(broker_data, lead_data, activity_data)
            
            st.success("Dados carregados com sucesso!")
            return {
                'brokers': broker_data,
                'leads': lead_data,
                'activities': activity_data,
                'ranking': ranking_data
            }
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return None

# Function to display broker ranking
def display_ranking(ranking_data):
    st.header("Ranking de Corretores")
    
    # Sort brokers by points in descending order
    sorted_brokers = ranking_data.sort_values(by='pontos', ascending=False).reset_index(drop=True)
    sorted_brokers.index = sorted_brokers.index + 1  # Start index from 1
    
    # Create ranking columns
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Display ranking table
        ranking_display = sorted_brokers[['nome', 'pontos']].copy()
        ranking_display.index.name = 'posição'
        st.dataframe(
            ranking_display,
            use_container_width=True,
            column_config={
                "nome": "Corretor",
                "pontos": "Pontuação"
            },
            hide_index=False
        )

# Function to display broker details
def display_broker_details(broker_id, data):
    broker = data['brokers'][data['brokers']['id'] == broker_id].iloc[0]
    broker_leads = data['leads'][data['leads']['responsavel_id'] == broker_id]
    broker_activities = data['activities'][data['activities']['user_id'] == broker_id]
    
    st.subheader(f"Dashboard Individual: {broker['nome']}")
    
    # Create columns for broker information
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.metric("Pontuação Total", data['ranking'][data['ranking']['id'] == broker_id]['pontos'].values[0])
        st.metric("Posição no Ranking", int(data['ranking'][data['ranking']['id'] == broker_id].index[0]) + 1)
        st.metric("Total de Leads", len(broker_leads))
    
    with col2:
        # Create conversion funnel
        funnel_fig = create_conversion_funnel(broker_leads)
        st.plotly_chart(funnel_fig, use_container_width=True)
    
    # Create heatmap of broker activities
    st.subheader("Mapa de Calor de Atividades")
    if not broker_activities.empty:
        heatmap_fig = create_heatmap(broker_activities)
        st.plotly_chart(heatmap_fig, use_container_width=True)
    else:
        st.info("Não há dados de atividades suficientes para gerar o mapa de calor.")
    
    # Display alerts
    st.subheader("Alertas")
    
    # Get alerts from ranking data
    alerts = []
    
    broker_row = data['ranking'][data['ranking']['id'] == broker_id]
    
    if not broker_row.empty:
        if broker_row['leads_sem_interacao_24h'].values[0] > 0:
            alerts.append(f"⚠️ {broker_row['leads_sem_interacao_24h'].values[0]} leads sem interação há mais de 24h")
        
        if broker_row['leads_respondidos_apos_18h'].values[0] > 0:
            alerts.append(f"⚠️ {broker_row['leads_respondidos_apos_18h'].values[0]} leads respondidos após 18h")
        
        if broker_row['leads_tempo_resposta_acima_12h'].values[0] > 0:
            alerts.append(f"⚠️ {broker_row['leads_tempo_resposta_acima_12h'].values[0]} leads com tempo médio de resposta acima de 12h")
        
        if broker_row['leads_5_dias_sem_mudanca'].values[0] > 0:
            alerts.append(f"⚠️ {broker_row['leads_5_dias_sem_mudanca'].values[0]} leads com mais de 5 dias sem mudança de etapa")
    
    if alerts:
        for alert in alerts:
            st.warning(alert)
    else:
        st.success("Não há alertas para este corretor.")

# Main application
def main():
    st.title("Dashboard de Corretores Imobiliários")
    
    # Add sidebar for controls
    with st.sidebar:
        st.header("Controles")
        refresh_btn = st.button("Atualizar Dados")
        
        if refresh_btn:
            st.cache_data.clear()
            st.rerun()
    
    # Load data
    data = load_data()
    
    if data is not None:
        # Display tabs for different views
        tab1, tab2 = st.tabs(["Ranking Geral", "Dashboard Individual"])
        
        with tab1:
            display_ranking(data['ranking'])
        
        with tab2:
            # Broker selection
            broker_options = data['brokers'][['id', 'nome']].copy()
            broker_options['display_name'] = broker_options['nome']
            
            selected_broker = st.selectbox(
                "Selecione um corretor",
                options=broker_options['id'].tolist(),
                format_func=lambda x: broker_options.loc[broker_options['id'] == x, 'display_name'].iloc[0]
            )
            
            if selected_broker:
                display_broker_details(selected_broker, data)
    else:
        st.error("Não foi possível carregar os dados. Verifique a conexão com a API e o banco de dados.")

if __name__ == "__main__":
    main()
