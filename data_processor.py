import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_data(brokers, leads, activities):
    """
    Process and clean data from Kommo API for dashboard use
    
    Args:
        brokers (pd.DataFrame): Raw broker data from Kommo API
        leads (pd.DataFrame): Raw lead data from Kommo API
        activities (pd.DataFrame): Raw activity data from Kommo API
    
    Returns:
        tuple: (processed_brokers, processed_leads, processed_activities)
    """
    try:
        logger.info("Processing data for dashboard use")
        
        # Make copies to avoid modifying the original DataFrames
        processed_brokers = brokers.copy() if not brokers.empty else pd.DataFrame()
        processed_leads = leads.copy() if not leads.empty else pd.DataFrame()
        processed_activities = activities.copy() if not activities.empty else pd.DataFrame()
        
        # ===== Process broker data =====
        if not processed_brokers.empty:
            # Ensure required columns exist
            if 'id' not in processed_brokers.columns:
                processed_brokers['id'] = processed_brokers.index
            
            if 'nome' not in processed_brokers.columns:
                processed_brokers['nome'] = "Corretor " + processed_brokers['id'].astype(str)
            
            # Add default photo URL if missing
            if 'foto_url' not in processed_brokers.columns:
                processed_brokers['foto_url'] = None
            
            # Filter out non-broker users if role information is available
            if 'cargo' in processed_brokers.columns:
                processed_brokers = processed_brokers[
                    processed_brokers['cargo'].str.contains('Corretor|Vendedor|Agente', na=False, case=False)
                ]
        else:
            # Create empty DataFrame with required columns
            processed_brokers = pd.DataFrame(columns=['id', 'nome', 'email', 'foto_url', 'cargo'])
        
        # ===== Process lead data =====
        if not processed_leads.empty:
            # Convert string dates to datetime if needed
            date_columns = ['criado_em', 'atualizado_em']
            for col in date_columns:
                if col in processed_leads.columns:
                    if processed_leads[col].dtype == 'object':
                        processed_leads[col] = pd.to_datetime(processed_leads[col], errors='coerce')
            
            # Categorize lead stages
            if 'etapa' in processed_leads.columns:
                # Map stages to standardized categories
                stage_mapping = {
                    # Initial contact
                    'Novo': 'Contato Inicial',
                    'Qualificação': 'Contato Inicial',
                    'Primeiro Contato': 'Contato Inicial',
                    
                    # Visit
                    'Agendamento': 'Visita',
                    'Visita Agendada': 'Visita',
                    'Visitado': 'Visita',
                    
                    # Proposal
                    'Proposta': 'Proposta',
                    'Contrato': 'Proposta',
                    'Negociação': 'Proposta',
                    
                    # Closed
                    'Ganho': 'Venda',
                    'Fechado': 'Venda',
                    'Venda': 'Venda',
                    'Perdido': 'Perdido'
                }
                
                # Apply mapping with a default value for unknown stages
                processed_leads['etapa_categoria'] = processed_leads['etapa'].map(
                    lambda x: next((v for k, v in stage_mapping.items() if k.lower() in str(x).lower()), 'Contato Inicial')
                )
        else:
            # Create empty DataFrame with required columns
            processed_leads = pd.DataFrame(columns=[
                'id', 'nome', 'responsavel_id', 'etapa', 'etapa_categoria', 'status',
                'criado_em', 'atualizado_em', 'fechado'
            ])
        
        # ===== Process activity data =====
        if not processed_activities.empty:
            # Convert string dates to datetime if needed
            if 'criado_em' in processed_activities.columns and processed_activities['criado_em'].dtype == 'object':
                processed_activities['criado_em'] = pd.to_datetime(processed_activities['criado_em'], errors='coerce')
            
            # Extract day of week and hour for heatmap
            if 'criado_em' in processed_activities.columns:
                # Map Portuguese day names
                day_mapping = {
                    'Monday': 'Segunda',
                    'Tuesday': 'Terça',
                    'Wednesday': 'Quarta',
                    'Thursday': 'Quinta',
                    'Friday': 'Sexta',
                    'Saturday': 'Sábado',
                    'Sunday': 'Domingo'
                }
                
                processed_activities['dia_semana'] = processed_activities['criado_em'].dt.day_name().map(day_mapping)
                processed_activities['hora'] = processed_activities['criado_em'].dt.hour
            
            # Ensure all activities have a lead_id
            if 'lead_id' not in processed_activities.columns:
                processed_activities['lead_id'] = None
        else:
            # Create empty DataFrame with required columns
            processed_activities = pd.DataFrame(columns=[
                'id', 'lead_id', 'user_id', 'tipo', 'criado_em', 'dia_semana', 'hora'
            ])
        
        logger.info("Data processing completed successfully")
        return processed_brokers, processed_leads, processed_activities
    
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise
