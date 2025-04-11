import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_broker_points(broker_data, lead_data, activity_data):
    """
    Calculate points for each broker based on gamification rules
    
    Gamification rules:
    - Lead respondido em até 1 hora (+2 pts)
    - Lead visitado (+5 pts)
    - Proposta enviada (+8 pts)
    - Venda realizada (+15 pts)
    - Lead atualizado no CRM no mesmo dia (+2 pts)
    - Feedback positivo do gestor (+3 pts)
    - Lead sem interação há mais de 24h (-3 pts)
    
    Args:
        broker_data (pd.DataFrame): DataFrame with broker information
        lead_data (pd.DataFrame): DataFrame with lead information
        activity_data (pd.DataFrame): DataFrame with activity information
    
    Returns:
        pd.DataFrame: DataFrame with calculated points for each broker
    """
    try:
        logger.info("Calculating broker points based on gamification rules")
        
        # Create a new DataFrame to store the points
        points_df = broker_data[['id', 'nome']].copy()
        points_df['pontos'] = 0
        
        # Initialize metrics
        points_df['leads_respondidos_1h'] = 0
        points_df['leads_visitados'] = 0
        points_df['propostas_enviadas'] = 0
        points_df['vendas_realizadas'] = 0
        points_df['leads_atualizados_mesmo_dia'] = 0
        points_df['feedbacks_positivos'] = 0
        points_df['leads_sem_interacao_24h'] = 0
        points_df['leads_respondidos_apos_18h'] = 0
        points_df['leads_tempo_resposta_acima_12h'] = 0
        points_df['leads_5_dias_sem_mudanca'] = 0
        
        # Current time for comparison
        now = datetime.now()
        
        # Calculate points for each broker
        for idx, broker in points_df.iterrows():
            broker_id = broker['id']
            
            # Get leads assigned to this broker
            broker_leads = lead_data[lead_data['responsavel_id'] == broker_id]
            
            # Get activities performed by this broker
            broker_activities = activity_data[activity_data['user_id'] == broker_id]
            
            # ===== Positive indicators =====
            
            # Rule: Lead respondido em até 1 hora (+2 pts)
            # Count message response activities within 1 hour of lead creation
            if not broker_leads.empty and not broker_activities.empty:
                for _, lead in broker_leads.iterrows():
                    lead_id = lead['id']
                    lead_created = lead['criado_em']
                    
                    if lead_created:
                        # Find first message response for this lead
                        lead_responses = broker_activities[
                            (broker_activities['lead_id'] == lead_id) & 
                            (broker_activities['tipo'] == 'mensagem_enviada')
                        ]
                        
                        if not lead_responses.empty:
                            first_response = lead_responses.sort_values('criado_em').iloc[0]
                            response_time = first_response['criado_em'] - lead_created
                            
                            # If response within 1 hour
                            if response_time.total_seconds() <= 3600:  # 1 hour in seconds
                                points_df.at[idx, 'leads_respondidos_1h'] += 1
                                points_df.at[idx, 'pontos'] += 2
            
            # Rule: Lead visitado (+5 pts)
            # Look for status changes indicating a visit
            visit_activities = broker_activities[
                (broker_activities['tipo'] == 'mudança_status') & 
                (broker_activities['valor_novo'].str.contains('Visitado|Visita|Agendado', na=False, case=False))
            ]
            visit_count = len(visit_activities)
            points_df.at[idx, 'leads_visitados'] = visit_count
            points_df.at[idx, 'pontos'] += visit_count * 5
            
            # Rule: Proposta enviada (+8 pts)
            # Look for status changes indicating a proposal
            proposal_activities = broker_activities[
                (broker_activities['tipo'] == 'mudança_status') & 
                (broker_activities['valor_novo'].str.contains('Proposta|Contrato', na=False, case=False))
            ]
            proposal_count = len(proposal_activities)
            points_df.at[idx, 'propostas_enviadas'] = proposal_count
            points_df.at[idx, 'pontos'] += proposal_count * 8
            
            # Rule: Venda realizada (+15 pts)
            # Count closed deals
            sales_count = len(broker_leads[broker_leads['status'] == 'Ganho'])
            points_df.at[idx, 'vendas_realizadas'] = sales_count
            points_df.at[idx, 'pontos'] += sales_count * 15
            
            # Rule: Lead atualizado no CRM no mesmo dia (+2 pts)
            # Count leads updated on the same day they were created
            if not broker_leads.empty:
                same_day_updates = 0
                for _, lead in broker_leads.iterrows():
                    if lead['criado_em'] and lead['atualizado_em']:
                        if lead['criado_em'].date() == lead['atualizado_em'].date():
                            same_day_updates += 1
                
                points_df.at[idx, 'leads_atualizados_mesmo_dia'] = same_day_updates
                points_df.at[idx, 'pontos'] += same_day_updates * 2
            
            # ===== Negative indicators =====
            
            # Rule: Lead sem interação há mais de 24h (-3 pts)
            # Count leads with no activity in the last 24 hours
            if not broker_leads.empty and not broker_activities.empty:
                no_interaction_count = 0
                for _, lead in broker_leads.iterrows():
                    lead_id = lead['id']
                    
                    # Skip closed leads
                    if lead['fechado']:
                        continue
                    
                    # Find last activity for this lead
                    lead_activities = broker_activities[broker_activities['lead_id'] == lead_id]
                    
                    if lead_activities.empty:
                        # If no activities, use lead creation date
                        last_activity_time = lead['criado_em']
                    else:
                        last_activity_time = lead_activities['criado_em'].max()
                    
                    # If last activity was more than 24 hours ago
                    if last_activity_time and (now - last_activity_time).total_seconds() > 86400:  # 24 hours in seconds
                        no_interaction_count += 1
                
                points_df.at[idx, 'leads_sem_interacao_24h'] = no_interaction_count
                points_df.at[idx, 'pontos'] -= no_interaction_count * 3
            
            # Alert: Lead respondido só após 18h (alerta)
            if not broker_leads.empty and not broker_activities.empty:
                late_response_count = 0
                for _, lead in broker_leads.iterrows():
                    lead_id = lead['id']
                    lead_created = lead['criado_em']
                    
                    if lead_created:
                        # Find first message response for this lead
                        lead_responses = broker_activities[
                            (broker_activities['lead_id'] == lead_id) & 
                            (broker_activities['tipo'] == 'mensagem_enviada')
                        ]
                        
                        if not lead_responses.empty:
                            first_response = lead_responses.sort_values('criado_em').iloc[0]
                            response_time = first_response['criado_em'] - lead_created
                            
                            # If response after 18 hours
                            if response_time.total_seconds() > 64800:  # 18 hours in seconds
                                late_response_count += 1
                
                points_df.at[idx, 'leads_respondidos_apos_18h'] = late_response_count
            
            # Alert: Lead com tempo médio de resposta acima de 12h (alerta)
            if not broker_leads.empty and not broker_activities.empty:
                slow_response_count = 0
                for _, lead in broker_leads.iterrows():
                    lead_id = lead['id']
                    
                    # Find message activities for this lead
                    lead_messages = broker_activities[
                        (broker_activities['lead_id'] == lead_id) & 
                        (broker_activities['tipo'].isin(['mensagem_recebida', 'mensagem_enviada']))
                    ]
                    
                    if not lead_messages.empty and len(lead_messages) >= 2:
                        # Group by type and sort by time
                        lead_messages = lead_messages.sort_values('criado_em')
                        
                        # Calculate response times
                        response_times = []
                        last_received = None
                        
                        for _, msg in lead_messages.iterrows():
                            if msg['tipo'] == 'mensagem_recebida':
                                last_received = msg['criado_em']
                            elif msg['tipo'] == 'mensagem_enviada' and last_received is not None:
                                response_time = (msg['criado_em'] - last_received).total_seconds() / 3600  # hours
                                response_times.append(response_time)
                                last_received = None
                        
                        # Calculate average response time
                        if response_times:
                            avg_response_time = sum(response_times) / len(response_times)
                            
                            # If average response time > 12 hours
                            if avg_response_time > 12:
                                slow_response_count += 1
                
                points_df.at[idx, 'leads_tempo_resposta_acima_12h'] = slow_response_count
            
            # Alert: Leads com mais de 5 dias sem mudança de etapa (alerta)
            if not broker_leads.empty:
                stale_leads_count = 0
                for _, lead in broker_leads.iterrows():
                    # Skip closed leads
                    if lead['fechado']:
                        continue
                    
                    # If lead was last updated more than 5 days ago
                    if lead['atualizado_em'] and (now - lead['atualizado_em']).days > 5:
                        stale_leads_count += 1
                
                points_df.at[idx, 'leads_5_dias_sem_mudanca'] = stale_leads_count
        
        # Ensure points are not negative
        points_df['pontos'] = points_df['pontos'].apply(lambda x: max(0, x))
        
        logger.info("Broker points calculation completed")
        return points_df
    
    except Exception as e:
        logger.error(f"Error calculating broker points: {str(e)}")
        raise
