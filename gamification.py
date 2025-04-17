import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_broker_points(broker_data, lead_data, activity_data):
    """
    Calculate points for each broker based on enhanced gamification rules

    Gamification rules (positive points):
    - Lead respondido em até 1 hora (+2 pts)
    - Lead visitado (+5 pts)
    - Proposta enviada (+8 pts)
    - Venda realizada (+15 pts)
    - Lead atualizado no CRM no mesmo dia (+2 pts)
    - Feedback positivo do gestor (+3 pts)
    - Resposta ao cliente em menos de 3 horas (+4 pts)
    - Resposta a todos os leads do dia (+5 pts)
    - Cadastro completo do lead com todas as informações (+3 pts)
    - Acompanhamento pós-venda registrado (+10 pts)

    Gamification rules (negative points):
    - Lead sem interação há mais de 24h (-3 pts)
    - Lead ignorado por mais de 48h (-5 pts)
    - Lead com reclamação registrada (-4 pts)
    - Perda de lead para concorrente (-6 pts)

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

        # Initialize metrics (positive)
        points_df['leads_respondidos_1h'] = 0
        points_df['leads_visitados'] = 0
        points_df['propostas_enviadas'] = 0
        points_df['vendas_realizadas'] = 0
        points_df['leads_atualizados_mesmo_dia'] = 0
        points_df['feedbacks_positivos'] = 0
        points_df['resposta_rapida_3h'] = 0
        points_df['todos_leads_respondidos'] = 0
        points_df['cadastro_completo'] = 0
        points_df['acompanhamento_pos_venda'] = 0

        # Initialize metrics (negative)
        points_df['leads_sem_interacao_24h'] = 0
        points_df['leads_ignorados_48h'] = 0
        points_df['leads_com_reclamacao'] = 0
        points_df['leads_perdidos'] = 0

        # Initialize alert metrics (no points deduction, just for display)
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

            # ===== POSITIVE POINTS =====

            # Rule: Lead respondido em até 1 hora (+2 pts)
            # Count message response activities within 1 hour of lead creation
            if not broker_leads.empty and not broker_activities.empty:
                leads_responded_1h = 0
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
                                leads_responded_1h += 1

                points_df.at[idx, 'leads_respondidos_1h'] = leads_responded_1h
                points_df.at[idx, 'pontos'] += leads_responded_1h * 2

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

            # Rule: Resposta ao cliente em menos de 3 horas (+4 pts)
            # Count quick responses to client messages
            if not broker_leads.empty and not broker_activities.empty:
                quick_responses = 0
                for _, lead in broker_leads.iterrows():
                    lead_id = lead['id']

                    # Find message activities for this lead
                    lead_messages = broker_activities[
                        (broker_activities['lead_id'] == lead_id) & 
                        (broker_activities['tipo'].isin(['mensagem_recebida', 'mensagem_enviada']))
                    ]

                    if not lead_messages.empty:
                        # Sort by time
                        lead_messages = lead_messages.sort_values('criado_em')

                        # Check for quick responses
                        for i in range(1, len(lead_messages)):
                            prev_msg = lead_messages.iloc[i-1]
                            curr_msg = lead_messages.iloc[i]

                            if prev_msg['tipo'] == 'mensagem_recebida' and curr_msg['tipo'] == 'mensagem_enviada':
                                response_time = (curr_msg['criado_em'] - prev_msg['criado_em']).total_seconds() / 3600
                                if response_time < 3:  # Less than 3 hours
                                    quick_responses += 1

                points_df.at[idx, 'resposta_rapida_3h'] = quick_responses
                points_df.at[idx, 'pontos'] += quick_responses * 4

            # Rule: Resposta a todos os leads do dia (+5 pts)
            # Check if broker responded to all leads received today
            today_leads = broker_leads[broker_leads['criado_em'].dt.date == now.date()] if not broker_leads.empty else pd.DataFrame()

            if not today_leads.empty:
                all_responded = True
                for _, lead in today_leads.iterrows():
                    lead_id = lead['id']

                    # Find responses for this lead
                    responses = broker_activities[
                        (broker_activities['lead_id'] == lead_id) & 
                        (broker_activities['tipo'] == 'mensagem_enviada')
                    ]

                    if responses.empty:
                        all_responded = False
                        break

                if all_responded and len(today_leads) > 0:
                    points_df.at[idx, 'todos_leads_respondidos'] = 1
                    points_df.at[idx, 'pontos'] += 5

            # Rule: Cadastro completo do lead com todas as informações (+3 pts)
            # Check for leads with complete information
            if not broker_leads.empty:
                complete_leads = 0
                for _, lead in broker_leads.iterrows():
                    # Check if lead has all required fields (simplified check for example)
                    if lead['nome'] and lead['contato_nome'] and not pd.isna(lead['valor']):
                        complete_leads += 1

                points_df.at[idx, 'cadastro_completo'] = complete_leads
                points_df.at[idx, 'pontos'] += complete_leads * 3

            # Rule: Acompanhamento pós-venda registrado (+10 pts)
            # Look for follow-up activities after sale
            if not broker_leads.empty and not broker_activities.empty:
                post_sale_followups = 0
                closed_deals = broker_leads[broker_leads['status'] == 'Ganho']

                for _, deal in closed_deals.iterrows():
                    deal_id = deal['id']
                    deal_closed_date = deal['atualizado_em']

                    if deal_closed_date:
                        # Find activities after deal was closed
                        followup_activities = broker_activities[
                            (broker_activities['lead_id'] == deal_id) & 
                            (broker_activities['criado_em'] > deal_closed_date)
                        ]

                        if not followup_activities.empty:
                            post_sale_followups += 1

                points_df.at[idx, 'acompanhamento_pos_venda'] = post_sale_followups
                points_df.at[idx, 'pontos'] += post_sale_followups * 10

            # ===== NEGATIVE POINTS =====

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

            # Rule: Lead ignorado por mais de 48h (-5 pts)
            # Count leads ignored for more than 48 hours
            if not broker_leads.empty and not broker_activities.empty:
                ignored_leads = 0
                for _, lead in broker_leads.iterrows():
                    lead_id = lead['id']

                    # Skip closed leads
                    if lead['fechado']:
                        continue

                    # Find activities for this lead
                    lead_activities = broker_activities[broker_activities['lead_id'] == lead_id]

                    # If no activities or last activity was more than 48 hours ago
                    if lead_activities.empty:
                        if lead['criado_em'] and (now - lead['criado_em']).total_seconds() > 172800:  # 48 hours
                            ignored_leads += 1
                    else:
                        last_activity_time = lead_activities['criado_em'].max()
                        if (now - last_activity_time).total_seconds() > 172800:  # 48 hours
                            ignored_leads += 1

                points_df.at[idx, 'leads_ignorados_48h'] = ignored_leads
                points_df.at[idx, 'pontos'] -= ignored_leads * 5

            # Rule: Lead com reclamação registrada (-4 pts)
            # Look for activities indicating complaints
            if not broker_activities.empty:
                complaint_count = len(broker_activities[
                    broker_activities['tipo'].str.contains('reclamação|problema|insatisfação', na=False, case=False) |
                    broker_activities['valor_novo'].str.contains('reclamação|problema|insatisfação', na=False, case=False)
                ])

                points_df.at[idx, 'leads_com_reclamacao'] = complaint_count
                points_df.at[idx, 'pontos'] -= complaint_count * 4

            # Rule: Perda de lead para concorrente (-6 pts)
            # Count leads lost to competitors
            if not broker_leads.empty:
                lost_leads = len(broker_leads[
                    (broker_leads['status'] == 'Perdido') & 
                    ((broker_leads['etapa'].str.contains('concorrente', na=False, case=False)) |
                     (broker_leads['status_id'].isin([31, 143, 142])))  # IDs for lost to competitor status
                ])

                points_df.at[idx, 'leads_perdidos'] = lost_leads
                points_df.at[idx, 'pontos'] -= lost_leads * 6

            # ===== ALERTS (no points impact) =====

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
    

        logger.info("Broker points calculation completed")
        return points_df

    except Exception as e:
        logger.error(f"Error calculating broker points: {str(e)}")
        raise