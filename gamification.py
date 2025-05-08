import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_broker_points(broker_data, lead_data, activity_data, rules, company_id=None):
    """
    Calculate points for each broker based on dynamic rules loaded from Supabase
    """
    try:
        logger.info("Calculating broker points based on dynamic rules")
        
        if company_id is None and 'company_id' in broker_data.columns:
            company_id = broker_data['company_id'].iloc[0]

        def check_broker_activity(activities, now):
            """Verifica se houve atividade no horário comercial"""
            if activities.empty:
                return False

            # Converte o horário atual para apenas hora
            current_time = now.time()

            # Define os períodos de trabalho
            morning_start = datetime.strptime('08:30', '%H:%M').time()
            morning_end = datetime.strptime('12:00', '%H:%M').time()
            afternoon_start = datetime.strptime('13:30', '%H:%M').time()
            afternoon_end = datetime.strptime('18:00', '%H:%M').time()

            # Filtra atividades das últimas 3 horas em dias úteis
            three_hours_ago = now - timedelta(hours=3)
            recent_activities = activities[
                (activities['criado_em'] >= three_hours_ago) & 
                (activities['criado_em'].dt.weekday < 5)  # Apenas dias úteis (segunda a sexta)
            ]

            if recent_activities.empty:
                # Verifica se estamos no horário comercial
                is_working_hours = (
                    (morning_start <= current_time <= morning_end) or
                    (afternoon_start <= current_time <= afternoon_end)
                )
                return not is_working_hours

            return False

        # Create a new DataFrame to store the points
        points_df = broker_data[['id', 'nome']].copy()
        points_df['pontos'] = 0
        points_df['corretor_ocioso_mais_de_3h'] = 0
        points_df['company_id'] = company_id

        # Initialize all rule columns with zero
        for rule_name in rules.keys():
            points_df[rule_name] = 0

        # Current time for comparison
        now = datetime.now()

        # Calculate points for each broker
        for idx, broker in points_df.iterrows():
            broker_id = broker['id']

            # Get leads and activities for this broker
            broker_leads = lead_data[lead_data['responsavel_id'] == broker_id]
            broker_activities = activity_data[activity_data['user_id'] == broker_id]

            # Verifica ociosidade
            is_idle = check_broker_activity(broker_activities, now)
            points_df.at[idx, 'corretor_ocioso_mais_de_3h'] = 1 if is_idle else 0

            if not broker_leads.empty and not broker_activities.empty:
                # Lead respondido em até 1 hora
                if 'leads_respondidos_1h' in rules:
                    leads_responded_1h = 0
                    for _, lead in broker_leads.iterrows():
                        lead_responses = broker_activities[
                            (broker_activities['lead_id'] == lead['id']) & 
                            (broker_activities['tipo'] == 'mensagem_enviada')
                        ]
                        if not lead_responses.empty:
                            first_response = lead_responses.sort_values('criado_em').iloc[0]
                            if (first_response['criado_em'] - lead['criado_em']).total_seconds() <= 3600:
                                leads_responded_1h += 1
                    points_df.at[idx, 'leads_respondidos_1h'] = leads_responded_1h
                    points_df.at[idx, 'pontos'] += leads_responded_1h * rules['leads_respondidos_1h']

                # Lead visitado
                if 'leads_visitados' in rules:
                    visit_activities = broker_activities[
                        (broker_activities['tipo'] == 'mudança_status') & 
                        (broker_activities['valor_novo'].str.contains('Visitado|Visita|Agendado', na=False, case=False))
                    ]
                    visit_count = len(visit_activities)
                    points_df.at[idx, 'leads_visitados'] = visit_count
                    points_df.at[idx, 'pontos'] += visit_count * rules['leads_visitados']

                # Proposta enviada
                if 'propostas_enviadas' in rules:
                    proposal_activities = broker_activities[
                        (broker_activities['tipo'] == 'mudança_status') & 
                        (broker_activities['valor_novo'].str.contains('Proposta|Contrato', na=False, case=False))
                    ]
                    proposal_count = len(proposal_activities)
                    points_df.at[idx, 'propostas_enviadas'] = proposal_count
                    points_df.at[idx, 'pontos'] += proposal_count * rules['propostas_enviadas']

                # Venda realizada
                if 'vendas_realizadas' in rules:
                    sales_count = len(broker_leads[broker_leads['status'] == 'Ganho'])
                    points_df.at[idx, 'vendas_realizadas'] = sales_count
                    points_df.at[idx, 'pontos'] += sales_count * rules['vendas_realizadas']

                # Lead atualizado no CRM no mesmo dia
                if 'leads_atualizados_mesmo_dia' in rules:
                    same_day_updates = 0
                    for _, lead in broker_leads.iterrows():
                        if lead['criado_em'] and lead['atualizado_em']:
                            if lead['criado_em'].date() == lead['atualizado_em'].date():
                                same_day_updates += 1
                    points_df.at[idx, 'leads_atualizados_mesmo_dia'] = same_day_updates
                    points_df.at[idx, 'pontos'] += same_day_updates * rules['leads_atualizados_mesmo_dia']

                # Resposta ao cliente em menos de 3 horas
                if 'resposta_rapida_3h' in rules:
                    quick_responses = 0
                    for _, lead in broker_leads.iterrows():
                        lead_messages = broker_activities[
                            (broker_activities['lead_id'] == lead['id']) & 
                            (broker_activities['tipo'].isin(['mensagem_recebida', 'mensagem_enviada']))
                        ]
                        if not lead_messages.empty:
                            lead_messages = lead_messages.sort_values('criado_em')
                            for i in range(1, len(lead_messages)):
                                prev_msg = lead_messages.iloc[i-1]
                                curr_msg = lead_messages.iloc[i]
                                if prev_msg['tipo'] == 'mensagem_recebida' and curr_msg['tipo'] == 'mensagem_enviada':
                                    response_time = (curr_msg['criado_em'] - prev_msg['criado_em']).total_seconds() / 3600
                                    if response_time < 3:
                                        quick_responses += 1
                    points_df.at[idx, 'resposta_rapida_3h'] = quick_responses
                    points_df.at[idx, 'pontos'] += quick_responses * rules['resposta_rapida_3h']

                # Resposta a todos os leads do dia
                if 'todos_leads_respondidos' in rules:
                    today_leads = broker_leads[broker_leads['criado_em'].dt.date == now.date()] if not broker_leads.empty else pd.DataFrame()
                    if not today_leads.empty:
                        all_responded = True
                        for _, lead in today_leads.iterrows():
                            responses = broker_activities[
                                (broker_activities['lead_id'] == lead['id']) & 
                                (broker_activities['tipo'] == 'mensagem_enviada')
                            ]
                            if responses.empty:
                                all_responded = False
                                break
                        if all_responded and len(today_leads) > 0:
                            points_df.at[idx, 'todos_leads_respondidos'] = 1
                            points_df.at[idx, 'pontos'] += rules['todos_leads_respondidos']

                # Cadastro completo do lead com todas as informações
                if 'cadastro_completo' in rules:
                    complete_leads = 0
                    for _, lead in broker_leads.iterrows():
                        if lead['nome'] and lead['contato_nome'] and not pd.isna(lead['valor']):
                            complete_leads += 1
                    points_df.at[idx, 'cadastro_completo'] = complete_leads
                    points_df.at[idx, 'pontos'] += complete_leads * rules['cadastro_completo']

                # Acompanhamento pós-venda registrado
                if 'acompanhamento_pos_venda' in rules:
                    post_sale_followups = 0
                    closed_deals = broker_leads[broker_leads['status'] == 'Ganho']
                    for _, deal in closed_deals.iterrows():
                        deal_closed_date = deal['atualizado_em']
                        if deal_closed_date:
                            followup_activities = broker_activities[
                                (broker_activities['lead_id'] == deal['id']) & 
                                (broker_activities['criado_em'] > deal_closed_date)
                            ]
                            if not followup_activities.empty:
                                post_sale_followups += 1
                    points_df.at[idx, 'acompanhamento_pos_venda'] = post_sale_followups
                    points_df.at[idx, 'pontos'] += post_sale_followups * rules['acompanhamento_pos_venda']

                # Lead sem interação há mais de 24h
                if 'leads_sem_interacao_24h' in rules:
                    no_interaction_count = 0
                    for _, lead in broker_leads.iterrows():
                        if lead['fechado']:
                            continue
                        lead_activities = broker_activities[broker_activities['lead_id'] == lead['id']]
                        last_activity_time = lead_activities['criado_em'].max() if not lead_activities.empty else lead['criado_em']
                        if last_activity_time and (now - last_activity_time).total_seconds() > 86400:
                            no_interaction_count += 1
                    points_df.at[idx, 'leads_sem_interacao_24h'] = no_interaction_count
                    points_df.at[idx, 'pontos'] -= no_interaction_count * rules['leads_sem_interacao_24h']

                # Lead ignorado por mais de 48h
                if 'leads_ignorados_48h' in rules:
                    ignored_leads = 0
                    for _, lead in broker_leads.iterrows():
                        if lead['fechado']:
                            continue
                        lead_activities = broker_activities[broker_activities['lead_id'] == lead['id']]
                        if lead_activities.empty:
                            if lead['criado_em'] and (now - lead['criado_em']).total_seconds() > 172800:
                                ignored_leads += 1
                        else:
                            last_activity_time = lead_activities['criado_em'].max()
                            if (now - last_activity_time).total_seconds() > 172800:
                                ignored_leads += 1
                    points_df.at[idx, 'leads_ignorados_48h'] = ignored_leads
                    points_df.at[idx, 'pontos'] -= ignored_leads * rules['leads_ignorados_48h']

                # Perda de lead para concorrente
                if 'leads_perdidos' in rules:
                    lost_leads = len(broker_leads[
                        (broker_leads['status'] == 'Perdido') & 
                        ((broker_leads['etapa'].str.contains('concorrente', na=False, case=False)) |
                         (broker_leads['status_id'].isin([31, 143, 142])))
                    ])
                    points_df.at[idx, 'leads_perdidos'] = lost_leads
                    points_df.at[idx, 'pontos'] -= lost_leads * rules['leads_perdidos']

                # Feedback positivo do gestor
                if 'feedbacks_positivos' in rules:
                    feedback_count = len(broker_activities[(broker_activities['tipo'] == 'feedback_positivo')])
                    points_df.at[idx, 'feedbacks_positivos'] = feedback_count
                    points_df.at[idx, 'pontos'] += feedback_count * rules['feedbacks_positivos']

        logger.info("Broker points calculation completed")
        return points_df

    except Exception as e:
        logger.error(f"Error calculating broker points: {str(e)}")
        raise