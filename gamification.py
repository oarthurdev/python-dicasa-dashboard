
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_broker_points(broker_data, lead_data, activity_data, rules, company_id=None):
    try:
        logger.info("Calculating broker points based on dynamic rules")

        # Define timezone de São Paulo
        sao_paulo_tz = pytz.timezone('America/Sao_Paulo')

        # Converte colunas de data para datetime com timezone
        lead_data['criado_em'] = pd.to_datetime(lead_data['criado_em'], utc=True).dt.tz_convert(sao_paulo_tz)
        lead_data['atualizado_em'] = pd.to_datetime(lead_data['atualizado_em'], utc=True).dt.tz_convert(sao_paulo_tz)
        activity_data['criado_em'] = pd.to_datetime(activity_data['criado_em'], utc=True).dt.tz_convert(sao_paulo_tz)

        if company_id is None and 'company_id' in broker_data.columns:
            company_id = broker_data['company_id'].iloc[0]

        points_df = broker_data[['id', 'nome']].copy()
        points_df['pontos'] = 0
        points_df['company_id'] = company_id

        # Inicializar todas as colunas de regras com 0
        for rule_name in rules.keys():
            points_df[rule_name] = 0

        now = datetime.now(sao_paulo_tz)

        for idx, broker in points_df.iterrows():
            broker_id = broker['id']
            broker_leads = lead_data[lead_data['responsavel_id'] == broker_id]
            broker_activities = activity_data[activity_data['user_id'] == broker_id]

            # 1. Leads respondidos em 1 hora (60 pontos)
            if 'leads_respondidos_1h' in rules:
                leads_responded_1h = 0
                for _, lead in broker_leads.iterrows():
                    # Buscar primeira mensagem enviada pelo broker para este lead
                    first_response = broker_activities[
                        (broker_activities['lead_id'] == lead['id']) & 
                        (broker_activities['tipo'] == 'mensagem_enviada')
                    ].sort_values('criado_em')
                    
                    if not first_response.empty:
                        response_time = (first_response.iloc[0]['criado_em'] - lead['criado_em']).total_seconds()
                        if response_time <= 3600:  # 1 hora = 3600 segundos
                            leads_responded_1h += 1
                            
                points_df.at[idx, 'leads_respondidos_1h'] = leads_responded_1h
                points_df.at[idx, 'pontos'] += leads_responded_1h * rules['leads_respondidos_1h']

            # 2. Leads visitados (40 pontos)
            if 'leads_visitados' in rules:
                # Contar mudanças de status para etapas que indicam visita
                visit_activities = broker_activities[
                    (broker_activities['tipo'] == 'mudança_status') & 
                    (broker_activities['valor_novo'].str.contains('Visita|Visitado|Agendamento|Apresentação', na=False, case=False))
                ]
                visit_count = len(visit_activities.groupby('lead_id'))  # Contar leads únicos visitados
                points_df.at[idx, 'leads_visitados'] = visit_count
                points_df.at[idx, 'pontos'] += visit_count * rules['leads_visitados']

            # 3. Propostas enviadas (8 pontos)
            if 'propostas_enviadas' in rules:
                # Contar mudanças de status para etapas de proposta
                proposal_activities = broker_activities[
                    (broker_activities['tipo'] == 'mudança_status') & 
                    (broker_activities['valor_novo'].str.contains('Proposta|Contrato|Negociação', na=False, case=False))
                ]
                proposal_count = len(proposal_activities.groupby('lead_id'))  # Contar leads únicos com proposta
                points_df.at[idx, 'propostas_enviadas'] = proposal_count
                points_df.at[idx, 'pontos'] += proposal_count * rules['propostas_enviadas']

            # 4. Vendas realizadas (20 pontos)
            if 'vendas_realizadas' in rules:
                # Contar leads com status "Ganho" (status_id = 142)
                sales_count = len(broker_leads[broker_leads['status_id'] == 142])
                points_df.at[idx, 'vendas_realizadas'] = sales_count
                points_df.at[idx, 'pontos'] += sales_count * rules['vendas_realizadas']

            # 5. Leads atualizados no mesmo dia (2 pontos)
            if 'leads_atualizados_mesmo_dia' in rules:
                same_day_updates = 0
                for _, lead in broker_leads.iterrows():
                    if lead['criado_em'] and lead['atualizado_em']:
                        # Verificar se houve atividade do broker no mesmo dia da criação
                        lead_activities_same_day = broker_activities[
                            (broker_activities['lead_id'] == lead['id']) &
                            (broker_activities['criado_em'].dt.date == lead['criado_em'].date())
                        ]
                        if not lead_activities_same_day.empty:
                            same_day_updates += 1
                            
                points_df.at[idx, 'leads_atualizados_mesmo_dia'] = same_day_updates
                points_df.at[idx, 'pontos'] += same_day_updates * rules['leads_atualizados_mesmo_dia']

            # 6. Resposta rápida em menos de 3 horas (4 pontos)
            if 'resposta_rapida_3h' in rules:
                quick_responses = 0
                for _, lead in broker_leads.iterrows():
                    # Buscar mensagens recebidas e enviadas para este lead
                    lead_messages = broker_activities[
                        (broker_activities['lead_id'] == lead['id']) & 
                        (broker_activities['tipo'].isin(['mensagem_recebida', 'mensagem_enviada']))
                    ].sort_values('criado_em')
                    
                    # Analisar sequências de mensagem recebida seguida de enviada
                    for i in range(len(lead_messages) - 1):
                        current_msg = lead_messages.iloc[i]
                        next_msg = lead_messages.iloc[i + 1]
                        
                        if (current_msg['tipo'] == 'mensagem_recebida' and 
                            next_msg['tipo'] == 'mensagem_enviada'):
                            response_time_hours = (next_msg['criado_em'] - current_msg['criado_em']).total_seconds() / 3600
                            if response_time_hours < 3:
                                quick_responses += 1
                                
                points_df.at[idx, 'resposta_rapida_3h'] = quick_responses
                points_df.at[idx, 'pontos'] += quick_responses * rules['resposta_rapida_3h']

            # 7. Todos os leads do dia respondidos (5 pontos)
            if 'todos_leads_respondidos' in rules:
                today_leads = broker_leads[broker_leads['criado_em'].dt.date == now.date()]
                all_responded = 0
                
                if not today_leads.empty:
                    responded_count = 0
                    for _, lead in today_leads.iterrows():
                        responses = broker_activities[
                            (broker_activities['lead_id'] == lead['id']) & 
                            (broker_activities['tipo'] == 'mensagem_enviada')
                        ]
                        if not responses.empty:
                            responded_count += 1
                    
                    # Se todos os leads do dia foram respondidos
                    if responded_count == len(today_leads):
                        all_responded = 1
                        
                points_df.at[idx, 'todos_leads_respondidos'] = all_responded
                points_df.at[idx, 'pontos'] += all_responded * rules['todos_leads_respondidos']

            # 8. Cadastro completo (3 pontos)
            if 'cadastro_completo' in rules:
                complete_leads = 0
                for _, lead in broker_leads.iterrows():
                    # Verificar se lead tem informações básicas preenchidas
                    if (lead['nome'] and 
                        lead['contato_nome'] and 
                        pd.notna(lead['valor']) and 
                        lead['valor'] > 0):
                        complete_leads += 1
                        
                points_df.at[idx, 'cadastro_completo'] = complete_leads
                points_df.at[idx, 'pontos'] += complete_leads * rules['cadastro_completo']

            # 9. Acompanhamento pós-venda (10 pontos)
            if 'acompanhamento_pos_venda' in rules:
                post_sale_followups = 0
                closed_deals = broker_leads[broker_leads['status_id'] == 142]  # Leads ganhos
                
                for _, deal in closed_deals.iterrows():
                    # Buscar atividades após o fechamento
                    followup_activities = broker_activities[
                        (broker_activities['lead_id'] == deal['id']) & 
                        (broker_activities['criado_em'] > deal['atualizado_em']) &
                        (broker_activities['tipo'].isin(['mensagem_enviada', 'tarefa_concluida']))
                    ]
                    if not followup_activities.empty:
                        post_sale_followups += 1
                        
                points_df.at[idx, 'acompanhamento_pos_venda'] = post_sale_followups
                points_df.at[idx, 'pontos'] += post_sale_followups * rules['acompanhamento_pos_venda']

            # 10. Leads sem interação até 24 horas (-5 pontos)
            if 'leads_sem_interacao_24h' in rules:
                no_interaction_count = 0
                for _, lead in broker_leads.iterrows():
                    # Pular leads já fechados
                    if lead['status_id'] in [142, 143]:  # Ganho ou Perdido
                        continue
                        
                    # Buscar última atividade do broker neste lead
                    last_activity = broker_activities[
                        broker_activities['lead_id'] == lead['id']
                    ]['criado_em'].max()
                    
                    reference_time = last_activity if pd.notna(last_activity) else lead['criado_em']
                    hours_since_activity = (now - reference_time).total_seconds() / 3600
                    
                    if hours_since_activity > 24:
                        no_interaction_count += 1
                        
                points_df.at[idx, 'leads_sem_interacao_24h'] = no_interaction_count
                points_df.at[idx, 'pontos'] -= no_interaction_count * abs(rules['leads_sem_interacao_24h'])

            # 11. Leads ignorados até 48 horas (0 pontos - neutro)
            if 'leads_ignorados_48h' in rules:
                ignored_leads = 0
                for _, lead in broker_leads.iterrows():
                    # Pular leads já fechados
                    if lead['status_id'] in [142, 143]:
                        continue
                        
                    # Verificar se houve alguma atividade do broker
                    broker_lead_activities = broker_activities[
                        broker_activities['lead_id'] == lead['id']
                    ]
                    
                    if broker_lead_activities.empty:
                        # Sem atividade - verificar tempo desde criação
                        hours_since_creation = (now - lead['criado_em']).total_seconds() / 3600
                        if hours_since_creation > 48:
                            ignored_leads += 1
                    else:
                        # Com atividade - verificar última atividade
                        last_activity = broker_lead_activities['criado_em'].max()
                        hours_since_activity = (now - last_activity).total_seconds() / 3600
                        if hours_since_activity > 48:
                            ignored_leads += 1
                            
                points_df.at[idx, 'leads_ignorados_48h'] = ignored_leads
                # Regra neutra (0 pontos)

            # 12. Leads perdidos (-10 pontos)
            if 'leads_perdidos' in rules:
                # Contar leads com status "Perdido" (status_id = 143)
                lost_leads = len(broker_leads[broker_leads['status_id'] == 143])
                points_df.at[idx, 'leads_perdidos'] = lost_leads
                points_df.at[idx, 'pontos'] -= lost_leads * abs(rules['leads_perdidos'])

        logger.info("Broker points calculation completed")
        return points_df

    except Exception as e:
        logger.error(f"Error calculating broker points: {str(e)}")
        raise
