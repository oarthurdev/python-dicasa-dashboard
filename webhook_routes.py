import datetime
from flask import Blueprint, request, jsonify
import os
import logging
from dotenv import load_dotenv
import pandas as pd

from kommo_api import KommoAPI
from supabase_db import SupabaseClient

load_dotenv()

logger = logging.getLogger(__name__)
webhook_bp = Blueprint('webhook', __name__, url_prefix='/webhook')

kommo_api = KommoAPI(
    api_url=os.getenv("KOMMO_API_URL"),
    access_token=os.getenv("ACCESS_TOKEN_KOMMO")
)

supabase = SupabaseClient(
    url=os.getenv("VITE_SUPABASE_URL"),
    key=os.getenv("VITE_SUPABASE_ANON_KEY")
)

def update_broker_points():
    """Atualiza os pontos dos corretores com dados mais recentes"""
    try:
        # Buscar dados atualizados
        brokers = kommo_api.get_users()
        leads = kommo_api.get_leads()
        activities = kommo_api.get_activities()

        # Para cada corretor, calcular métricas
        points_data = []
        for _, broker in brokers.iterrows():
            broker_id = broker['id']
            broker_leads = leads[leads['responsavel_id'] == broker_id]
            broker_activities = activities[activities['user_id'] == broker_id]

            # Calcular leads respondidos em 1h
            leads_1h = sum(1 for _, lead in broker_leads.iterrows() if any(
                (a['criado_em'] - lead['criado_em']).total_seconds() <= 3600
                for _, a in broker_activities[broker_activities['lead_id'] ==
                                              lead['id']].iterrows()))

            # Calcular leads visitados
            leads_visitados = len(
                broker_activities[broker_activities['valor_novo'].str.contains(
                    'Visitado|Visita', na=False, case=False)])

            # Calcular propostas enviadas
            propostas = len(
                broker_activities[broker_activities['valor_novo'].str.contains(
                    'Proposta|Contrato', na=False, case=False)])

            # Calcular vendas realizadas
            vendas = len(broker_leads[broker_leads['status'] == 'Ganho'])

            # Leads atualizados no mesmo dia
            atualizados_dia = sum(
                1 for _, lead in broker_leads.iterrows()
                if lead['criado_em'].date() == lead['atualizado_em'].date())

            # Calcular resposta rápida (3h)
            respostas_3h = 0
            for _, lead in broker_leads.iterrows():
                lead_msgs = broker_activities[
                    (broker_activities['lead_id'] == lead['id'])
                    & (broker_activities['tipo'].isin(
                        ['mensagem_recebida', 'mensagem_enviada'])
                       )].sort_values('criado_em')

                for i in range(1, len(lead_msgs)):
                    if (lead_msgs.iloc[i]['criado_em'] -
                            lead_msgs.iloc[i - 1]['criado_em']
                        ).total_seconds() <= 10800:
                        respostas_3h += 1

            # Calcular leads sem interação 24h
            sem_interacao = sum(
                1 for _, lead in broker_leads.iterrows()
                if (datetime.now() -
                    lead['atualizado_em']).total_seconds() > 86400)

            # Calcular leads ignorados 48h
            ignorados = sum(
                1 for _, lead in broker_leads.iterrows()
                if (datetime.now() -
                    lead['atualizado_em']).total_seconds() > 172800)

            # Calcular pontuação total
            pontos = (leads_1h * 2 + leads_visitados * 5 + propostas * 8 +
                      vendas * 15 + atualizados_dia * 2 + respostas_3h * 4 -
                      sem_interacao * 3 - ignorados * 5)

            points_data.append({
                'id': broker_id,
                'nome': broker['nome'],
                'pontos': max(0, pontos),
                'leads_respondidos_1h': leads_1h,
                'leads_visitados': leads_visitados,
                'propostas_enviadas': propostas,
                'vendas_realizadas': vendas,
                'leads_atualizados_mesmo_dia': atualizados_dia,
                'resposta_rapida_3h': respostas_3h,
                'leads_sem_interacao_24h': sem_interacao,
                'leads_ignorados_48h': ignorados,
                'updated_at': datetime.now()
            })

        # Atualizar pontos no Supabase
        points_df = pd.DataFrame(points_data)
        supabase.upsert_broker_points(points_df)

        return True
    except Exception as e:
        logger.error(f"Erro ao atualizar pontos: {str(e)}")
        return False


@webhook_bp.route('/kommo', methods=['POST'])
def handle_webhook():
    try:
        webhook_data = request.json
        logger.info(f"Webhook recebido: {webhook_data}")

        # Identificar tipo de evento
        event_type = webhook_data.get('type', '')
        entity_type = webhook_data.get('entity', {}).get('type', '')

        # Eventos que devem disparar atualização de pontos
        update_events = [
            'lead_status_changed',  # Mudança de status do lead
            'task_completed',  # Tarefa concluída
            'note_created',  # Nota criada (feedback)
            'incoming_chat_message',  # Mensagem recebida
            'outgoing_chat_message'  # Mensagem enviada
        ]

        # Atualizar pontos se o evento for relevante
        if event_type in update_events or entity_type in ['lead', 'user']:
            success = update_broker_points()
            if success:
                return jsonify({
                    "status": "success",
                    "message": "Pontos atualizados"
                }), 200
            else:
                return jsonify({
                    "status": "error",
                    "message": "Erro ao atualizar pontos"
                }), 500

        return jsonify({
            "status": "success",
            "message": "Evento processado"
        }), 200

    except Exception as e:
        logger.error(f"Erro ao processar webhook: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
