import datetime
from flask import Blueprint, request, jsonify
import os
import json
import logging
from dotenv import load_dotenv
import pandas as pd

from libs.kommo_api import KommoAPI
from libs.supabase_db import SupabaseClient
from libs.sync_manager import SyncManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Separate logger for sync operations
sync_logger = logging.getLogger('sync')
sync_handler = logging.FileHandler('sync.log')
sync_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sync_handler.setFormatter(sync_formatter)
sync_logger.addHandler(sync_handler)
sync_logger.setLevel(logging.DEBUG)

load_dotenv()

webhook_bp = Blueprint('webhook', __name__, url_prefix='/webhook')

kommo_api = KommoAPI(
    api_url=os.getenv("KOMMO_API_URL"),
    access_token=os.getenv("ACCESS_TOKEN_KOMMO")
)

supabase = SupabaseClient(
    url=os.getenv("VITE_SUPABASE_URL"),
    key=os.getenv("VITE_SUPABASE_ANON_KEY")
)

sync_manager = SyncManager(kommo_api, supabase)

def update_broker_points():
    """Atualiza os pontos dos corretores com dados mais recentes"""
    try:
        # Buscar dados atualizados
        brokers = kommo_api.get_users()
        leads = kommo_api.get_leads()
        activities = kommo_api.get_activities()

        # Para cada corretor, calcular métricas (apenas corretores, não administradores)
        points_data = []
        for _, broker in brokers[brokers['cargo'] == 'Corretor'].iterrows():
            broker_id = broker['id']
            broker_leads = leads[leads['responsavel_id'] == broker_id]
            broker_activities = activities[activities['user_id'] == broker_id]

            # Calcular leads respondidos em 1h (+2 pts)
            leads_1h = sum(1 for _, lead in broker_leads.iterrows() if any(
                (a['criado_em'] - lead['criado_em']).total_seconds() <= 3600
                for _, a in broker_activities[
                    (broker_activities['lead_id'] == lead['id']) & 
                    (broker_activities['tipo'] == 'mensagem_enviada')
                ].iterrows()))

            # Calcular leads visitados (+5 pts)
            leads_visitados = len(
                broker_activities[broker_activities['valor_novo'].str.contains(
                    'Visitado|Visita', na=False, case=False)])

            # Calcular propostas enviadas (+8 pts)
            propostas = len(
                broker_activities[broker_activities['valor_novo'].str.contains(
                    'Proposta|Contrato', na=False, case=False)])

            # Calcular vendas realizadas (+15 pts)
            vendas = len(broker_leads[broker_leads['status'] == 'Ganho'])

            # Leads atualizados no mesmo dia (+2 pts)
            atualizados_dia = sum(
                1 for _, lead in broker_leads.iterrows()
                if lead['criado_em'].date() == lead['atualizado_em'].date())

            # Feedback positivo do gestor (+3 pts)
            feedbacks_positivos = len(
                broker_activities[
                    (broker_activities['tipo'] == 'note_created') & 
                    (broker_activities['valor_novo'].str.contains(
                        'positivo|bom|excelente|parabéns', 
                        na=False, case=False
                    ))
                ])

            # Resposta rápida em 3h (+4 pts)
            respostas_3h = 0
            for _, lead in broker_leads.iterrows():
                lead_msgs = broker_activities[
                    (broker_activities['lead_id'] == lead['id']) &
                    (broker_activities['tipo'].isin(['mensagem_recebida', 'mensagem_enviada']))
                ].sort_values('criado_em')

                for i in range(1, len(lead_msgs)):
                    if lead_msgs.iloc[i-1]['tipo'] == 'mensagem_recebida' and \
                       lead_msgs.iloc[i]['tipo'] == 'mensagem_enviada' and \
                       (lead_msgs.iloc[i]['criado_em'] - lead_msgs.iloc[i-1]['criado_em']).total_seconds() <= 10800:
                        respostas_3h += 1

            # Todos os leads do dia respondidos (+5 pts)
            now = datetime.datetime.now()
            today_leads = broker_leads[broker_leads['criado_em'].dt.date == now.date()]
            todos_respondidos = 1 if not today_leads.empty and all(
                any(a['tipo'] == 'mensagem_enviada' for _, a in 
                    broker_activities[broker_activities['lead_id'] == lead['id']].iterrows())
                for _, lead in today_leads.iterrows()
            ) else 0

            # Cadastro completo do lead (+3 pts)
            cadastros_completos = sum(1 for _, lead in broker_leads.iterrows()
                if pd.notna(lead['nome']) and 
                pd.notna(lead['contato_nome']) and 
                pd.notna(lead['valor']))

            # Acompanhamento pós-venda (+10 pts)
            acompanhamento_pos_venda = sum(1 for _, lead in broker_leads[broker_leads['status'] == 'Ganho'].iterrows()
                if any(a['criado_em'] > lead['atualizado_em'] 
                    for _, a in broker_activities[broker_activities['lead_id'] == lead['id']].iterrows()))

            # Leads sem interação 24h (-3 pts)
            sem_interacao = sum(
                1 for _, lead in broker_leads.iterrows()
                if (now - lead['atualizado_em']).total_seconds() > 86400)

            # Leads ignorados 48h (-5 pts)
            ignorados = sum(
                1 for _, lead in broker_leads.iterrows()
                if (now - lead['atualizado_em']).total_seconds() > 172800)

            # Leads com reclamação (-4 pts)
            reclamacoes = len(
                broker_activities[
                    broker_activities['valor_novo'].str.contains(
                        'reclamação|insatisfeito|problema|queixa',
                        na=False, case=False
                    )
                ])

            # Leads perdidos para concorrente (-6 pts)
            perdidos = len(broker_leads[
                (broker_leads['status'] == 'Perdido') & 
                (broker_leads['etapa'].str.contains('concorrente', na=False, case=False))
            ])

            # Métricas de alerta (sem pontos)
            respondidos_18h = sum(1 for _, lead in broker_leads.iterrows() if any(
                (a['criado_em'] - lead['criado_em']).total_seconds() > 64800
                for _, a in broker_activities[
                    (broker_activities['lead_id'] == lead['id']) & 
                    (broker_activities['tipo'] == 'mensagem_enviada')
                ].iterrows()))

            tempo_resposta_12h = sum(1 for _, lead in broker_leads.iterrows()
                if any((a2['criado_em'] - a1['criado_em']).total_seconds() > 43200
                    for (_, a1), (_, a2) in zip(
                        broker_activities[
                            (broker_activities['lead_id'] == lead['id']) & 
                            (broker_activities['tipo'] == 'mensagem_recebida')
                        ].iterrows(),
                        broker_activities[
                            (broker_activities['lead_id'] == lead['id']) & 
                            (broker_activities['tipo'] == 'mensagem_enviada')
                        ].iterrows()
                    )))

            sem_mudanca_5dias = sum(1 for _, lead in broker_leads.iterrows()
                if (now - lead['atualizado_em']).days > 5)

            # Calcular pontuação total
            pontos = (
                leads_1h * 2 +                    # Lead respondido em 1h
                leads_visitados * 5 +             # Lead visitado
                propostas * 8 +                   # Proposta enviada
                vendas * 15 +                     # Venda realizada
                atualizados_dia * 2 +            # Lead atualizado no mesmo dia
                feedbacks_positivos * 3 +         # Feedback positivo
                respostas_3h * 4 +               # Resposta em 3h
                todos_respondidos * 5 +           # Todos leads do dia respondidos
                cadastros_completos * 3 +         # Cadastro completo
                acompanhamento_pos_venda * 10 -   # Acompanhamento pós-venda
                sem_interacao * 3 -              # Lead sem interação 24h
                ignorados * 5 -                  # Lead ignorado 48h
                reclamacoes * 4 -                # Lead com reclamação
                perdidos * 6                     # Lead perdido para concorrente
            )

            points_data.append({
                'id': broker_id,
                'nome': broker['nome'],
                'pontos': max(0, pontos),
                'leads_respondidos_1h': leads_1h,
                'leads_visitados': leads_visitados,
                'propostas_enviadas': propostas,
                'vendas_realizadas': vendas,
                'leads_atualizados_mesmo_dia': atualizados_dia,
                'feedbacks_positivos': feedbacks_positivos,
                'resposta_rapida_3h': respostas_3h,
                'todos_leads_respondidos': todos_respondidos,
                'cadastro_completo': cadastros_completos,
                'acompanhamento_pos_venda': acompanhamento_pos_venda,
                'leads_sem_interacao_24h': sem_interacao,
                'leads_ignorados_48h': ignorados,
                'leads_com_reclamacao': reclamacoes,
                'leads_perdidos': perdidos,
                'leads_respondidos_apos_18h': respondidos_18h,
                'leads_tempo_resposta_acima_12h': tempo_resposta_12h,
                'leads_5_dias_sem_mudanca': sem_mudanca_5dias,
                'updated_at': now
            })

        # Atualizar pontos no Supabase
        points_df = pd.DataFrame(points_data)
        supabase.upsert_broker_points(points_df)

        return True
    except Exception as e:
        webhook_logger.error(f"Erro ao atualizar pontos: {str(e)}")
        return False


@webhook_bp.route('/kommo', methods=['POST', 'PATCH'])
def handle_webhook():
    try:
        if not request.data:
            webhook_logger.error("Requisição sem corpo recebida.")
            return jsonify({"status": "error", "message": "Requisição sem dados"}), 400

        try:
            webhook_data = request.get_json(force=True)
        except Exception as e:
            webhook_logger.error(f"Erro ao carregar JSON: {str(e)}")
            return jsonify({"status": "error", "message": "JSON inválido"}), 400

        webhook_logger.info("Webhook recebido: %s", json.dumps(webhook_data, indent=2))

        # Validações mínimas
        event_type = webhook_data.get('type')
        if not event_type:
            return jsonify({"status": "ignored", "message": "Evento sem tipo"}), 200

        # Verifica se é um tipo de evento que merece ação
        eventos_relevantes = {
            'lead_added',
            'lead_status_changed',
            'note_created',
            'task_completed',
            'incoming_chat_message',
            'outgoing_chat_message'
        }

        leads_status = webhook_data.get('leads', {}).get('status', [])
        should_update = event_type in eventos_relevantes

        # Se status do lead for "ganho" ou "perdido", força atualização
        status_ids_relevantes = [142, 143]  # 142 = ganho, 143 = perdido
        is_sale = any(
            status.get('status_id') == 142 and status.get('responsible_user_id')
            for status in leads_status
        )
        should_update = should_update or any(
            status.get('status_id') in status_ids_relevantes
            for status in leads_status
        )

        if should_update or is_sale:
            webhook_logger.debug(f"Sincronizando dados: evento={event_type}, is_sale={is_sale}")
            if sync_manager.force_sync():
                if update_broker_points():
                    return jsonify({
                        "status": "success",
                        "message": "Dados sincronizados e pontos atualizados"
                    }), 200
                else:
                    return jsonify({
                        "status": "error",
                        "message": "Erro ao atualizar pontos"
                    }), 500
            else:
                return jsonify({
                    "status": "error",
                    "message": "Erro ao sincronizar dados"
                }), 500

        return jsonify({
            "status": "success",
            "message": "Evento irrelevante processado com sucesso"
        }), 200

    except Exception as e:
        webhook_logger.exception("Erro interno no processamento do webhook")
        return jsonify({"status": "error", "message": str(e)}), 500

@webhook_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint that verifies API and database connectivity"""
    try:
        # Check API connection
        kommo_api._make_request("users", params={"limit": 1})

        # Check database connection
        supabase.client.table("brokers").select("id").limit(1).execute()

        status = {
            "status": "healthy",
            "api": "connected",
            "database": "connected",
            "last_sync": {
                resource: sync_manager.last_sync[resource].isoformat() 
                if sync_manager.last_sync[resource] else None
                for resource in sync_manager.last_sync
            }
        }
        return jsonify(status), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500