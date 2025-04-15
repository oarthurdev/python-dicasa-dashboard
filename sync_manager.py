
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

class SyncManager:
    def __init__(self, kommo_api, supabase_client):
        self.kommo_api = kommo_api
        self.supabase = supabase_client
        self.last_sync = {
            'users': None,
            'leads': None,
            'activities': None
        }
        self.sync_interval = 300  # seconds (5 minutes)

    def needs_sync(self, resource: str) -> bool:
        last = self.last_sync.get(resource)
        if not last:
            return True
        return (datetime.now() - last) > timedelta(seconds=self.sync_interval)

    def update_sync_time(self, resource: str):
        self.last_sync[resource] = datetime.now()

    def sync_data(self) -> bool:
        try:
            # Sync users/brokers
            if self.needs_sync('users'):
                brokers = self.kommo_api.get_users()
                if not brokers.empty:
                    self.supabase.upsert_brokers(brokers)
                self.update_sync_time('users')

            # Sync leads
            if self.needs_sync('leads'):
                leads = self.kommo_api.get_leads()
                if not leads.empty:
                    self.supabase.upsert_leads(leads)
                self.update_sync_time('leads')

            # Sync activities
            if self.needs_sync('activities'):
                activities = self.kommo_api.get_activities()
                if not activities.empty:
                    self.supabase.upsert_activities(activities)
                self.update_sync_time('activities')

            # Calculate and update points for each broker
            if not brokers.empty and not leads.empty and not activities.empty:
                points_data = []
                now = datetime.now()

                for _, broker in brokers[brokers['cargo'] == 'Corretor'].iterrows():
                    broker_id = broker['id']
                    broker_leads = leads[leads['responsavel_id'] == broker_id]
                    broker_activities = activities[activities['user_id'] == broker_id]

                    # Calculate metrics
                    leads_1h = sum(1 for _, lead in broker_leads.iterrows() if any(
                        (a['criado_em'] - lead['criado_em']).total_seconds() <= 3600
                        for _, a in broker_activities[broker_activities['lead_id'] == lead['id']].iterrows()))

                    leads_visitados = len(broker_activities[broker_activities['valor_novo'].str.contains(
                        'Visitado|Visita', na=False, case=False)])

                    propostas = len(broker_activities[broker_activities['valor_novo'].str.contains(
                        'Proposta|Contrato', na=False, case=False)])

                    vendas = len(broker_leads[broker_leads['status'] == 'Ganho'])

                    atualizados_dia = sum(1 for _, lead in broker_leads.iterrows()
                        if lead['criado_em'].date() == lead['atualizado_em'].date())

                    respostas_3h = sum(1 for _, lead in broker_leads.iterrows()
                        if any((a2['criado_em'] - a1['criado_em']).total_seconds() <= 10800
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

                    sem_interacao = sum(1 for _, lead in broker_leads.iterrows()
                        if (now - lead['atualizado_em']).total_seconds() > 86400)

                    ignorados = sum(1 for _, lead in broker_leads.iterrows()
                        if (now - lead['atualizado_em']).total_seconds() > 172800)

                    # Calculate total points
                    pontos = (
                        leads_1h * 2 +               # Lead respondido em 1h
                        leads_visitados * 5 +        # Lead visitado
                        propostas * 8 +              # Proposta enviada
                        vendas * 15 +                # Venda realizada
                        atualizados_dia * 2 +        # Lead atualizado no mesmo dia
                        respostas_3h * 4 -           # Resposta em 3h
                        sem_interacao * 3 -          # Lead sem interação 24h
                        ignorados * 5                # Lead ignorado 48h
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
                        'resposta_rapida_3h': respostas_3h,
                        'leads_sem_interacao_24h': sem_interacao,
                        'leads_ignorados_48h': ignorados,
                        'updated_at': now
                    })

                # Update points in Supabase
                if points_data:
                    points_df = pd.DataFrame(points_data)
                    self.supabase.upsert_broker_points(points_df)

            return True

        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            return False

    def force_sync(self) -> bool:
        """Force immediate sync of all data"""
        self.last_sync = {k: None for k in self.last_sync.keys()}
        return self.sync_data()
