import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
import pytz
from dateutil import parser

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

sao_paulo_tz = pytz.timezone('America/Sao_Paulo')


def parse_datetime_sp(value):
    if not value:
        return None

    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=sao_paulo_tz)
    elif isinstance(value, str):
        dt = parser.parse(value)
        if dt.tzinfo is None:
            dt = sao_paulo_tz.localize(dt)
        else:
            dt = dt.astimezone(sao_paulo_tz)
    elif isinstance(value, datetime):
        dt = value if value.tzinfo else sao_paulo_tz.localize(value)
        dt = dt.astimezone(sao_paulo_tz)
    else:
        return None

    return dt


class KommoAPI:

    

    def set_date_range(self, start_date, end_date):
        """Set date range for API queries"""
        self.start_date = start_date
        self.end_date = end_date
        if isinstance(start_date, datetime):
            self.api_config['sync_start_date'] = int(start_date.timestamp())
        if isinstance(end_date, datetime):
            self.api_config['sync_end_date'] = int(end_date.timestamp())

    def __init__(self, api_url=None, access_token=None, api_config=None, supabase_client=None):
        try:
            logger.info("Initializing KommoAPI")

            from .rate_limit_monitor import RateLimitMonitor
            self.rate_monitor = RateLimitMonitor()

            self.api_config = api_config
            self.supabase_client = supabase_client
            self.api_url = api_url or (api_config.get('api_url') if api_config
                                       else None) or os.getenv("KOMMO_API_URL")
            self.access_token = access_token or (
                api_config.get('access_token')
                if api_config else None) or os.getenv("ACCESS_TOKEN_KOMMO")
            self.start_date = None
            self.end_date = None

            if not self.api_url or not self.access_token:
                raise ValueError("API URL and access token must be provided")

            if self.api_url.endswith('/'):
                self.api_url = self.api_url[:-1]

            logger.info("KommoAPI initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing KommoAPI: {str(e)}")
            raise

    def _make_request(self,
                      endpoint,
                      method="GET",
                      params=None,
                      data=None,
                      retry_count=3):
        """
        Make a request to the Kommo API with retry logic and rate limiting
        """
        url = f"{self.api_url}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        for attempt in range(retry_count):
            try:
                # Aplica rate limiting de 7 req/s conforme documentação Kommo
                self.rate_monitor.enforce_rate_limit()

                logger.info(f"Making API request to: {url}")
                response = requests.request(method=method,
                                            url=url,
                                            headers=headers,
                                            params=params,
                                            json=data)

                # Log response status and content for debugging
                logger.info(f"Response status: {response.status_code}")
                logger.debug(f"Response content: {response.text[:500]}")

                response.raise_for_status()

                # Check if response content is empty
                if not response.text.strip():
                    logger.warning("Empty response received from API")
                    return {}

                return response.json()

            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if hasattr(
                    e, 'response') else 0

                # Usa o novo handler de erros específicos da Kommo
                if status_code in (429, 403, 504):
                    if not self.rate_monitor.handle_kommo_error(
                            status_code, endpoint, attempt):
                        logger.error(
                            f"Stopping retries for {endpoint} due to {status_code}"
                        )
                        raise
                    self.rate_monitor.wait_before_retry(endpoint, attempt)
                else:
                    logger.warning(
                        f"API request failed (attempt {attempt+1}/{retry_count}): {str(e)}"
                    )
                    if attempt >= retry_count - 1:
                        raise
                    time.sleep(2)  # Default delay for non-rate-limit errors

    def get_users(self, active_only=True):
        """
        Retrieve all users (brokers) from Kommo CRM

        Args:
            active_only (bool): If True, only return active users
        """
        try:
            logger.info("Retrieving ALL users from Kommo CRM")

            # Get safe pagination limits
            limits = self._get_safe_pagination_limits()

            users_data = []
            page = 1
            max_user_pages = 20  # Users são geralmente poucos, limite menor

            while page <= max_user_pages:
                try:
                    response = self._make_request("users",
                                                  params={
                                                      "page": page,
                                                      "limit":
                                                      limits['page_size']
                                                  })

                    if not response.get("_embedded", {}).get("users", []):
                        logger.info(
                            f"Nenhum usuário encontrado na página {page}")
                        break

                    users = response["_embedded"]["users"]
                    users_data.extend(users)
                    logger.info(
                        f"Página {page}: {len(users)} usuários encontrados (total: {len(users_data)})"
                    )

                    # Users API typically has fewer pages, break if less than limit
                    if len(users) < limits['page_size']:
                        logger.info("Última página de usuários atingida")
                        break

                    page += 1
                    time.sleep(limits['delay_between_pages'])

                except Exception as e:
                    logger.error(
                        f"Erro ao buscar usuários página {page}: {str(e)}")
                    break

            logger.info(f"Total de usuários encontrados: {len(users_data)}")

            # Process users data into a more usable format
            processed_users = []
            for user in users_data:
                # Skip inactive users if active_only is True
                is_active = user.get("rights", {}).get("is_active", False)
                if active_only and not is_active:
                    continue

                processed_users.append({
                    "id":
                    user.get("id"),
                    "nome":
                    f"{user.get('name', '')} {user.get('lastname', '')}".strip(
                    ),
                    "email":
                    user.get("email"),
                    "foto_url":
                    user.get("_links", {}).get("avatar", {}).get("href"),
                    "criado_em": (parse_datetime_sp(user.get("created_at"))
                                  if user.get("created_at") else None),
                    "cargo":
                    user.get("rights", {}).get("is_admin") and "Administrador"
                    or "Corretor"
                })

            logger.info(
                f"Usuários processados (ativos): {len(processed_users)}")
            return pd.DataFrame(processed_users)

        except Exception as e:
            logger.error(f"Failed to retrieve users: {str(e)}")
            raise

    

    def _get_safe_pagination_limits(self):
        """Define limites seguros para paginação respeitando 7 req/s da API Kommo"""
        return {
            'max_pages_per_request': 150,  # Aumentado para sincronização completa
            'max_total_records': 50000,   # Aumentado para capturar todos os dados
            'page_size': 50,              # Mantido para estabilidade
            'delay_between_pages': 0.0    # Removido - rate limiting é feito no _make_request
        }

    def get_leads(self, company_id=None):
        """
        Retrieve leads from specific pipelines in Kommo CRM
        Args:
            company_id (str): Optional company ID to filter leads
        """
        try:
            # Get company_id from config
            company_id = company_id or self.api_config.get('company_id')

            # Get target pipeline IDs from database for this specific company
            target_pipeline_ids = []
            if hasattr(self, 'supabase_client') and self.supabase_client:
                try:
                    # Get pipeline_id from kommo_config table for this company
                    result = self.supabase_client.client.table("kommo_config").select("pipeline_id").eq(
                        "company_id", company_id).eq("active", True).execute()
                    
                    if result.data and result.data[0].get('pipeline_id'):
                        pipeline_data = result.data[0]['pipeline_id']
                        if isinstance(pipeline_data, list):
                            target_pipeline_ids = pipeline_data
                        elif isinstance(pipeline_data, str):
                            # Try to parse as JSON array
                            import json
                            try:
                                target_pipeline_ids = json.loads(pipeline_data)
                            except:
                                logger.error(f"Failed to parse pipeline_id JSON for company {company_id}")
                                return pd.DataFrame()
                        
                        logger.info(f"Company {company_id} - Pipeline IDs from config: {target_pipeline_ids}")
                    else:
                        logger.error(f"No pipeline_id found in kommo_config for company {company_id}")
                        return pd.DataFrame()
                        
                except Exception as e:
                    logger.error(f"Error getting pipeline IDs from database for company {company_id}: {e}")
                    return pd.DataFrame()
            else:
                logger.error("Supabase client not available")
                return pd.DataFrame()
            
            if not target_pipeline_ids:
                logger.error(f"No pipeline IDs configured for company {company_id}")
                return pd.DataFrame()

            # Get safe pagination limits
            limits = self._get_safe_pagination_limits()

            logger.info(f"Buscando etapas dos pipelines específicos: {target_pipeline_ids}")
            pipeline_response = self._make_request("leads/pipelines")
            pipelines = pipeline_response.get("_embedded",
                                              {}).get("pipelines", [])

            status_map = {}
            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                # Only get statuses for target pipelines configured for this company
                if pipeline_id in target_pipeline_ids:
                    for status in pipeline.get("_embedded",
                                               {}).get("statuses", []):
                        status_id = status.get("id")
                        status_name = status.get("name")
                        pipeline_name = pipeline.get("name", "")
                        # Include pipeline name in status for better identification
                        status_map[status_id] = f"{status_name} ({pipeline_name})"

            logger.info(f"Etapas dos pipelines {target_pipeline_ids} carregadas com sucesso")
            logger.info(
                f"Retrieving leads from pipelines {target_pipeline_ids} in Kommo CRM")

            filtered_leads = []
            page = 1
            per_page = limits['page_size']
            empty_streak = 0
            max_empty_streak = 3

            while page <= limits['max_pages_per_request'] and len(
                    filtered_leads) < limits['max_total_records']:
                
                # Make separate requests for each target pipeline
                for pipeline_id in target_pipeline_ids:
                    params = {
                        "page": page,
                        "limit": per_page,
                        "with": "contacts,pipeline_id,loss_reason,catalog_elements,company",
                        "filter[pipeline_id]": pipeline_id
                    }

                    try:
                        logger.info(f"Fetching leads from pipeline {pipeline_id}, page {page}")
                        response = self._make_request("leads", params=params)
                        leads = response.get("_embedded", {}).get("leads", [])

                        if leads:
                            filtered_leads.extend(leads)
                            logger.info(
                                f"Pipeline {pipeline_id}, Página {page}: {len(leads)} leads encontrados"
                            )
                        else:
                            logger.info(
                                f"Pipeline {pipeline_id}, Página {page}: nenhum lead encontrado"
                            )

                        # Rate limiting delay between requests
                        time.sleep(limits['delay_between_pages'])

                    except Exception as e:
                        logger.error(f"Erro no pipeline {pipeline_id}, página {page}: {str(e)}")
                        continue

                # Check if we hit limits after processing all pipelines for this page
                if len(filtered_leads) >= limits['max_total_records']:
                    logger.warning(f"Limite de registros atingido: {limits['max_total_records']}")
                    break

                page += 1
                
                # Simple pagination break - if no leads found in any pipeline for this page
                page_had_leads = False
                for pipeline_id in target_pipeline_ids:
                    test_params = {
                        "page": page,
                        "limit": 1,
                        "filter[pipeline_id]": pipeline_id
                    }
                    try:
                        test_response = self._make_request("leads", params=test_params)
                        if test_response.get("_embedded", {}).get("leads", []):
                            page_had_leads = True
                            break
                    except:
                        continue
                
                if not page_had_leads:
                    logger.info("Nenhum lead encontrado em nenhum pipeline para esta página - finalizando")
                    break

            # Check if we hit limits
            if len(filtered_leads) >= limits['max_total_records']:
                logger.warning(
                    f"Limite de registros atingido: {limits['max_total_records']}"
                )
            if page >= limits['max_pages_per_request']:
                logger.warning(
                    f"Limite de páginas atingido: {limits['max_pages_per_request']}"
                )

            logger.info(f"Total de leads encontrados: {len(filtered_leads)}")

            processed_leads = []
            for lead in filtered_leads:
                responsavel_id = lead.get("responsible_user_id")
                contato_nome = ""
                if lead.get("_embedded", {}).get("contacts"):
                    contato_nome = lead["_embedded"]["contacts"][0].get(
                        "name", "")

                status_id = lead.get("status_id")
                etapa = status_map.get(status_id, "Desconhecido")

                processed_leads.append({
                    "id":
                    lead.get("id"),
                    "nome":
                    lead.get("name"),
                    "responsavel_id":
                    responsavel_id,
                    "contato_nome":
                    contato_nome,
                    "valor":
                    lead.get("price"),
                    "status_id":
                    status_id,
                    "pipeline_id":
                    lead.get("pipeline_id"),
                    "etapa":
                    etapa,
                    "criado_em": (parse_datetime_sp(lead.get("created_at"))
                                  if lead.get("created_at") else None),
                    "atualizado_em": (parse_datetime_sp(lead.get("updated_at"))
                                      if lead.get("updated_at") else None),
                    "fechado":
                    lead.get("closed_at") is not None,
                    "status":
                    ("Ganho" if status_id == 142 else
                     "Perdido" if status_id == 143 else "Em progresso")
                })

            return pd.DataFrame(processed_leads)

        except Exception as e:
            logger.error(f"Erro ao buscar leads: {str(e)}")
            return pd.DataFrame()

    def get_activities(self, company_id=None):
        """
        Retrieve all activities from Kommo CRM for specific company with safe pagination
        Args:
            company_id (str): Optional company ID to filter activities
        Returns:
            pd.DataFrame: Processed activities data
        """
        try:
            logger.info(
                "Retrieving ALL activities from Kommo CRM (no date filters)")

            # Get safe pagination limits
            limits = self._get_safe_pagination_limits()

            # Eventos disponíveis na API da Kommo - todos os eventos que retornam dados
            available_event_types = [
                "lead_status_changed", "incoming_chat_message",
                "outgoing_chat_message", "task_completed", "task_added",
                "common_note_added", "outgoing_call", "incoming_call",
                "lead_created", "lead_updated", "lead_deleted",
                "contact_created", "contact_updated", "contact_deleted",
                "company_created", "company_updated", "company_deleted",
                "task_result_added", "custom_field_value_changed",
                "sale_field_changed", "lead_linked", "lead_unlinked",
                "incoming_sms", "outgoing_sms", "entity_tag_added",
                "entity_tag_deleted", "entity_responsible_changed"
            ]

            # Fazer requisições sem filtros específicos para capturar todos os eventos
            base_params = {
                "limit": limits['page_size']
            }

            activities_data = []
            page = 1
            empty_streak = 0
            max_empty_streak = 3

            while page <= limits['max_pages_per_request'] and len(
                    activities_data) < limits['max_total_records']:
                try:
                    # Create params with page number for this iteration
                    current_params = base_params.copy()
                    current_params["page"] = page

                    # Fazer requisição sem filtros específicos para capturar todos os eventos
                    response = self._make_request("events", params=current_params)

                    # Validação robusta da resposta
                    events = []
                    if response is None:
                        logger.warning(f"Página {page}: resposta nula da API")
                    elif not isinstance(response, dict):
                        logger.warning(
                            f"Página {page}: resposta inválida (tipo: {type(response)})"
                        )
                    else:
                        embedded = response.get("_embedded")
                        if embedded is None:
                            logger.info(
                                f"Página {page}: nenhum dado embedded encontrado"
                            )
                        elif not isinstance(embedded, dict):
                            logger.warning(
                                f"Página {page}: embedded inválido (tipo: {type(embedded)})"
                            )
                        else:
                            events = embedded.get("events", [])
                            if not isinstance(events, list):
                                logger.warning(
                                    f"Página {page}: events não é uma lista (tipo: {type(events)})"
                                )
                                events = []

                    # Processar eventos encontrados - filtrando pelos tipos desejados após receber todos
                    if events and len(events) > 0:
                        # Filtrar apenas eventos válidos (dicionários) e pelos tipos que queremos
                        valid_events = []
                        for event in events:
                            if isinstance(event, dict):
                                event_type = event.get('type', '')
                                # Filtrar apenas os tipos de eventos que nos interessam
                                if event_type in available_event_types:
                                    valid_events.append(event)
                        
                        if len(valid_events) != len([e for e in events if isinstance(e, dict)]):
                            filtered_out = len([e for e in events if isinstance(e, dict)]) - len(valid_events)
                            logger.info(
                                f"Página {page}: {filtered_out} eventos filtrados por tipo não relevante"
                            )

                        if valid_events:
                            activities_data.extend(valid_events)
                            empty_streak = 0
                            logger.info(
                                f"Página {page}: {len(valid_events)} atividades válidas encontradas (total: {len(activities_data)})"
                            )
                        else:
                            empty_streak += 1
                            logger.info(
                                f"Página {page}: nenhuma atividade válida encontrada"
                            )
                    else:
                        empty_streak += 1
                        logger.info(
                            f"Página {page}: nenhuma atividade encontrada (streak: {empty_streak})"
                        )

                    # Verificar se deve parar por páginas vazias consecutivas
                    if empty_streak >= max_empty_streak:
                        logger.info(
                            f"Parando: {max_empty_streak} páginas vazias consecutivas"
                        )
                        break

                    # Verificar se chegou ao fim dos dados
                    valid_events_count = len(
                        [e for e in events
                         if isinstance(e, dict)]) if events else 0
                    if valid_events_count < limits['page_size']:
                        logger.info(
                            "Última página atingida (menos registros que o limite)"
                        )
                        break

                    page += 1
                    time.sleep(limits['delay_between_pages'])

                except Exception as e:
                    logger.error(f"Erro na página {page}: {str(e)}")
                    logger.exception("Detalhes completos do erro:")

                    # Verificar se o erro é relacionado a filtros inválidos
                    if "400" in str(e) or "Bad Request" in str(e):
                        logger.error(
                            "Erro 400 - possível filtro de evento inválido")
                        # Tentar com menos filtros na próxima iteração seria uma opção
                        break

                    # Continue to next page on other errors
                    page += 1
                    time.sleep(2)  # Extra delay on error

            # Check if we hit limits
            if len(activities_data) >= limits['max_total_records']:
                logger.warning(
                    f"Limite de registros atingido: {limits['max_total_records']}"
                )
            if page >= limits['max_pages_per_request']:
                logger.warning(
                    f"Limite de páginas atingido: {limits['max_pages_per_request']}"
                )

            logger.info(
                f"Total de atividades recuperadas: {len(activities_data)}")

            # Validação final dos dados antes do processamento
            if not activities_data:
                logger.warning(
                    "Nenhuma atividade encontrada - retornando DataFrame vazio"
                )
                return pd.DataFrame()

            if not isinstance(activities_data, list):
                logger.error(
                    f"activities_data não é uma lista: {type(activities_data)}"
                )
                return pd.DataFrame()

            # Filtrar e validar atividades antes do processamento
            valid_activities = []
            for i, activity in enumerate(activities_data):
                if not isinstance(activity, dict):
                    logger.warning(
                        f"Atividade {i} não é um dicionário válido: {type(activity)}"
                    )
                    continue
                if not activity.get('type'):
                    logger.warning(f"Atividade {i} não possui tipo definido")
                    continue
                valid_activities.append(activity)

            if not valid_activities:
                logger.warning(
                    "Nenhuma atividade válida encontrada após filtragem")
                return pd.DataFrame()

            logger.info(
                f"Atividades válidas para processamento: {len(valid_activities)}"
            )

            # Log event types for debugging - usando apenas atividades válidas
            event_types = set()
            for activity in valid_activities:
                activity_type = activity.get('type')
                if activity_type:
                    event_types.add(activity_type)

            logger.info(f"Event types found: {event_types}")

            # Usar valid_activities em vez de activities_data
            activities_data = valid_activities

            # Mapeamento completo dos eventos da Kommo API - todos os eventos sincronizados
            type_mapping = {
                "lead_status_changed": "mudança_status",
                "incoming_chat_message": "mensagem_recebida",
                "outgoing_chat_message": "mensagem_enviada",
                "task_completed": "tarefa_concluida",
                "task_added": "tarefa_criada",
                "common_note_added": "nota_adicionada",
                "outgoing_call": "chamada_realizada",
                "incoming_call": "chamada_recebida",
                "lead_created": "lead_criado",
                "lead_updated": "lead_atualizado",
                "lead_deleted": "lead_excluido",
                "contact_created": "contato_criado",
                "contact_updated": "contato_atualizado",
                "contact_deleted": "contato_excluido",
                "company_created": "empresa_criada",
                "company_updated": "empresa_atualizada",
                "company_deleted": "empresa_excluida",
                "task_result_added": "resultado_tarefa_adicionado",
                "custom_field_value_changed": "campo_personalizado_alterado",
                "sale_field_changed": "campo_venda_alterado",
                "lead_linked": "lead_vinculado",
                "lead_unlinked": "lead_desvinculado",
                "incoming_sms": "sms_recebido",
                "outgoing_sms": "sms_enviado",
                "entity_tag_added": "tag_adicionada",
                "entity_tag_deleted": "tag_removida",
                "entity_responsible_changed": "responsavel_alterado"
            }

            processed_activities = []
            for i, activity in enumerate(activities_data):
                # Verificar se activity é um dicionário válido
                if not isinstance(activity, dict):
                    logger.warning(
                        f"Atividade {i}: não é um dicionário válido (tipo: {type(activity)})"
                    )
                    continue

                # Verificar se possui campos essenciais
                if not activity.get("id"):
                    logger.warning(f"Atividade {i}: não possui ID válido")
                    continue

                # Extrair informações específicas baseado no tipo de evento
                activity_type = activity.get("type", "")
                if not activity_type:
                    logger.warning(
                        f"Atividade {i} (ID: {activity.get('id')}): tipo não definido"
                    )
                    activity_type = "unknown"

                entity_type = activity.get("entity_type", "")
                entity_id = activity.get("entity_id")

                # Log para eventos não mapeados
                if activity_type not in type_mapping:
                    logger.info(
                        f"Tipo de evento não mapeado encontrado: {activity_type}"
                    )

                # Determinar lead_id baseado no tipo de entidade
                lead_id = None
                if entity_type == "lead":
                    lead_id = entity_id
                elif entity_type == "contact":
                    # Para eventos de contato, tentar extrair lead_id
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after,
                                  dict) and "leads" in value_after:
                        leads_data = value_after.get("leads", [])
                        if isinstance(leads_data,
                                      list) and len(leads_data) > 0:
                            if isinstance(leads_data[0], dict):
                                lead_id = leads_data[0].get("id")

                # Extrair informações específicas para mensagens
                message_text = None
                message_source = None
                if activity_type in [
                        "incoming_chat_message", "outgoing_chat_message"
                ]:
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after, dict):
                        message_text = value_after.get("text", "")
                        message_source = value_after.get("source", "")

                # Extrair informações de mudança de status
                status_before = None
                status_after = None
                if activity_type == "lead_status_changed":
                    value_before = activity.get("value_before", {})
                    value_after = activity.get("value_after", {})
                    if isinstance(value_before, dict):
                        status_before = value_before.get("status_id")
                    if isinstance(value_after, dict):
                        status_after = value_after.get("status_id")

                # Extrair informações de tarefas
                task_text = None
                task_type = None
                if activity_type in ["task_completed", "task_added"]:
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after, dict):
                        task_text = value_after.get("text", "")
                        task_type = value_after.get("task_type_id")

                # Extrair informações de notas
                note_text = None
                if activity_type == "common_note_added":
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after, dict):
                        note_text = value_after.get("text", "")

                # Extrair informações de chamadas
                call_duration = None
                call_result = None
                if activity_type in ["incoming_call", "outgoing_call"]:
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after, dict):
                        call_duration = value_after.get("duration")
                        call_result = value_after.get("call_result")

                # Extrair informações de SMS
                sms_text = None
                if activity_type in ["incoming_sms", "outgoing_sms"]:
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after, dict):
                        sms_text = value_after.get("text", "")

                # Extrair informações de alterações de responsável
                old_responsible = None
                new_responsible = None
                if activity_type == "entity_responsible_changed":
                    value_before = activity.get("value_before", {})
                    value_after = activity.get("value_after", {})
                    if isinstance(value_before, dict):
                        old_responsible = value_before.get("responsible_user_id")
                    if isinstance(value_after, dict):
                        new_responsible = value_after.get("responsible_user_id")

                # Extrair informações de tags
                tag_name = None
                if activity_type in ["entity_tag_added", "entity_tag_deleted"]:
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after, dict):
                        tag_name = value_after.get("name", "")

                processed_activity = {
                    "id":
                    activity.get("id"),
                    "lead_id":
                    lead_id,
                    "user_id":
                    activity.get("created_by"),
                    "tipo":
                    type_mapping.get(activity_type, "outro"),
                    "valor_anterior":
                    activity.get("value_before"),
                    "valor_novo":
                    activity.get("value_after"),
                    "status_anterior":
                    status_before,
                    "status_novo":
                    status_after,
                    "texto_mensagem":
                    message_text,
                    "fonte_mensagem":
                    message_source,
                    "texto_tarefa":
                    task_text,
                    "tipo_tarefa":
                    task_type,
                    "texto_nota":
                    note_text,
                    "duracao_chamada":
                    call_duration,
                    "resultado_chamada":
                    call_result,
                    "texto_sms":
                    sms_text,
                    "responsavel_anterior":
                    old_responsible,
                    "responsavel_novo":
                    new_responsible,
                    "nome_tag":
                    tag_name,
                    "entity_type":
                    entity_type,
                    "entity_id":
                    entity_id,
                    "criado_em": (parse_datetime_sp(activity.get("created_at"))
                                  if activity.get("created_at") else None),
                }
                processed_activities.append(processed_activity)

            # Criar DataFrame e processar datas de uma vez
            df = pd.DataFrame(processed_activities)
            if not df.empty and "criado_em" in df.columns:
                df["dia_semana"] = df["criado_em"].dt.strftime("%A")
                df["hora"] = df["criado_em"].dt.hour

            logger.info("Processamento de atividades concluído com sucesso")
            return df

        except Exception as e:
            logger.error(f"Failed to retrieve activities: {str(e)}")
            raise

    def get_lead_notes(self, lead_id):
        """
        Retrieve notes for a specific lead
        Args:
            lead_id (int): ID of the lead
        """
        try:
            logger.info(f"Retrieving notes for lead {lead_id}")

            notes_data = []
            page = 1

            while True:
                response = self._make_request(
                    f"leads/{lead_id}/notes",
                    params={
                        "page": page,
                        "limit": min(250, 250)  # Respeitando limite
                    })

                if not response.get("_embedded", {}).get("notes", []):
                    break

                notes = response["_embedded"]["notes"]
                notes_data.extend(notes)
                page += 1

            processed_notes = []
            for note in notes_data:
                # Skip system/automatic notes if possible
                if note.get("created_by") == 0:  # Sistema
                    continue

                processed_notes.append({
                    "id":
                    note.get("id"),
                    "lead_id":
                    lead_id,
                    "user_id":
                    note.get("created_by"),
                    "texto":
                    note.get("text"),
                    "criado_em":
                    datetime.fromtimestamp(note.get("created_at", 0))
                    if note.get("created_at") else None
                })

            return pd.DataFrame(processed_notes)

        except Exception as e:
            logger.error(
                f"Failed to retrieve notes for lead {lead_id}: {str(e)}")
            return pd.DataFrame()

    def get_tasks(self):
        """
        Retrieve all tasks from Kommo CRM
        """
        try:
            logger.info("Retrieving tasks from Kommo CRM")

            tasks_data = []
            page = 1
            max_pages = 3  # Limit to 3 pages (reduzido conforme limitações)

            while True:
                logger.info(f"Fetching tasks page {page}")
                response = self._make_request(
                    "tasks",
                    params={
                        "page": page,
                        "limit": min(30, 250)  # Respeitando limite
                    })

                if not response.get("_embedded", {}).get("tasks", []):
                    logger.info("No more tasks found")
                    break

                tasks = response["_embedded"]["tasks"]
                tasks_data.extend(tasks)
                logger.info(
                    f"Retrieved {len(tasks)} tasks (total: {len(tasks_data)})")

                page += 1

                # Break after specified number of pages to avoid rate limiting
                if page > max_pages:
                    logger.info(
                        f"Reached maximum number of pages ({max_pages})")
                    break

            # Process tasks data into a more usable format
            processed_tasks = []
            for task in tasks_data:
                # Convert timestamps to datetime
                created_at = datetime.fromtimestamp(task.get(
                    "created_at", 0)) if task.get("created_at") else None
                updated_at = datetime.fromtimestamp(task.get(
                    "updated_at", 0)) if task.get("updated_at") else None
                complete_till = datetime.fromtimestamp(
                    task.get("complete_till",
                             0)) if task.get("complete_till") else None

                processed_tasks.append({
                    "id":
                    task.get("id"),
                    "responsavel_id":
                    task.get("responsible_user_id"),
                    "lead_id":
                    task.get("entity_id")
                    if task.get("entity_type") == "lead" else None,
                    "texto":
                    task.get("text"),
                    "tipo":
                    task.get("task_type"),
                    "completada":
                    task.get("is_completed"),
                    "criado_em":
                    created_at,
                    "atualizado_em":
                    updated_at,
                    "prazo":
                    complete_till
                })

            return pd.DataFrame(processed_tasks)

        except Exception as e:
            logger.error(f"Failed to retrieve tasks: {str(e)}")
            raise

    def _make_request_with_params(self, endpoint, base_params, retry_count=3):
        """
        Make request handling multiple parameters with same name
        """
        import urllib.parse

        url = f"{self.api_url}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        # Build query string manually to handle multiple filter[type] parameters
        query_parts = []
        for key, value in base_params.items():
            if isinstance(value, list):
                for item in value:
                    query_parts.append(
                        f"{urllib.parse.quote(key)}={urllib.parse.quote(str(item))}"
                    )
            else:
                query_parts.append(
                    f"{urllib.parse.quote(key)}={urllib.parse.quote(str(value))}"
                )

        query_string = "&".join(query_parts)
        full_url = f"{url}?{query_string}"

        for attempt in range(retry_count):
            try:
                self.rate_monitor.enforce_rate_limit()

                logger.info(f"Making API request to: {full_url}")
                response = requests.get(full_url, headers=headers)

                logger.info(f"Response status: {response.status_code}")
                logger.debug(f"Response content: {response.text[:500]}")

                response.raise_for_status()

                if not response.text.strip():
                    logger.warning("Empty response received from API")
                    return {}

                return response.json()

            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if hasattr(
                    e, 'response') else 0

                if status_code in (429, 403, 504):
                    if not self.rate_monitor.handle_kommo_error(
                            status_code, endpoint, attempt):
                        logger.error(
                            f"Stopping retries for {endpoint} due to {status_code}"
                        )
                        raise
                    self.rate_monitor.wait_before_retry(endpoint, attempt)
                else:
                    logger.warning(
                        f"API request failed (attempt {attempt+1}/{retry_count}): {str(e)}"
                    )
                    if attempt >= retry_count - 1:
                        raise
                    time.sleep(2)
