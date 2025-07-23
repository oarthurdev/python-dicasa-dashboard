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

    def __init__(self, api_url=None, access_token=None, api_config=None):
        try:
            logger.info("Initializing KommoAPI")

            self.api_config = api_config
            self.api_url = api_url or (api_config.get('api_url') if api_config
                                       else None) or os.getenv("KOMMO_API_URL")
            self.access_token = access_token or (
                api_config.get('access_token')
                if api_config else None) or os.getenv("ACCESS_TOKEN_KOMMO")
            self.start_date = None
            self.end_date = None

            if not self.api_url or not self.access_token:
                raise ValueError("API URL and access token must be provided")

            # Ensure API URL does not end with slash
            if self.api_url.endswith('/'):
                self.api_url = self.api_url[:-1]

            logger.info("KommoAPI initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing KommoAPI: {str(e)}")
            raise

    def set_date_range(self, start_date, end_date):
        """Set date range for API queries"""
        self.start_date = start_date
        self.end_date = end_date
        if isinstance(start_date, datetime):
            self.api_config['sync_start_date'] = int(start_date.timestamp())
        if isinstance(end_date, datetime):
            self.api_config['sync_end_date'] = int(end_date.timestamp())

    def __init__(self, api_url=None, access_token=None, api_config=None):
        try:
            logger.info("Initializing KommoAPI")

            from .rate_limit_monitor import RateLimitMonitor
            self.rate_monitor = RateLimitMonitor()

            self.api_config = api_config
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
        """Define limites seguros para paginação e evita processamento infinito"""
        return {
            'max_pages_per_request': 100,  # Máximo 100 páginas por chamada
            'max_total_records': 10000,  # Máximo 10k registros por empresa
            'page_size': 50,  # Tamanho da página reduzido para evitar timeouts
            'delay_between_pages': 0.2  # 200ms entre páginas
        }

    def get_leads(self, company_id=None):
        """
        Retrieve all leads from Kommo CRM for specific company with safe pagination
        Args:
            company_id (str): Optional company ID to filter leads
        """
        try:
            # Get pipeline_id and company_id from config for proper filtering
            pipeline_id = self.api_config.get('pipeline_id')
            company_id = self.api_config.get('company_id')

            # Get safe pagination limits
            limits = self._get_safe_pagination_limits()

            logger.info(
                f"Buscando etapas do pipeline {pipeline_id if pipeline_id else 'all'}"
            )
            pipeline_response = self._make_request("leads/pipelines")
            pipelines = pipeline_response.get("_embedded",
                                              {}).get("pipelines", [])

            status_map = {}
            for pipeline in pipelines:
                # Only get statuses for configured pipeline if pipeline_id exists
                if not pipeline_id or str(
                        pipeline.get('id')) == str(pipeline_id):
                    for status in pipeline.get("_embedded",
                                               {}).get("statuses", []):
                        status_id = status.get("id")
                        status_name = status.get("name")
                        status_map[status_id] = status_name
                    if pipeline_id:  # If we found our pipeline, no need to continue
                        break

            logger.info("Etapas carregadas com sucesso")
            logger.info(
                "Retrieving ALL leads from Kommo CRM (no date filters)")

            filtered_leads = []
            page = 1
            per_page = limits['page_size']
            empty_streak = 0
            max_empty_streak = 3

            while page <= limits['max_pages_per_request'] and len(
                    filtered_leads) < limits['max_total_records']:
                params = {
                    "page":
                    page,
                    "limit":
                    per_page,
                    "with":
                    "contacts,pipeline_id,loss_reason,catalog_elements,company"
                }

                # Add pipeline filter if configured
                if pipeline_id:
                    params["filter[pipeline_id]"] = pipeline_id

                try:
                    response = self._make_request("leads", params=params)
                    leads = response.get("_embedded", {}).get("leads", [])

                    if leads:
                        filtered_leads.extend(leads)
                        empty_streak = 0
                        logger.info(
                            f"Página {page}: {len(leads)} leads encontrados (total: {len(filtered_leads)})"
                        )
                    else:
                        empty_streak += 1
                        logger.info(
                            f"Página {page}: nenhum lead encontrado (streak: {empty_streak})"
                        )

                        if empty_streak >= max_empty_streak:
                            logger.info(
                                f"Parando: {max_empty_streak} páginas vazias consecutivas"
                            )
                            break

                    # Check if we received less than per_page items (end of data)
                    if len(leads) < per_page:
                        logger.info(
                            "Última página atingida (menos registros que o limite)"
                        )
                        break

                    page += 1

                    # Rate limiting delay between pages
                    time.sleep(limits['delay_between_pages'])

                except Exception as e:
                    logger.error(f"Erro na página {page}: {str(e)}")
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

            base_params = {
                "limit":
                limits['page_size'],
                "filter[type]": [
                    "lead_status_changed", "incoming_chat_message",
                    "outgoing_chat_message", "task_completed", "task_added",
                    "common_note_added", "outgoing_call"
                ]
            }

            activities_data = []
            page = 1
            empty_streak = 0
            max_empty_streak = 3

            while page <= limits['max_pages_per_request'] and len(
                    activities_data) < limits['max_total_records']:
                try:
                    params = {**base_params, "page": page}
                    response = self._make_request_with_params("events", base_params)

                    events = []
                    if isinstance(response, dict):
                        events = response.get("_embedded",
                                              {}).get("events", [])

                    if events:
                        activities_data.extend(events)
                        empty_streak = 0
                        logger.info(
                            f"Página {page}: {len(events)} atividades encontradas (total: {len(activities_data)})"
                        )
                    else:
                        empty_streak += 1
                        logger.info(
                            f"Página {page}: nenhuma atividade encontrada (streak: {empty_streak})"
                        )

                        if empty_streak >= max_empty_streak:
                            logger.info(
                                f"Parando: {max_empty_streak} páginas vazias consecutivas"
                            )
                            break

                    # Check if we received less than page_size items (end of data)
                    if len(events) < limits['page_size']:
                        logger.info(
                            "Última página atingida (menos registros que o limite)"
                        )
                        break

                    page += 1

                    # Rate limiting delay between pages
                    time.sleep(limits['delay_between_pages'])

                except Exception as e:
                    logger.error(f"Erro na página {page}: {str(e)}")
                    # Continue to next page on error
                    page += 1
                    time.sleep(1)  # Extra delay on error

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

            if not activities_data:
                logger.warning("Nenhuma atividade encontrada")
                return pd.DataFrame()

            # Log event types for debugging
            event_types = set(event.get('type') for event in activities_data)
            logger.info(f"Event types found: {event_types}")

            # Mapeamento correto dos eventos da Kommo API
            type_mapping = {
                "lead_status_changed": "mudança_status",
                "incoming_chat_message": "mensagem_recebida",
                "outgoing_chat_message": "mensagem_enviada",
                "task_completed": "tarefa_concluida",
                "task_added": "tarefa_criada",
                "common_note_added": "nota_adicionada",
                "outgoing_call": "ligacao_realizada"
            }

            processed_activities = []
            for activity in activities_data:
                # Extrair informações específicas baseado no tipo de evento
                activity_type = activity.get("type", "")
                entity_type = activity.get("entity_type", "")
                entity_id = activity.get("entity_id")

                # Determinar lead_id baseado no tipo de entidade
                lead_id = None
                if entity_type == "lead":
                    lead_id = entity_id
                elif entity_type == "contact" and activity.get(
                        "value_after", {}).get("leads"):
                    # Para eventos de contato, tentar extrair lead_id
                    leads_data = activity.get("value_after",
                                              {}).get("leads", [])
                    if leads_data and isinstance(leads_data,
                                                 list) and len(leads_data) > 0:
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
                if activity_type in ["task_completed", "task_created"]:
                    value_after = activity.get("value_after", {})
                    if isinstance(value_after, dict):
                        task_text = value_after.get("text", "")
                        task_type = value_after.get("task_type_id")

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
                    query_parts.append(f"{urllib.parse.quote(key)}={urllib.parse.quote(str(item))}")
            else:
                query_parts.append(f"{urllib.parse.quote(key)}={urllib.parse.quote(str(value))}")

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
                status_code = e.response.status_code if hasattr(e, 'response') else 0

                if status_code in (429, 403, 504):
                    if not self.rate_monitor.handle_kommo_error(status_code, endpoint, attempt):
                        logger.error(f"Stopping retries for {endpoint} due to {status_code}")
                        raise
                    self.rate_monitor.wait_before_retry(endpoint, attempt)
                else:
                    logger.warning(f"API request failed (attempt {attempt+1}/{retry_count}): {str(e)}")
                    if attempt >= retry_count - 1:
                        raise
                    time.sleep(2)