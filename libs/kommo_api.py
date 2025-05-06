import os
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KommoAPI:

    def __init__(self, api_url=None, access_token=None, supabase_client=None):
        if supabase_client:
            self.api_config = supabase_client.load_kommo_config()
            self.api_url = self.api_config.get('api_url')
            self.access_token = self.api_config.get('access_token')
        else:
            self.api_url = api_url or os.getenv("KOMMO_API_URL")
            self.access_token = access_token or os.getenv("ACCESS_TOKEN_KOMMO")
            self.api_config = {}

        if not self.api_url or not self.access_token:
            raise ValueError("API URL and access token must be provided")

        # Ensure API URL does not end with slash
        if self.api_url.endswith('/'):
            self.api_url = self.api_url[:-1]

    def _make_request(self,
                      endpoint,
                      method="GET",
                      params=None,
                      data=None,
                      retry_count=3,
                      retry_delay=2):
        """
        Make a request to the Kommo API with retry logic
        """
        url = f"{self.api_url}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        for attempt in range(retry_count):
            try:
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
                logger.warning(
                    f"API request failed (attempt {attempt+1}/{retry_count}): {str(e)}"
                )

                if attempt < retry_count - 1:
                    time.sleep(retry_delay)
                else:
                    raise

    def get_users(self, active_only=True):
        """
        Retrieve users (brokers) from Kommo CRM

        Args:
            active_only (bool): If True, only return active users
        """
        try:
            logger.info("Retrieving users from Kommo CRM")

            users_data = []
            page = 1

            while True:
                response = self._make_request("users",
                                              params={
                                                  "page": page,
                                                  "limit": 30
                                              })

                if not response.get("_embedded", {}).get("users", []):
                    break

                users = response["_embedded"]["users"]
                users_data.extend(users)
                page += 1

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
                    "criado_em":
                    user.get("created_at"),
                    "cargo":
                    user.get("rights", {}).get("is_admin") and "Administrador"
                    or "Corretor"
                })

            return pd.DataFrame(processed_users)

        except Exception as e:
            logger.error(f"Failed to retrieve users: {str(e)}")
            raise

    def _get_date_filters(self):
        """Obtém os filtros de data da configuração"""
        try:
            # Load config directly from supabase since it's already loaded in constructor
            start_date = self.api_config.get('sync_start_date')
            end_date = self.api_config.get('sync_end_date')

            return start_date, end_date
        except Exception as e:
            logger.error(f"Erro ao obter filtros de data: {str(e)}")
            return None, None

    def get_leads(self):
        """
        Retrieve leads from Kommo CRM filtering only pipeline_id = 8865067
        and map status_id to status name (etapa)
        """
        try:
            logger.info("Buscando etapas do pipeline 8865067")
            pipeline_response = self._make_request("leads/pipelines")
            pipelines = pipeline_response.get("_embedded",
                                              {}).get("pipelines", [])

            status_map = {}
            for pipeline in pipelines:
                if pipeline.get("id") == 8865067:
                    for status in pipeline.get("_embedded",
                                               {}).get("statuses", []):
                        status_id = status.get("id")
                        status_name = status.get("name")
                        status_map[status_id] = status_name
                    break

            logger.info("Etapas carregadas com sucesso")

            logger.info(
                "Retrieving leads from Kommo CRM (pipeline_id = 8865067)")

            filtered_leads = []
            page = 1
            empty_streak = 0
            stop_after = 1  # Para após 1 página vazia

            while True:
                time.sleep(1)
                start_ts, end_ts = self._get_date_filters()
                params = {
                    "page":
                    page,
                    "limit":
                    250,
                    "filter[pipeline_id]":
                    8865067,
                    "with":
                    "contacts,pipeline_id,loss_reason,catalog_elements,company"
                }

                if start_ts:
                    params["filter[created_at][from]"] = start_ts
                if end_ts:
                    params["filter[created_at][to]"] = end_ts

                response = self._make_request("leads", params=params)

                leads = response.get("_embedded", {}).get("leads", [])
                filtered_page_leads = [
                    lead for lead in leads
                    if lead.get("pipeline_id") == 8865067
                ]

                if not filtered_page_leads:
                    empty_streak += 1
                    if empty_streak >= stop_after:
                        logger.info(
                            f"Parando busca: {stop_after} páginas vazias consecutivas"
                        )
                        break
                else:
                    empty_streak = 0
                    filtered_leads.extend(filtered_page_leads)

                logger.info(
                    f"Processada página {page}, encontrados {len(filtered_page_leads)} leads"
                )
                page += 1

            logger.info(
                f"Total de leads com pipeline 8865067: {len(filtered_leads)}")

            processed_leads = []
            for lead in filtered_leads:
                responsavel_id = lead.get("responsible_user_id")
                contato_nome = ""
                if lead.get("_embedded", {}).get("contacts"):
                    contato_nome = lead["_embedded"]["contacts"][0].get(
                        "name", "")

                created_at = datetime.fromtimestamp(lead.get(
                    "created_at", 0)) if lead.get("created_at") else None
                updated_at = datetime.fromtimestamp(lead.get(
                    "updated_at", 0)) if lead.get("updated_at") else None

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
                    "criado_em":
                    created_at,
                    "atualizado_em":
                    updated_at,
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

    def get_activities(self, page_size=250, max_workers=5, max_pages=500):
        """
        Retrieve activities from Kommo CRM using parallel requests with proper filtering
        
        Args:
            page_size (int): Number of records per page
            max_workers (int): Maximum number of parallel requests
            max_pages (int): Maximum number of pages to fetch
        
        Returns:
            pd.DataFrame: Processed activities data
        """
        try:
            logger.info("Retrieving activities from Kommo CRM")

            start_ts, end_ts = self._get_date_filters()
            base_params = {
                "limit":
                page_size,
                "filter[type]": [
                    "lead_status_changed", "incoming_chat_message",
                    "outgoing_chat_message"
                ]
            }

            if start_ts:
                base_params["filter[created_at][from]"] = start_ts
            if end_ts:
                base_params["filter[created_at][to]"] = end_ts

            def fetch_page(page):
                try:
                    time.sleep(1)  # Rate limiting
                    params = {**base_params, "page": page}
                    response = self._make_request("events", params=params)
                    return response.get("_embedded", {}).get("events", [])
                except Exception as e:
                    logger.error(f"Error fetching page {page}: {e}")
                    return []

            activities_data = []
            page = 1
            empty_streak = 0
            stop_after = 1

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                while page <= max_pages:
                    # Define batch of pages to fetch
                    current_batch = range(
                        page, min(page + max_workers, max_pages + 1))
                    futures = {
                        executor.submit(fetch_page, p): p
                        for p in current_batch
                    }

                    batch_empty = True
                    for future in as_completed(futures):
                        page_num = futures[future]
                        events = future.result()

                        if events:
                            batch_empty = False
                            activities_data.extend(events)
                            logger.info(
                                f"Page {page_num} fetched: {len(events)} events"
                            )

                    if batch_empty:
                        empty_streak += 1
                        if empty_streak >= stop_after:
                            logger.info(
                                f"Stopping: {stop_after} empty batches")
                            break
                    else:
                        empty_streak = 0

                    page += len(current_batch)

                    # Safety check for maximum pages
                    if page > max_pages:
                        logger.info(
                            f"Reached maximum number of pages ({max_pages})")
                        break

            total_activities = len(activities_data)
            logger.info(f"Total de atividades recuperadas: {total_activities}")

            if not activities_data:
                logger.warning("Nenhuma atividade encontrada")
                return pd.DataFrame()

            # Log event types for debugging
            event_types = set(event.get('type') for event in activities_data)
            logger.info(f"Event types found: {event_types}")

            # Processamento otimizado usando list comprehension
            type_mapping = {
                "lead_status_changed": "mudança_status",
                "incoming_chat_message": "mensagem_recebida",
                "outgoing_chat_message": "mensagem_enviada",
                "task_completed": "tarefa_concluida"
            }

            processed_activities = [{
                "id":
                activity.get("id"),
                "lead_id":
                activity.get("entity_id")
                if activity.get("entity_type") == "lead" else None,
                "user_id":
                activity.get("created_by"),
                "tipo":
                type_mapping.get(activity.get("type"), "outro"),
                "valor_anterior":
                activity.get("value_before"),
                "valor_novo":
                activity.get("value_after"),
                "criado_em":
                datetime.fromtimestamp(activity.get("created_at", 0))
                if activity.get("created_at") else None
            } for activity in activities_data]

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

    def get_tasks(self):
        """
        Retrieve all tasks from Kommo CRM
        """
        try:
            logger.info("Retrieving tasks from Kommo CRM")

            tasks_data = []
            page = 1
            max_pages = 3  # Limit to 3 pages (150 tasks)

            while True:
                logger.info(f"Fetching tasks page {page}")
                response = self._make_request("tasks",
                                              params={
                                                  "page": page,
                                                  "limit": 30
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
