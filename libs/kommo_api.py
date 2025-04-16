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

    def __init__(self, api_url=None, access_token=None):
        self.api_url = api_url or os.getenv("KOMMO_API_URL")
        self.access_token = access_token or os.getenv("ACCESS_TOKEN_KOMMO")

        if not self.api_url or not self.access_token:
            raise ValueError("API URL and access token must be provided")

        # Validate token format
        if not self.access_token.strip().startswith("Bearer "):
            self.access_token = f"Bearer {self.access_token.strip()}"

        # Ensure API URL does not end with slash
        if self.api_url.endswith('/'):
            self.api_url = self.api_url[:-1]

        # Validate token on initialization
        self._validate_token()

    def _validate_token(self):
        """Validate the access token by making a test request"""
        try:
            response = requests.get(
                f"{self.api_url}/api/v4/account",
                headers={"Authorization": self.access_token}
            )

            if response.status_code == 403:
                raise ValueError(
                    "Invalid or expired access token. Please check your credentials."
                )
        except Exception as e:
            logger.error(f"Failed to validate token: {str(e)}")
            raise

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
        url = f"{self.api_url}/api/v4/{endpoint}"
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

    def get_leads(self):
        """
        Retrieve leads from Kommo CRM filtering only pipeline_id = 8865067
        with rate limiting
        """
        try:
            logger.info(
                "Retrieving leads from Kommo CRM (pipeline_id = 8865067)")

            filtered_leads = []
            page = 1
            empty_streak = 0
            stop_after = 3  # Para após 3 páginas vazias consecutivas

            while True:
                # Rate limiting - espera 1 segundo entre requests
                time.sleep(1)

                response = self._make_request(
                    "leads",
                    params={
                        "page":
                        page,
                        "limit":
                        50,
                        "with":
                        "contacts,pipeline_id,loss_reason,catalog_elements,company"
                    })

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

                page += 1
                logger.info(
                    f"Processada página {page-1}, encontrados {len(filtered_page_leads)} leads"
                )

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
                    lead.get("status_id"),
                    "pipeline_id":
                    lead.get("pipeline_id"),
                    "etapa":
                    lead.get("_embedded",
                             {}).get("status", {}).get("name", "Desconhecido"),
                    "criado_em":
                    created_at,
                    "atualizado_em":
                    updated_at,
                    "fechado":
                    lead.get("closed_at") is not None,
                    "status":
                    ("Ganho" if lead.get("status_id") == 142 else "Perdido"
                     if lead.get("status_id") == 143 else "Em progresso")
                })

            return pd.DataFrame(processed_leads)

        except Exception as e:
            logger.error(f"Failed to retrieve leads: {str(e)}")
            raise

    def get_activities(self):
        """
        Retrieve all activities from Kommo CRM with rate limiting
        """
        try:
            logger.info("Retrieving activities from Kommo CRM")

            activities_data = []
            page = 1
            empty_streak = 0
            stop_after = 3  # Para após 3 páginas vazias consecutivas

            now = int(time.time())
            filter_from = now - (7 * 24 * 60 * 60)

            while True:
                # Rate limiting - espera 1 segundo entre requests
                time.sleep(1)

                response = self._make_request(
                    "events",
                    params={
                        "page": page,
                        "limit": 500,
                        "filter[type]":
                        "lead_status_changed,incoming_chat_message,outgoing_chat_message,task_completed",
                        "filter[created_at][from]": filter_from,
                    })

                events = response.get("_embedded", {}).get("events", [])

                if not events:
                    empty_streak += 1
                    if empty_streak >= stop_after:
                        logger.info(
                            f"Parando busca: {stop_after} páginas vazias consecutivas"
                        )
                        break
                else:
                    empty_streak = 0
                    activities_data.extend(events)

                page += 1
                logger.info(
                    f"Processada página {page-1}, encontrados {len(events)} eventos"
                )

            logger.info(
                f"Total de atividades recuperadas: {len(activities_data)}")

            # Processamento
            processed_activities = []
            for activity in activities_data:
                created_at = datetime.fromtimestamp(
                    activity.get("created_at",
                                 0)) if activity.get("created_at") else None

                activity_type = {
                    "lead_status_changed": "mudança_status",
                    "incoming_chat_message": "mensagem_recebida",
                    "outgoing_chat_message": "mensagem_enviada",
                    "task_completed": "tarefa_concluida"
                }.get(activity.get("type"), "outro")

                processed_activities.append({
                    "id":
                    activity.get("id"),
                    "lead_id":
                    activity.get("entity_id")
                    if activity.get("entity_type") == "lead" else None,
                    "user_id":
                    activity.get("created_by"),
                    "tipo":
                    activity_type,
                    "valor_anterior":
                    activity.get("value_before"),
                    "valor_novo":
                    activity.get("value_after"),
                    "criado_em":
                    created_at,
                    "dia_semana":
                    created_at.strftime("%A") if created_at else None,
                    "hora":
                    created_at.hour if created_at else None
                })

            return pd.DataFrame(processed_activities)

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
