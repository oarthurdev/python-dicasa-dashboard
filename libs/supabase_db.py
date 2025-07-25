import os
from libs.kommo_api import KommoAPI
from libs.sync_manager import SyncManager
from supabase import create_client
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import requests
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SupabaseClient:

    def __init__(self, url=None, key=None):
        self.url = url or os.getenv("VITE_SUPABASE_URL")
        self.key = key or os.getenv("VITE_SUPABASE_ANON_KEY")

        if not self.url or not self.key:
            raise ValueError("Supabase URL and key must be provided")

        try:
            self.client = create_client(self.url, self.key)
            logger.info("Supabase client initialized successfully")
            self.kommo_config = None
            self.rules = None
            self.last_check = datetime.now()

            # Try initial load of config and rules
            self._load_initial_config()

        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            raise

    def _load_initial_config(self):
        """Try to load initial configuration without raising errors"""
        try:
            config = self.client.table("kommo_config").select("*").eq(
                "active", True).execute()
            if config.data:
                self.kommo_config = config.data[0]

                if not self.kommo_config.get('company_id'):
                    company_id = self._get_company_id(
                        self.kommo_config['api_url'],
                        self.kommo_config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', self.kommo_config['id']).execute()
                    self.kommo_config['company_id'] = company_id

                try:
                    self.rules = self.load_rules()
                except Exception as e:
                    logger.warning(f"Could not load rules: {e}")
                    self.rules = {}

                logger.info("Initial configuration loaded successfully")
                return

            logger.info("Waiting for Kommo configuration to be added...")
        except Exception as e:
            logger.error(f"Error loading initial configuration: {str(e)}")

    def _handle_config_update(self, updated_config):
        """Handle kommo_config updates"""
        try:
            if updated_config and updated_config != self.kommo_config:
                logger.info("Kommo configuration updated")
                self.kommo_config = updated_config

                if not updated_config.get('company_id'):
                    company_id = self._get_company_id(
                        updated_config['api_url'],
                        updated_config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', updated_config['id']).execute()
                    updated_config['company_id'] = company_id

                self._sync_all_data(updated_config)
                logger.info("Configuration update handled successfully")
        except Exception as e:
            logger.error(f"Failed to handle config update: {str(e)}")

    def check_config_changes(self):
        """Check for configuration changes periodically"""
        try:
            current_time = datetime.now()
            if (current_time - self.last_check
                ).total_seconds() < 30:  # Check every 30 seconds
                return

            self.last_check = current_time
            result = self.client.table("kommo_config").select("*").execute()

            if not result.data:
                return

            new_config = result.data[0]

            if not self.kommo_config:
                logger.info("New Kommo configuration detected")
                self._handle_config_insert({"new": new_config})
            elif new_config != self.kommo_config:
                logger.info("Kommo configuration updated")
                self._handle_config_update(new_config)

                # Atualiza a cópia da config local
                self.kommo_config = new_config

                if not new_config.get('company_id'):
                    company_id = self._get_company_id(
                        new_config['api_url'], new_config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', new_config['id']).execute()
                    new_config['company_id'] = company_id

                self._sync_all_data(new_config)
                logger.info("Configuration update handled successfully")

        except Exception as e:
            logger.error(f"Failed to check or handle config changes: {str(e)}")

    def _sync_company_data(self, config, company_id):
        """Separate thread function to handle company data synchronization"""
        try:
            logger.info(f"Starting sync thread for company {company_id}")
            kommo_api = KommoAPI(api_url=config['api_url'],
                                 access_token=config['access_token'])
            sync_manager = SyncManager(kommo_api, self, config)

            brokers = kommo_api.get_users()
            leads = kommo_api.get_leads()
            activities = kommo_api.get_activities()

            # Add company_id to all DataFrames
            if not brokers.empty:
                brokers['company_id'] = company_id
            if not leads.empty:
                leads['company_id'] = company_id
            if not activities.empty:
                activities['company_id'] = company_id

            # Sync all data with company_id
            sync_manager.sync_data(brokers=brokers,
                                   leads=leads,
                                   activities=activities,
                                   company_id=company_id)

            # Initialize broker points for this company
            self.initialize_broker_points(company_id)

            # Update broker points after sync
            self.update_broker_points(brokers=brokers,
                                      leads=leads,
                                      activities=activities,
                                      company_id=company_id)
        except Exception as e:
            logger.error(
                f"Error in sync thread for company {company_id}: {str(e)}")

    def _handle_config_insert(self, event):
        """Handle new kommo_config insertion"""
        try:
            new_config = event.get("new", {})
            if new_config:
                logger.info("New Kommo configuration detected")
                self.kommo_config = new_config

                # Get company_id and update config
                company_id = self._get_company_id(new_config['api_url'],
                                                  new_config['access_token'])
                self.client.table("kommo_config").update({
                    'company_id': company_id,
                    'active': True
                }).eq('id', new_config['id']).execute()

                # Trigger sync through FastAPI endpoint
                try:
                    response = requests.post("http://0.0.0.0:5002/start")
                    if response.status_code == 200:
                        logger.info("Sync started for all companies")

                        while True:
                            try:
                                status_response = requests.get("http://0.0.0.0:5002/status")
                                if status_response.status_code == 200:
                                    all_status = status_response.json()
                                    company_status = all_status.get(str(company_id))

                                    if not company_status:
                                        logger.error(f"No status found for company {company_id}")
                                        break

                                    status = company_status.get('status')

                                    if status in ('initializing', 'running'):
                                        logger.info(f"Company {company_id} sync in progress: {status}")
                                        time.sleep(30)  # Check every 30 seconds
                                        continue
                                    else:
                                        logger.info(f"Sync completed for company {company_id} with status: {status}")
                                        break
                                else:
                                    logger.error(f"Failed to get sync status. HTTP {status_response.status_code}")
                                    break
                            except Exception as e:
                                logger.error(f"Exception while checking sync status: {e}")
                                break
                    else:
                        logger.error(f"Failed to start sync for company {company_id}")
                except Exception as e:
                    logger.error(f"Error in sync process: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            raise

    def insert_log(self, type: str, message: str):
        """Insere um log na tabela sync_logs"""
        try:
            self.client.table("sync_logs").insert({
                "timestamp":
                datetime.now().isoformat(),
                "type":
                type,
                "message":
                message,
                "company_id":
                self.kommo_config.get('company_id')
            }).execute()
        except Exception as e:
            logger.error(f"Failed to insert log: {str(e)}")

    def load_kommo_config(self, company_id=None):
        """Load Kommo API configuration from Supabase"""
        try:
            query = self.client.table("kommo_config").select("*").eq(
                "active", True)

            if company_id:
                query = query.eq("company_id", company_id)

            result = query.execute()
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            if not result.data:
                # Apenas ignorar e retornar lista vazia para quem chamar essa função
                return []

            configs = result.data
            for config in configs:
                if config.get('company_id') is None:
                    company_id = self._get_company_id(config['api_url'],
                                                      config['access_token'])
                    self.client.table("kommo_config").update({
                        'company_id':
                        company_id
                    }).eq('id', config['id']).execute()
                    config['company_id'] = company_id
                    self._sync_all_data(config)

            return configs
        except Exception as e:
            logger.error(f"Failed to load Kommo config: {str(e)}")
            raise

    def _get_company_id(self, api_url, access_token):
        """Get company ID from Kommo API"""
        try:
            response = requests.get(
                f"{api_url}/api/v4/account",
                headers={"Authorization": f"Bearer {access_token}"})
            response.raise_for_status()
            return response.json().get('id')
        except Exception as e:
            logger.error(f"Failed to get company ID: {str(e)}")
            raise

    def _sync_all_data(self, config):
        """Trigger sync for all tables with company_id"""
        try:
            kommo_api = KommoAPI(api_url=config['api_url'],
                                 access_token=config['access_token'])
            brokers = kommo_api.get_users()
            leads = kommo_api.get_leads()
            activities = kommo_api.get_activities()

            # Add company_id to all DataFrames
            for df in [brokers, leads, activities]:
                if not df.empty:
                    df['company_id'] = config['company_id']

            # Sync all data
            self.upsert_brokers(brokers)
            self.upsert_leads(leads)
            self.upsert_activities(activities)

            # Initialize broker points with company_id
            self.initialize_broker_points(config['company_id'])

        except Exception as e:
            logger.error(f"Failed to sync data: {str(e)}")
            raise

    def load_rules(self):
        """Load gamification rules from Supabase"""
        try:
            result = self.client.table("rules").select("*").execute()
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            if not result.data:
                raise ValueError("No gamification rules found")

            rules_dict = {}
            for rule in result.data:
                rules_dict[rule['coluna_nome']] = rule['pontos']

            return rules_dict
        except Exception as e:
            logger.error(f"Failed to load rules: {str(e)}")
            raise

    def upsert_brokers(self, brokers_df):
        """
        Insert or update broker data in the Supabase database

        Args:
            brokers_df (pandas.DataFrame): DataFrame containing broker data
        """
        try:
            if brokers_df.empty:
                logger.warning("No broker data to insert")
                return

            logger.info(f"Upserting {len(brokers_df)} brokers to Supabase")

            # Filtrar apenas corretores
            brokers_df_filtered = brokers_df[brokers_df['cargo'] == 'Corretor'].copy()

            if brokers_df_filtered.empty:
                logger.warning("No brokers with 'Corretor' role found")
                return

            # Convert DataFrame to list of dicts
            brokers_data = brokers_df_filtered.to_dict(orient="records")

            # Add updated_at timestamp
            for broker in brokers_data:
                broker["updated_at"] = datetime.now().isoformat()

            # Upsert data to Supabase - inserir novos e atualizar existentes
            result = self.client.table("brokers").upsert(brokers_data, on_conflict='id').execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info(f"Brokers upserted successfully: {len(brokers_data)} records processed")
            return result

        except Exception as e:
            logger.error(f"Failed to upsert brokers: {str(e)}")
            raise


    def upsert_activities(self, activities_df):
        """
        Insert or update activity data in the Supabase database

        Args:
            activities_df (pandas.DataFrame): DataFrame containing activity data
        """
        try:
            if activities_df.empty:
                logger.warning("No activity data to insert")
                return

            logger.info(f"Processing {len(activities_df)} activities")

            # First, get a list of all lead_ids in the leads table
            try:
                # Query existing lead IDs from the database to ensure we only insert activities for existing leads
                leads_result = self.client.table("leads").select(
                    "id").execute()
                if hasattr(leads_result, "error") and leads_result.error:
                    raise Exception(
                        f"Supabase error querying leads: {leads_result.error}")

                # Create a set of existing lead IDs for faster lookup
                existing_lead_ids = set()
                for lead in leads_result.data:
                    existing_lead_ids.add(lead['id'])

                logger.info(
                    f"Found {len(existing_lead_ids)} existing leads in database"
                )
            except Exception as e:
                logger.warning(
                    f"Could not query existing leads, proceeding without validation: {str(e)}"
                )
                existing_lead_ids = None

            # Make a copy of the DataFrame to avoid modifying the original
            activities_df_clean = activities_df.copy()

            # Replace infinite values with None (null in JSON)
            numeric_cols = activities_df_clean.select_dtypes(
                include=['float', 'int']).columns
            for col in numeric_cols:
                # Replace NaN and infinite values with None
                mask = ~np.isfinite(activities_df_clean[col])
                if mask.any():
                    activities_df_clean.loc[mask, col] = None

            # Convert bigint columns from float to int to avoid "invalid input syntax for type bigint" errors
            bigint_columns = ['lead_id', 'user_id']
            for col in bigint_columns:
                if col in activities_df_clean.columns:
                    # Only convert finite values (NaN/None will be handled separately)
                    mask = np.isfinite(activities_df_clean[col])
                    if mask.any():
                        activities_df_clean.loc[mask,
                                                col] = activities_df_clean.loc[
                                                    mask, col].astype('Int64')

            # The 'id' column in activities table is of type TEXT in SQL, but Kommo API might return it as a number
            # We need to ensure it's converted to string
            if 'id' in activities_df_clean.columns:
                activities_df_clean['id'] = activities_df_clean['id'].astype(
                    str)

            # Get a list of all broker_ids in the brokers table
            try:
                # Query existing broker IDs from the database to ensure we only insert activities with valid user_ids
                brokers_result = self.client.table("brokers").select(
                    "id").execute()
                if hasattr(brokers_result, "error") and brokers_result.error:
                    raise Exception(
                        f"Supabase error querying brokers: {brokers_result.error}"
                    )

                # Create a set of existing broker IDs for faster lookup
                existing_broker_ids = set()
                for broker in brokers_result.data:
                    existing_broker_ids.add(broker['id'])

                logger.info(
                    f"Found {len(existing_broker_ids)} existing brokers in database"
                )
            except Exception as e:
                logger.warning(
                    f"Could not query existing brokers, proceeding without validation: {str(e)}"
                )
                existing_broker_ids = None

            # Filter activities to only include those with existing lead_ids and user_ids
            filter_needed = False

            # Filter by lead_id
            if existing_lead_ids is not None and 'lead_id' in activities_df_clean.columns:
                filter_needed = True
                original_count = len(activities_df_clean)
                activities_df_clean = activities_df_clean[
                    activities_df_clean['lead_id'].isin(existing_lead_ids)
                    | activities_df_clean['lead_id'].isna()]
                filtered_count = len(activities_df_clean)
                if filtered_count < original_count:
                    logger.warning(
                        f"Filtered out {original_count - filtered_count} activities with non-existent lead_ids"
                    )

            # Filter by user_id
            if existing_broker_ids is not None and 'user_id' in activities_df_clean.columns:
                filter_needed = True
                original_count = len(activities_df_clean)
                activities_df_clean = activities_df_clean[
                    activities_df_clean['user_id'].isin(existing_broker_ids)
                    | activities_df_clean['user_id'].isna()]
                filtered_count = len(activities_df_clean)
                if filtered_count < original_count:
                    logger.warning(
                        f"Filtered out {original_count - filtered_count} activities with non-existent user_ids"
                    )

            # If we have no activities after filtering, exit early
            if activities_df_clean.empty:
                logger.warning("No valid activities to insert after filtering")
                return

            logger.info(
                f"Upserting {len(activities_df_clean)} activities to Supabase")

            # Convert DataFrame to list of dicts
            activities_data = activities_df_clean.to_dict(orient="records")

            # Add updated_at timestamp and convert datetime objects
            for activity in activities_data:
                activity["updated_at"] = datetime.now().isoformat()

                # Convert datetime objects to ISO format
                if "criado_em" in activity and activity[
                        "criado_em"] is not None:
                    activity["criado_em"] = activity["criado_em"].isoformat()

                # Additional check for any remaining non-JSON compatible values and type conversions
                for key, value in list(
                        activity.items()
                ):  # Create a list to avoid "dictionary changed size during iteration"
                    # Check for NaN, Infinity, -Infinity in float values
                    if isinstance(value, float) and (np.isnan(value)
                                                     or np.isinf(value)):
                        activity[key] = None
                    # Convert float to int for bigint columns
                    elif key in bigint_columns and isinstance(
                            value, float) and value.is_integer():
                        activity[key] = int(value)

            # Upsert data to Supabase - inserir novos e atualizar existentes
            result = self.client.table("activities").upsert(
                activities_data, on_conflict='id').execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info(f"Activities upserted successfully: {len(activities_data)} records processed")
            return result

        except Exception as e:
            logger.error(f"Failed to upsert activities: {str(e)}")
            raise

    def get_broker_points(self):
        """
        Retrieve broker points from the Supabase database
        """
        try:
            logger.info("Retrieving broker points from Supabase")

            result = self.client.table("broker_points").select("*").execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            if not result.data:
                return pd.DataFrame()

            return pd.DataFrame(result.data)

        except Exception as e:
            logger.error(f"Failed to retrieve broker points: {str(e)}")
            raise

    def upsert_broker_points(self, points_df):
        """
        Atualiza ou insere os dados na tabela broker_points no Supabase.

        Args:
            points_df (pandas.DataFrame): DataFrame contendo os dados de pontuação dos corretores.
        """
        import numpy as np
        import logging

        logger = logging.getLogger(__name__)

        try:
            if points_df.empty:
                logger.warning("Nenhum dado de pontos para inserir.")
                return

            # Garante que company_id está presente
            if 'company_id' not in points_df.columns:
                logger.error("DataFrame não contém a coluna company_id")
                return

            # Filtra registros por company_id
            unique_companies = points_df['company_id'].unique()
            all_responses = []

            for company_id in unique_companies:
                company_df = points_df[points_df['company_id'] ==
                                       company_id].copy()

                logger.info(
                    f"Upsert de {len(company_df)} registros na tabela broker_points para company_id {company_id}."
                )

                # Trata valores infinitos ou inválidos
                numeric_cols = company_df.select_dtypes(
                    include=['float', 'int']).columns
                for col in numeric_cols:
                    mask = ~np.isfinite(company_df[col])
                    if mask.any():
                        company_df.loc[mask, col] = None

                # Realiza o upsert na tabela broker_points
                records = company_df.to_dict("records")
                for record in records:
                    for key, value in record.items():
                        if isinstance(value, pd.Timestamp):
                            record[key] = value.isoformat()

                # Verifica se os registros já existem e faz update ou insert
                for record in records:
                    broker_id = record.get('id')
                    if broker_id:
                        try:
                            # Verifica se o registro já existe
                            existing = self.client.table("broker_points").select("id").eq(
                                "id", broker_id).eq("company_id", company_id).execute()
                            
                            if existing.data:
                                # Update se existe - remove campos que não devem ser atualizados na condição
                                update_record = {k: v for k, v in record.items() if k not in ['id', 'company_id']}
                                response = self.client.table("broker_points").update(update_record).eq(
                                    "id", broker_id).eq("company_id", company_id).execute()
                            else:
                                # Insert se não existe
                                response = self.client.table("broker_points").insert(record).execute()
                            
                            if hasattr(response, "error") and response.error:
                                logger.error(f"Error updating broker points: {response.error}")
                                raise Exception(f"Error updating broker points: {response.error}")
                            
                            all_responses.append(response)
                        except Exception as individual_error:
                            logger.warning(f"Error processing record for broker {broker_id}: {individual_error}")
                            continue

            return all_responses

        except Exception as e:
            logger.error(f"Erro ao fazer upsert em broker_points: {e}")
            raise

    def ensure_webhook_table(self):
        """
        Ensure the from_webhook table exists with proper structure
        This is a safety check - the table should be created in Supabase dashboard
        """
        try:
            # Test if table exists by trying to select from it
            result = self.client.table("from_webhook").select("*").limit(1).execute()
            logger.info("from_webhook table exists and is accessible")
        except Exception as e:
            logger.warning(f"from_webhook table may not exist or is not accessible: {str(e)}")
            logger.info("Please ensure the from_webhook table is created in Supabase with the following structure:")
            logger.info("""
            CREATE TABLE from_webhook (
                id SERIAL PRIMARY KEY,
                webhook_type TEXT,
                payload_id TEXT,
                chat_id TEXT,
                talk_id TEXT,
                contact_id TEXT,
                text TEXT,
                created_at TEXT,
                element_type TEXT,
                entity_type TEXT,
                element_id TEXT,
                entity_id TEXT,
                message_type TEXT,
                author_id TEXT,
                author_type TEXT,
                author_name TEXT,
                author_avatar_url TEXT,
                origin TEXT,
                raw_payload JSONB,
                broker_id TEXT,
                lead_id TEXT,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
            """)

    def link_webhook_message_to_broker(self, webhook_message):
        """
        Vincula uma mensagem de webhook ao broker responsável

        Args:
            webhook_message (dict): Dados da mensagem do webhook

        Returns:
            dict: Dados atualizados com broker_id e lead_id
        """
        try:
            broker_id = None
            lead_id = None

            # 1. Se a mensagem tem author_id e é do tipo "outgoing", é do broker
            if (webhook_message.get('author_id') and 
                webhook_message.get('message_type') == 'outgoing'):

                # Verificar se o author_id é um broker válido
                broker_result = self.client.table("brokers").select("id, nome").eq(
                    "id", webhook_message['author_id']
                ).execute()

                if broker_result.data:
                    broker_id = webhook_message['author_id']
                    logger.info(f"Mensagem vinculada ao broker {broker_id} (mensagem enviada)")

            # 2. Para mensagens recebidas, buscar pelo lead responsável
            elif webhook_message.get('entity_id') and webhook_message.get('entity_type') == 'lead':
                lead_result = self.client.table("leads").select(
                    "id, responsavel_id"
                ).eq("id", webhook_message['entity_id']).execute()

                if lead_result.data:
                    lead_data = lead_result.data[0]
                    lead_id = lead_data['id']
                    broker_id = lead_data['responsavel_id']
                    logger.info(f"Mensagem vinculada ao broker {broker_id} via lead {lead_id}")

            # 3. Se ainda não encontrou, tentar pelo contact_id
            elif webhook_message.get('contact_id'):
                # Buscar leads que tenham esse contact como contato principal
                contact_leads = self.client.table("leads").select(
                    "id, responsavel_id, contato_nome"
                ).ilike("contato_nome", f"%{webhook_message.get('author_name', '')}%").execute()

                if contact_leads.data:
                    # Pegar o lead mais recente deste contato
                    latest_lead = contact_leads.data[0]
                    lead_id = latest_lead['id']
                    broker_id = latest_lead['responsavel_id']
                    logger.info(f"Mensagem vinculada ao broker {broker_id} via contact matching")

            # Atualizar o registro do webhook com os IDs encontrados
            if broker_id or lead_id:
                update_data = {}
                if broker_id:
                    update_data['broker_id'] = broker_id
                if lead_id:
                    update_data['lead_id'] = lead_id

                # Atualizar na base de dados se temos o ID do webhook
                if webhook_message.get('id'):
                    self.client.table("from_webhook").update(update_data).eq(
                        "payload_id", webhook_message.get('payload_id')
                    ).execute()

                webhook_message.update(update_data)
                logger.info(f"Webhook atualizado com broker_id: {broker_id}, lead_id: {lead_id}")

            return webhook_message

        except Exception as e:
            logger.error(f"Erro ao vincular mensagem ao broker: {str(e)}")
            return webhook_message

    def get_broker_messages(self, broker_id, limit=50):
        """
        Busca mensagens de um broker específico

        Args:
            broker_id (str): ID do broker
            limit (int): Limite de mensagens

        Returns:
            list: Lista de mensagens do broker
        """
        try:
            result = self.client.table("from_webhook").select("*").eq(
                "broker_id", broker_id
            ).order("inserted_at", desc=True).limit(limit).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Erro ao buscar mensagens do broker {broker_id}: {str(e)}")
            return []

    def get_lead_messages(self, lead_id, limit=50):
        """
        Busca mensagens de um lead específico

        Args:
            lead_id (str): ID do lead
            limit (int): Limite de mensagens

        Returns:
            list: Lista de mensagens do lead
        """
        try:
            result = self.client.table("from_webhook").select("*").eq(
                "lead_id", lead_id
            ).order("inserted_at", desc=True).limit(limit).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Erro ao buscar mensagens do lead {lead_id}: {str(e)}")
            return []

    def initialize_broker_points(self, company_id=None):
        """
        Cria registros na tabela broker_points para todos os corretores cadastrados,
        com os campos de pontuação zerados. Evita duplicações verificando existência.
        """
        company_id = company_id or self.kommo_config.get('company_id')
        try:
            # Buscar corretores com cargo "Corretor" e company_id específico
            brokers_result = self.client.table("brokers").select(
                "id, nome").eq("cargo", "Corretor").eq("company_id",
                                                       company_id).execute()
            if hasattr(brokers_result, "error") and brokers_result.error:
                raise Exception(
                    f"Erro ao buscar corretores: {brokers_result.error}")

            brokers = brokers_result.data
            if not brokers:
                logger.warning(
                    "Nenhum corretor encontrado para inicializar broker_points."
                )
                return

            # Buscar registros existentes para evitar duplicatas
            existing_result = self.client.table("broker_points").select(
                "id").eq("company_id", company_id).execute()
            
            existing_ids = set()
            if existing_result.data:
                existing_ids = {record['id'] for record in existing_result.data}

            # Filtrar apenas corretores que não têm registros
            brokers_to_insert = [b for b in brokers if b['id'] not in existing_ids]
            
            if not brokers_to_insert:
                logger.info(f"Todos os corretores já têm registros em broker_points para company_id {company_id}")
                return True

            # Criar registros com pontuação zero e company_id
            now = datetime.now().isoformat()
            new_records = [{
                "id": b["id"],
                "company_id": company_id,
                "nome": b["nome"],
                'leads_respondidos_1h': 0,
                'leads_visitados': 0,
                'propostas_enviadas': 0,
                'vendas_realizadas': 0,
                'leads_atualizados_mesmo_dia': 0,
                'feedbacks_positivos': 0,
                'resposta_rapida_3h': 0,
                'todos_leads_respondidos': 0,
                'cadastro_completo': 0,
                'acompanhamento_pos_venda': 0,
                'leads_sem_interacao_24h': 0,
                'leads_ignorados_48h': 0,
                'leads_perdidos': 0,
                'leads_respondidos_apos_18h': 0,
                'leads_tempo_resposta_acima_12h': 0,
                'leads_5_dias_sem_mudanca': 0,
                "pontos": 0,
                "updated_at": now
            } for b in brokers_to_insert]

            # Inserir registros novos
            if new_records:
                result = self.client.table("broker_points").insert(new_records).execute()

                if hasattr(result, "error") and result.error:
                    logger.error(f"Erro ao inserir broker_points: {result.error}")
                    return False

                logger.info(
                    f"Broker points inicializados para {len(new_records)} corretores."
                )
            return True

        except Exception as e:
            logger.error(f"Erro ao inicializar broker_points: {str(e)}")
            # Não fazer raise para não quebrar o fluxo principal
            return False

    def update_broker_points(self,
                             brokers=[],
                             leads=[],
                             activities=[],
                             company_id=None):
        """Update broker points based on current rules and data"""
        try:
            company_id = company_id or self.kommo_config.get('company_id')
            logger.info(f"Starting broker points calculation for company {company_id}")

            # Convert to DataFrames if needed
            if not isinstance(brokers, pd.DataFrame):
                if isinstance(brokers, list) and len(brokers) > 0:
                    brokers = pd.DataFrame(brokers)
                else:
                    logger.warning("No broker data provided")
                    return

            if not isinstance(leads, pd.DataFrame):
                if isinstance(leads, list):
                    leads = pd.DataFrame(leads) if len(leads) > 0 else pd.DataFrame()
                else:
                    leads = pd.DataFrame()

            if not isinstance(activities, pd.DataFrame):
                if isinstance(activities, list):
                    activities = pd.DataFrame(activities) if len(activities) > 0 else pd.DataFrame()
                else:
                    activities = pd.DataFrame()

            # Get date filter from component_filters table
            date_filter_start = None
            date_filter_end = None
            
            try:
                filter_result = self.client.table("component_filters").select("*").eq(
                    "component_name", "ranking_metrics"
                ).eq("company_id", company_id).execute()
                
                if filter_result.data:
                    filter_data = filter_result.data[0]
                    filter_type = filter_data.get('filter_type')
                    
                    # Calculate date ranges based on filter type
                    from datetime import datetime, timedelta
                    import pytz
                    
                    # Use São Paulo timezone for calculations
                    sao_paulo_tz = pytz.timezone('America/Sao_Paulo')
                    now = datetime.now(sao_paulo_tz)
                    
                    if filter_type == 'custom_range':
                        start_date = filter_data.get('start_date')
                        end_date = filter_data.get('end_date')
                        
                        if start_date and end_date:
                            date_filter_start = pd.to_datetime(start_date, utc=True)
                            date_filter_end = pd.to_datetime(end_date, utc=True)
                            logger.info(f"Using custom date range filter: {date_filter_start} to {date_filter_end}")
                        else:
                            logger.info(f"Filter type is custom_range but dates are null, using all data")
                    
                    elif filter_type == 'current_month':
                        # Current month from 1st day to today
                        first_day_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        date_filter_start = pd.to_datetime(first_day_of_month, utc=True)
                        date_filter_end = pd.to_datetime(now, utc=True)
                        logger.info(f"Using current month filter: {date_filter_start} to {date_filter_end}")
                    
                    elif filter_type == 'last_month':
                        # Last month from 1st to last day
                        first_day_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        last_day_last_month = first_day_current_month - timedelta(days=1)
                        first_day_last_month = last_day_last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        
                        date_filter_start = pd.to_datetime(first_day_last_month, utc=True)
                        date_filter_end = pd.to_datetime(last_day_last_month.replace(hour=23, minute=59, second=59), utc=True)
                        logger.info(f"Using last month filter: {date_filter_start} to {date_filter_end}")
                    
                    elif filter_type == 'current_week':
                        # Current week from Monday to today
                        days_since_monday = now.weekday()  # Monday is 0
                        monday_this_week = now - timedelta(days=days_since_monday)
                        monday_this_week = monday_this_week.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        date_filter_start = pd.to_datetime(monday_this_week, utc=True)
                        date_filter_end = pd.to_datetime(now, utc=True)
                        logger.info(f"Using current week filter: {date_filter_start} to {date_filter_end}")
                    
                    else:
                        logger.info(f"Unknown filter type: {filter_type}, using all data")
                        
                else:
                    logger.info("No component_filters found for ranking_metrics, using all data")
            except Exception as filter_error:
                logger.warning(f"Error loading component filters: {filter_error}, using all data")

            # Apply date filter to leads and activities if custom_range is set
            if date_filter_start and date_filter_end:
                # Filter leads by creation date
                if not leads.empty and 'criado_em' in leads.columns:
                    leads['criado_em'] = pd.to_datetime(leads['criado_em'], errors='coerce', utc=True)
                    leads = leads[
                        (leads['criado_em'] >= date_filter_start) & 
                        (leads['criado_em'] <= date_filter_end)
                    ]
                    logger.info(f"Filtered leads to {len(leads)} records within date range")

                # Filter activities by creation date
                if not activities.empty and 'criado_em' in activities.columns:
                    activities['criado_em'] = pd.to_datetime(activities['criado_em'], errors='coerce', utc=True)
                    activities = activities[
                        (activities['criado_em'] >= date_filter_start) & 
                        (activities['criado_em'] <= date_filter_end)
                    ]
                    logger.info(f"Filtered activities to {len(activities)} records within date range")

            # Load current rules
            rules = self.load_rules()
            if not rules:
                logger.warning("No rules found for point calculation")
                return

            # Get existing broker points
            existing_points = self.client.table("broker_points").select("*").eq("company_id", company_id).execute()
            points_dict = {point['id']: point for point in existing_points.data}

            # Calculate points for each broker
            for _, broker in brokers.iterrows():
                broker_id = broker['id']
                broker_name = broker.get('nome', 'Unknown')

                # Initialize points structure
                total_points = 0
                rule_results = {}

                # Get broker's leads and activities
                broker_leads = leads[leads['responsavel_id'] == broker_id] if not leads.empty else pd.DataFrame()
                broker_activities = activities[activities['user_id'] == broker_id] if not activities.empty else pd.DataFrame()

                logger.info(f"Calculating points for broker {broker_name} (ID: {broker_id})")
                logger.info(f"  - {len(broker_leads)} leads")
                logger.info(f"  - {len(broker_activities)} activities")

                # Apply each rule and calculate counts
                for rule_name, rule_config in rules.items():
                    try:
                        # Get the count of occurrences for this rule
                        count = self._calculate_rule_points(
                            rule_name, rule_config, broker_leads, broker_activities, leads, activities, company_id
                        )
                        rule_results[rule_name] = count

                        # Calculate points: count * points_per_occurrence from rules table
                        if isinstance(rule_config, dict):
                            points_per_occurrence = rule_config.get('pontos', 0)
                        else:
                            points_per_occurrence = rule_config

                        rule_points = count * points_per_occurrence
                        total_points += rule_points

                        if count > 0:
                            logger.info(f"  - {rule_name}: {count} occurrences × {points_per_occurrence} = {rule_points} points")

                    except Exception as e:
                        logger.error(f"Error calculating rule {rule_name} for broker {broker_id}: {str(e)}")
                        rule_results[rule_name] = 0

                # Update or insert broker points
                current_time = datetime.now().isoformat()

                broker_points_data = {
                    'id': broker_id,
                    'company_id': company_id,
                    'pontos': total_points,
                    'nome': broker_name,
                    'updated_at': current_time
                }

                # Add individual rule columns with counts (not points)
                for rule_name, count in rule_results.items():
                    broker_points_data[rule_name] = count

                try:
                    # Verificar se o registro existe e buscar dados atuais
                    existing_check = self.client.table("broker_points").select("*").eq(
                        "id", broker_id
                    ).eq("company_id", company_id).execute()
                    
                    if existing_check.data:
                        # Registro existe - comparar valores e atualizar apenas campos alterados
                        existing_data = existing_check.data[0]
                        update_data = {}
                        
                        # Verificar cada campo para mudanças
                        for key, new_value in broker_points_data.items():
                            if key in ['id', 'company_id']:
                                continue  # Não atualizar chaves primárias
                            
                            existing_value = existing_data.get(key)
                            
                            # Comparar valores considerando tipos diferentes
                            if existing_value != new_value:
                                # Verificar se são números equivalentes
                                if isinstance(existing_value, (int, float)) and isinstance(new_value, (int, float)):
                                    if existing_value != new_value:
                                        update_data[key] = new_value
                                else:
                                    update_data[key] = new_value
                        
                        # Só fazer update se houver mudanças
                        if update_data:
                            result = self.client.table("broker_points").update(update_data).eq(
                                "id", broker_id
                            ).eq("company_id", company_id).execute()
                            
                            if hasattr(result, "error") and result.error:
                                logger.error(f"Update error for broker {broker_id}: {result.error}")
                                continue
                            
                            logger.info(f"Updated {len(update_data)} fields for {broker_name}: {total_points} total points")
                        else:
                            logger.info(f"No changes detected for {broker_name} - skipping update")
                    else:
                        # Registro não existe - inserir novo
                        result = self.client.table("broker_points").insert(broker_points_data).execute()
                        
                        if hasattr(result, "error") and result.error:
                            logger.error(f"Insert error for broker {broker_id}: {result.error}")
                            continue
                        
                        logger.info(f"Inserted new record for {broker_name}: {total_points} total points")

                except Exception as db_error:
                    logger.error(f"Database error for broker {broker_id}: {str(db_error)}")
                    continue

            logger.info("Broker points calculation completed successfully")

        except Exception as e:
            logger.error(f"Error updating broker points: {str(e)}")
            # Don't raise to avoid breaking the sync process
            return

    def _calculate_rule_points(self, rule_name, rule_config, broker_leads, broker_activities, all_leads, all_activities, company_id):
        """Calculate count for a specific rule - returns the number of occurrences, not points"""
        try:
            # Ensure datetime columns are properly converted with better error handling
            try:
                if not broker_activities.empty and 'criado_em' in broker_activities.columns:
                    broker_activities = broker_activities.copy()
                    broker_activities.loc[:, 'criado_em'] = pd.to_datetime(broker_activities['criado_em'], errors='coerce', utc=True)
                
                if not broker_leads.empty:
                    broker_leads = broker_leads.copy()
                    if 'criado_em' in broker_leads.columns:
                        broker_leads.loc[:, 'criado_em'] = pd.to_datetime(broker_leads['criado_em'], errors='coerce', utc=True)
                    if 'atualizado_em' in broker_leads.columns:
                        broker_leads.loc[:, 'atualizado_em'] = pd.to_datetime(broker_leads['atualizado_em'], errors='coerce', utc=True)
            except Exception as date_error:
                logger.warning(f"Error converting datetime columns in rule {rule_name}: {date_error}")
                # Continue with original data if conversion fails

            if rule_name == "leads_respondidos_1h":
                # Leads respondidos em 1 hora - calcular baseado nas atividades e leads filtrados
                if broker_activities.empty or broker_leads.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    return 0

                leads_responded_1h = 0
                for _, lead in broker_leads.iterrows():
                    # Buscar primeira mensagem enviada pelo broker para este lead
                    first_response = broker_activities[
                        (broker_activities['lead_id'] == lead['id']) & 
                        (broker_activities.get('tipo', '') == 'mensagem_enviada')
                    ].sort_values('criado_em')
                    
                    if not first_response.empty and 'criado_em' in lead and pd.notna(lead['criado_em']):
                        response_time = (first_response.iloc[0]['criado_em'] - lead['criado_em']).total_seconds()
                        if response_time <= 3600:  # 1 hora = 3600 segundos
                            leads_responded_1h += 1
                            
                return leads_responded_1h

            elif rule_name == "leads_visitados":
                # Leads visitados - usando mudanças de status específicas (já filtradas por data)
                if broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    return 0

                visits = broker_activities[
                    (broker_activities.get('tipo', '') == 'mudança_status') &
                    (broker_activities.get('status_novo', pd.Series()).notna())
                ]
                unique_leads_visited = visits['lead_id'].nunique() if not visits.empty else 0
                return unique_leads_visited

            elif rule_name == "propostas_enviadas":
                # Propostas enviadas - usando mudanças para status específico ou notas (já filtradas por data)
                if broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    return 0

                try:
                    # Buscar por mudanças de status para "Proposta" ou notas contendo "proposta"
                    status_proposals = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status') & 
                        (broker_activities.get('valor_novo', pd.Series()).astype(str).str.contains('proposta', case=False, na=False))
                    ]
                    
                    note_proposals = broker_activities[
                        (broker_activities.get('tipo', '') == 'nota_adicionada') & 
                        (broker_activities.get('texto_mensagem', pd.Series()).astype(str).str.contains('proposta', case=False, na=False))
                    ]
                    
                    proposal_activities = pd.concat([status_proposals, note_proposals], ignore_index=True).drop_duplicates()
                    unique_proposals = proposal_activities['lead_id'].nunique() if not proposal_activities.empty else 0
                    return unique_proposals
                except Exception as e:
                    logger.warning(f"Error in propostas_enviadas calculation: {e}")
                    return 0

            elif rule_name == "vendas_realizadas":
                # Vendas realizadas - buscar atividades de mudança para status "Ganho" no período filtrado
                if broker_activities.empty:
                    # Se não há atividades, usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    # Usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

                try:
                    # Buscar atividades de mudança de status para "Ganho" no período filtrado
                    sales_activities = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status') & 
                        (broker_activities.get('valor_novo', pd.Series()).astype(str).str.contains('ganho|won|vendido', case=False, na=False))
                    ]
                    
                    # Se não encontrar por atividade, usar os leads com status Ganho que foram criados no período
                    if sales_activities.empty and not broker_leads.empty:
                        sales = broker_leads[broker_leads.get('status', '') == 'Ganho']
                        return len(sales)
                    
                    unique_sales = sales_activities['lead_id'].nunique() if not sales_activities.empty else 0
                    return unique_sales
                except Exception as e:
                    logger.warning(f"Error in vendas_realizadas calculation: {e}")
                    # Fallback para leads com status Ganho
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

            elif rule_name == "leads_atualizados_mesmo_dia":
                # Leads atualizados no mesmo dia da criação (já filtrados por data)
                if broker_leads.empty or broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    return 0

                try:
                    same_day_updates = 0
                    for _, lead in broker_leads.iterrows():
                        if pd.notna(lead.get('criado_em')):
                            # Verificar se houve atividade do broker no mesmo dia da criação
                            lead_activities_same_day = broker_activities[
                                (broker_activities['lead_id'] == lead['id']) &
                                (broker_activities['criado_em'].dt.date == lead['criado_em'].date())
                            ]
                            if not lead_activities_same_day.empty:
                                same_day_updates += 1
                                
                    return same_day_updates
                except Exception as e:
                    logger.warning(f"Error in leads_atualizados_mesmo_dia calculation: {e}")
                    return 0

            elif rule_name == "resposta_rapida_3h":
                # Resposta rápida em menos de 3 horas (já filtradas por data)
                if broker_activities.empty or broker_leads.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    return 0

                quick_responses = 0
                for _, lead in broker_leads.iterrows():
                    # Buscar mensagens para este lead
                    lead_messages = broker_activities[
                        (broker_activities['lead_id'] == lead['id']) & 
                        (broker_activities.get('tipo', '').isin(['mensagem_recebida', 'mensagem_enviada']))
                    ].sort_values('criado_em')
                    
                    # Analisar sequências de mensagem recebida seguida de enviada
                    for i in range(len(lead_messages) - 1):
                        current_msg = lead_messages.iloc[i]
                        next_msg = lead_messages.iloc[i + 1]
                        
                        if (current_msg.get('tipo') == 'mensagem_recebida' and 
                            next_msg.get('tipo') == 'mensagem_enviada'):
                            response_time_hours = (next_msg['criado_em'] - current_msg['criado_em']).total_seconds() / 3600
                            if response_time_hours < 3:
                                quick_responses += 1
                                break  # Contar apenas uma vez por lead
                                
                return quick_responses

            elif rule_name == "todos_leads_respondidos":
                # Todos os leads (dentro do período filtrado) foram respondidos
                if broker_leads.empty or broker_activities.empty:
                    return 0

                if len(broker_leads) == 0:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    return 0

                # Verificar se todos os leads no período tiveram resposta
                responded_count = 0
                for _, lead in broker_leads.iterrows():
                    responses = broker_activities[
                        (broker_activities['lead_id'] == lead['id']) & 
                        (broker_activities.get('tipo', '') == 'mensagem_enviada')
                    ]
                    if not responses.empty:
                        responded_count += 1
                
                # Se todos os leads foram respondidos
                if responded_count == len(broker_leads):
                    return 1

                return 0

            elif rule_name == "cadastro_completo":
                # Lead com cadastro completo (já filtrados por data de criação)
                if broker_leads.empty:
                    return 0

                complete_leads = broker_leads[
                    (broker_leads['nome'].notna()) &
                    (broker_leads['contato_nome'].notna()) &
                    (broker_leads['valor'].notna()) &
                    (broker_leads['valor'] > 0)
                ]
                return len(complete_leads)

            elif rule_name == "acompanhamento_pos_venda":
                # Acompanhamento pós-venda - buscar atividades de follow-up após vendas no período
                if broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    return 0

                try:
                    # Buscar atividades de follow-up após mudanças de status para "Ganho"
                    sales_activities = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status') & 
                        (broker_activities.get('valor_novo', pd.Series()).astype(str).str.contains('ganho|won|vendido', case=False, na=False))
                    ]
                    
                    if sales_activities.empty:
                        return 0
                    
                    follow_ups = 0
                    for _, sale_activity in sales_activities.iterrows():
                        # Buscar atividades de follow-up após esta venda
                        post_sale_activities = broker_activities[
                            (broker_activities['lead_id'] == sale_activity['lead_id']) &
                            (broker_activities['criado_em'] > sale_activity['criado_em']) &
                            (broker_activities['tipo'].isin(['mensagem_enviada', 'nota_adicionada', 'tarefa_concluida']))
                        ]
                        if not post_sale_activities.empty:
                            follow_ups += 1
                            
                    return follow_ups
                except Exception as e:
                    logger.warning(f"Error in acompanhamento_pos_venda calculation: {e}")
                    return 0

            elif rule_name == "leads_sem_interacao_24h":
                # Penalização para leads sem interação (baseado nos dados filtrados)
                if broker_leads.empty:
                    return 0

                try:
                    # Se não há atividades, todos os leads não tiveram interação
                    if broker_activities.empty:
                        # Contar apenas leads que não estão fechados
                        inactive_leads = broker_leads[
                            ~broker_leads.get('status', pd.Series()).isin(['Ganho', 'Perdido'])
                        ]
                        return len(inactive_leads)
                    
                    # Verificar se a coluna lead_id existe nas atividades
                    if 'lead_id' not in broker_activities.columns:
                        logger.warning("Column 'lead_id' not found in broker_activities")
                        return len(broker_leads)
                    
                    # Contar leads que não tiveram nenhuma atividade no período
                    inactive_count = 0
                    for _, lead in broker_leads.iterrows():
                        if 'status' in lead and lead['status'] in ['Ganho', 'Perdido']:
                            continue  # Pular leads já fechados
                            
                        lead_activities = broker_activities[
                            broker_activities['lead_id'] == lead['id']
                        ]
                        if lead_activities.empty:
                            inactive_count += 1

                    return inactive_count
                except Exception as e:
                    logger.warning(f"Error in leads_sem_interacao_24h calculation: {e}")
                    return 0

            elif rule_name == "leads_ignorados_48h":
                # Penalização para leads ignorados (baseado nos dados filtrados)
                if broker_leads.empty:
                    return 0

                try:
                    # Se não há atividades, todos os leads foram ignorados
                    if broker_activities.empty:
                        # Contar apenas leads que não estão fechados
                        ignored_leads = broker_leads[
                            ~broker_leads.get('status', pd.Series()).isin(['Ganho', 'Perdido'])
                        ]
                        return len(ignored_leads)
                    
                    # Verificar se a coluna lead_id existe nas atividades
                    if 'lead_id' not in broker_activities.columns:
                        logger.warning("Column 'lead_id' not found in broker_activities")
                        return len(broker_leads)
                    
                    # Verificar leads que nunca tiveram interação no período
                    ignored_count = 0
                    for _, lead in broker_leads.iterrows():
                        if 'status' in lead and lead['status'] in ['Ganho', 'Perdido']:
                            continue  # Pular leads já fechados
                            
                        activities = broker_activities[broker_activities['lead_id'] == lead['id']]
                        if activities.empty:
                            ignored_count += 1

                    return ignored_count
                except Exception as e:
                    logger.warning(f"Error in leads_ignorados_48h calculation: {e}")
                    return 0

            elif rule_name == "leads_perdidos":
                # Penalização para leads perdidos - buscar atividades de mudança para status "Perdido" no período
                if broker_activities.empty:
                    # Se não há atividades, usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        lost_leads = broker_leads[broker_leads['status'] == 'Perdido']
                        return len(lost_leads)
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(f"Column 'lead_id' not found in broker_activities for rule {rule_name}")
                    # Usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        lost_leads = broker_leads[broker_leads['status'] == 'Perdido']
                        return len(lost_leads)
                    return 0

                try:
                    # Buscar atividades de mudança de status para "Perdido" no período filtrado
                    lost_activities = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status') & 
                        (broker_activities.get('valor_novo', pd.Series()).astype(str).str.contains('perdido|lost|fechado|cancelado', case=False, na=False))
                    ]
                    
                    # Se não encontrar por atividade, usar os leads com status Perdido que foram criados no período
                    if lost_activities.empty and not broker_leads.empty:
                        lost_leads = broker_leads[broker_leads.get('status', '') == 'Perdido']
                        return len(lost_leads)
                    
                    unique_lost = lost_activities['lead_id'].nunique() if not lost_activities.empty else 0
                    return unique_lost
                except Exception as e:
                    logger.warning(f"Error in leads_perdidos calculation: {e}")
                    # Fallback para leads com status Perdido
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        lost_leads = broker_leads[broker_leads['status'] == 'Perdido']
                        return len(lost_leads)
                    return 0

            else:
                logger.warning(f"Unknown rule: {rule_name}")
                return 0

        except Exception as e:
            logger.error(f"Error calculating rule {rule_name}: {str(e)}")
            return 0