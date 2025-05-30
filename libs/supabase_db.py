import os
from libs.kommo_api import KommoAPI
from libs.sync_manager import SyncManager
from supabase import create_client
import pandas as pd
import numpy as np
import logging
from datetime import datetime
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

            # Upsert data to Supabase
            result = self.client.table("brokers").upsert(brokers_data).execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info("Brokers upserted successfully")
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

            # Upsert data to Supabase
            result = self.client.table("activities").upsert(
                activities_data).execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info("Activities upserted successfully")
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

                # Realiza o upsert com os dados tratados
                response = self.client.table("broker_points").upsert(
                    records).execute()
                all_responses.append(response)

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
        com os campos de pontuação zerados. Evita duplicações.
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

            broker_ids = [b["id"] for b in brokers]

            # Buscar IDs já existentes na tabela broker_points
            existing_result = self.client.table("broker_points").select(
                "id").in_("id", broker_ids).execute()
            existing_ids = {r["id"]
                            for r in existing_result.data
                            } if existing_result.data else set()

            # Filtrar somente os que ainda não existem
            new_brokers = [b for b in brokers if b["id"] not in existing_ids]

            if not new_brokers:
                logger.info(
                    "Todos os corretores já possuem entrada em broker_points.")
                return

            # Criar registros com pontuação zero
            now = datetime.now().isoformat()
            new_records = [{
                "id": b["id"],
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
            } for b in new_brokers]

            # Inserir no banco usando upsert para evitar duplicatas
            if new_records:
                result = self.client.table("broker_points").upsert(
                    new_records, on_conflict="id").execute()

                if hasattr(result, "error") and result.error:
                    raise Exception(
                        f"Erro ao inserir broker_points: {result.error}")

                logger.info(
                    f"{len(new_records)} registros criados em broker_points com sucesso."
                )
            return result

        except Exception as e:
            logger.error(f"Erro ao inicializar broker_points: {str(e)}")
            raise

    def update_broker_points(self,
                             brokers=[],
                             leads=[],
                             activities=[],
                             company_id=None):
        """
        Atualiza a tabela broker_points no Supabase com base nas regras de gamificação.
        Aceita dados já obtidos para evitar chamadas API desnecessárias.
        """
        import logging
        from gamification import calculate_broker_points
        import time

        logger = logging.getLogger(__name__)
        max_retries = 3
        retry_delay = 5  # segundos

        try:
            logger.info("Iniciando atualização dos pontos dos corretores...")

            # Garante que temos um company_id
            company_id = company_id or self.kommo_config.get('company_id')
            if not company_id:
                logger.error(
                    "company_id é necessário para atualizar broker_points")
                return

            # Primeiro, garantir que os brokers existam no banco e filtrar por company_id
            if brokers is not None and not brokers.empty:
                brokers = brokers[brokers['company_id'] == company_id].copy()
                if brokers.empty:
                    logger.warning(
                        f"Nenhum corretor encontrado para company_id {company_id}"
                    )
                    return
                self.upsert_brokers(brokers)
                time.sleep(1)  # Aguarda a conclusão do upsert de brokers

            # Se não recebeu dados em cache, busca da API
            if brokers is None or leads is None or activities is None:
                from .kommo_api import KommoAPI
                kommo_api = KommoAPI()

                # Tenta buscar os dados com retry em caso de erro
                max_retries = 3
                retry_delay = 5  # segundos

                for attempt in range(max_retries):
                    try:
                        brokers = kommo_api.get_users()
                        leads = kommo_api.get_leads()
                        activities = kommo_api.get_activities()
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        logger.warning(
                            f"Tentativa {attempt + 1} falhou: {str(e)}. Tentando novamente em {retry_delay} segundos..."
                        )
                        time.sleep(retry_delay)

            if brokers.empty or leads.empty or activities.empty:
                logger.warning(
                    "Dados insuficientes para cálculo de pontos. Verifique se todas as tabelas estão preenchidas."
                )
                return

            # Filtra apenas corretores ativos da empresa específica
            active_brokers = brokers[(brokers['cargo'] == 'Corretor')
                                     & (brokers['company_id'] == company_id)]
            if active_brokers.empty:
                logger.warning("Nenhum corretor ativo encontrado.")
                return

            # Carrega as regras e calcula os pontos
            rules = self.load_rules()
            self.insert_log("INFO", "Iniciando cálculo de pontos")
            points_df = calculate_broker_points(active_brokers,
                                                leads,
                                                activities,
                                                rules,
                                                company_id=company_id)
            self.insert_log("INFO", "Cálculo de pontos concluído")

            # Garante que todos os campos necessários existam
            required_fields = [
                'leads_respondidos_1h', 'leads_visitados',
                'propostas_enviadas', 'vendas_realizadas',
                'leads_atualizados_mesmo_dia', 'feedbacks_positivos',
                'resposta_rapida_3h', 'todos_leads_respondidos',
                'cadastro_completo', 'acompanhamento_pos_venda',
                'leads_sem_interacao_24h', 'leads_ignorados_48h',
                'leads_perdidos', 'leads_respondidos_apos_18h',
                'leads_tempo_resposta_acima_12h', 'leads_5_dias_sem_mudanca'
            ]

            for field in required_fields:
                if field not in points_df.columns:
                    points_df[field] = 0

            # Adiciona timestamp de atualização
            points_df['updated_at'] = pd.Timestamp.now()

            # Registra os pontos atualizados de cada corretor
            for _, row in points_df.iterrows():
                self.insert_log(
                    "INFO", f"Pontos atualizados - Corretor: {row['nome']} | "
                    f"Total: {row['pontos']} | "
                    f"Leads 1h: {row['leads_respondidos_1h']} | "
                    f"Leads visitados: {row['leads_visitados']} | "
                    f"Propostas: {row['propostas_enviadas']} | "
                    f"Vendas: {row['vendas_realizadas']}")

            # Atualiza o banco com retry em caso de erro
            for attempt in range(max_retries):
                try:
                    self.upsert_broker_points(points_df)
                    msg = f"Tabela broker_points atualizada com sucesso. {len(points_df)} registros atualizados."
                    logger.info(msg)
                    self.insert_log("INFO", msg)
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(
                        f"Tentativa de upsert {attempt + 1} falhou: {str(e)}. Tentando novamente em {retry_delay} segundos..."
                    )
                    time.sleep(retry_delay)

        except Exception as e:
            logger.error(f"Erro ao atualizar pontos dos corretores: {e}")
            raise

    def calculate_ticket_medio(self, leads_df):
        """
        Calcula o ticket médio dos leads ganhos e atualiza apenas a coluna ticket_medio
        dos brokers com base no responsavel_id.
        """
        try:
            config = self.kommo_config
            if not config:
                logger.error("Kommo configuration not found")
                return None

            kommo_api = KommoAPI(api_url=config['api_url'],
                                access_token=config['access_token'])

            # Obter os brokers (usuários)
            brokers_df = kommo_api.get_users()

            brokers_df['cargo'] = 'Corretor'
            
            if brokers_df.empty:
                logger.warning("Nenhum usuário retornado pela API Kommo.")
                return None

            # Filtrar leads ganhos (status_id = 142)
            leads_ganhos = leads_df[leads_df['status_id'] == 142].copy()

            # Limpar e converter 'valor'
            leads_ganhos = leads_ganhos.dropna(subset=['valor'])
            leads_ganhos['valor'] = leads_ganhos['valor'].astype(float)

            # Calcular ticket médio por responsavel_id
            ticket_medio_por_responsavel = (
                leads_ganhos
                .groupby('responsavel_id')['valor']
                .mean()
                .round(2)
            )

            brokers_df['ticket_medio'] = brokers_df['id'].map(ticket_medio_por_responsavel).fillna(0.0)

            self.upsert_brokers(brokers_df)

            logger.info(
                f"Ticket médio atualizado para {len(brokers_df)} brokers."
            )

        except Exception as e:
            logger.error(f"Erro ao calcular ticket médio e atualizar brokers: {str(e)}")
            return None

    def calculate_response_time(self, lead_id, created_at):
        """
        Calcula o tempo médio de resposta para um lead.
        """
        try:
            # Buscar as interações (notes) com o endpoint
            notes = self.get_lead_notes(lead_id)

            if notes:
                logger.info(
                    f"Encontradas {len(notes)} notas para o lead {lead_id}")
                # Encontrar a primeira nota criada por um usuário do time (excluindo notas automáticas, se possível).
                first_note = next(
                    (note for note in notes if note.get('created_by') and note.get('created_by') > 0),
                    None
                )

                if first_note:
                    logger.info(f"Primeira nota encontrada: {first_note}")
                    first_note_created_at = datetime.fromtimestamp(
                        first_note.get('created_at'))

                    # Calcular o tempo entre created_at da lead e o created_at da primeira nota do usuário (em segundos ou minutos).
                    response_time = (first_note_created_at -
                                     created_at).total_seconds()
                    return response_time

            return None

        except Exception as e:
            logger.error(
                f"Erro ao calcular o tempo médio de resposta para o lead {lead_id}: {str(e)}"
            )
            return None

    def get_lead_notes(self, lead_id):
        """
        Busca as notas de um lead na API do Kommo.
        """
        try:
            api_url = self.kommo_config['api_url']
            access_token = self.kommo_config['access_token']

            url = f"{api_url}/leads/{lead_id}/notes"
            headers = {"Authorization": f"Bearer {access_token}"}

            response = requests.get(url, headers=headers)
            response.raise_for_status()

            # Check if response has content before parsing JSON
            if response.content:
                return response.json().get('_embedded', {}).get('notes', [])
            return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao buscar notas do lead {lead_id}: {str(e)}")
            return None

    def upsert_leads(self, leads_df):
        """
        Insert or update lead data in the Supabase database

        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead data
        """
        try:
            if leads_df.empty:
                logger.warning("No lead data to insert")
                return

            logger.info(f"Upserting {len(leads_df)} leads to Supabase")

            leads_df_clean = leads_df.copy()

            # Replace infinite values with None
            numeric_cols = leads_df_clean.select_dtypes(
                include=['float', 'int']).columns
            for col in numeric_cols:
                mask = ~np.isfinite(leads_df_clean[col])
                if mask.any():
                    leads_df_clean.loc[mask, col] = None

            # Convert bigint-compatible columns
            bigint_columns = [
                'id', 'responsavel_id', 'status_id', 'pipeline_id'
            ]
            for col in bigint_columns:
                if col in leads_df_clean.columns:
                    mask = np.isfinite(leads_df_clean[col])
                    if mask.any():
                        leads_df_clean.loc[mask, col] = leads_df_clean.loc[
                            mask, col].astype('Int64')

            # Get valid broker IDs from database
            brokers_result = self.client.table("brokers").select(
                "id").execute()
            if hasattr(brokers_result, "error") and brokers_result.error:
                raise Exception(
                    f"Supabase error querying brokers: {brokers_result.error}")

            valid_broker_ids = {broker['id'] for broker in brokers_result.data}

            # No pipeline filtering
            if leads_df_clean.empty:
                logger.warning("No leads found")
                return

            # Segundo filtro: responsavel_id válido
            leads_df_clean = leads_df_clean[
                leads_df_clean['responsavel_id'].isin(valid_broker_ids)
                | leads_df_clean['responsavel_id'].isna()]

            logger.info(
                f"Total de leads após filtragem: {len(leads_df_clean)}")

            # Convert to dict format
            leads_data = leads_df_clean.to_dict(orient="records")

            # Calculate ticket médio first
            self.calculate_ticket_medio(leads_df_clean)

            processed_leads = []
            for lead in leads_data:
                responsavel_id = lead.get("responsible_user_id")

                # Calculate tempo médio for this lead
                created_at = datetime.fromtimestamp(lead.get(
                    "created_at", 0)) if lead.get("created_at") else None
                tempo_medio = None
                if created_at:
                    tempo_medio = self.calculate_response_time(
                        lead.get("id"), created_at)

                lead["updated_at"] = datetime.now().isoformat()

                if "criado_em" in lead and lead["criado_em"] is not None:
                    lead["criado_em"] = lead["criado_em"].isoformat()
                if "atualizado_em" in lead and lead[
                        "atualizado_em"] is not None:
                    lead["atualizado_em"] = lead["atualizado_em"].isoformat()

                for key, value in list(lead.items()):
                    if isinstance(value, float) and (np.isnan(value)
                                                     or np.isinf(value)):
                        lead[key] = None
                    elif key in bigint_columns and isinstance(
                            value, float) and value.is_integer():
                        lead[key] = int(value)

                lead["status"] = ("Ganho" if lead.get("status_id") == 142 else
                                  "Perdido" if lead.get("status_id") == 143
                                  else "Em progresso")
                lead["tempo_medio"] = tempo_medio

                processed_leads.append(lead)

            # No pipeline-based deletion

            # Upsert to Supabase
            result = self.client.table("leads").upsert(
                processed_leads).execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info("Leads upserted successfully")
            return result

        except Exception as e:
            logger.error(f"Failed to upsert leads: {str(e)}")
            raise
