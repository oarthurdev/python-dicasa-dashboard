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
                                 access_token=config['access_token'],
                                 supabase_client=self)
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

                # Setup default rules for new company
                self.setup_company_rules(company_id)

                # Trigger sync through FastAPI endpoint
                try:
                    response = requests.post("http://0.0.0.0:5002/start")
                    if response.status_code == 200:
                        logger.info("Sync started for all companies")

                        while True:
                            try:
                                status_response = requests.get(
                                    "http://0.0.0.0:5002/status")
                                if status_response.status_code == 200:
                                    all_status = status_response.json()
                                    company_status = all_status.get(
                                        str(company_id))

                                    if not company_status:
                                        logger.error(
                                            f"No status found for company {company_id}"
                                        )
                                        break

                                    status = company_status.get('status')

                                    if status in ('initializing', 'running'):
                                        logger.info(
                                            f"Company {company_id} sync in progress: {status}"
                                        )
                                        time.sleep(
                                            30)  # Check every 30 seconds
                                        continue
                                    else:
                                        logger.info(
                                            f"Sync completed for company {company_id} with status: {status}"
                                        )
                                        break
                                else:
                                    logger.error(
                                        f"Failed to get sync status. HTTP {status_response.status_code}"
                                    )
                                    break
                            except Exception as e:
                                logger.error(
                                    f"Exception while checking sync status: {e}"
                                )
                                break
                    else:
                        logger.error(
                            f"Failed to start sync for company {company_id}")
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
                                 access_token=config['access_token'],
                                 supabase_client=self)
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

    def load_rules(self, company_id=None):
        """Load gamification rules from Supabase for specific company"""
        try:
            company_id = company_id or self.kommo_config.get('company_id')
            if not company_id:
                logger.warning("No company_id provided for loading rules")
                return {}

            # First try to load company-specific rules from company_rules table
            company_rules_result = self.client.table("company_rules").select(
                """
                rules!inner(coluna_nome, pontos),
                pontos,
                active
            """).eq("company_id", company_id).eq("active", True).execute()

            rules_dict = {}

            if company_rules_result.data:
                # Use company-specific rule points
                for rule in company_rules_result.data:
                    rules_dict[rule['rules']['coluna_nome']] = rule['pontos']
                logger.info(f"Loaded {len(rules_dict)} company-specific rules")
            else:
                # Fallback to default rules
                result = self.client.table("rules").select("*").eq(
                    "company_id", company_id).execute()
                if result.data:
                    for rule in result.data:
                        rules_dict[rule['coluna_nome']] = rule['pontos']
                    logger.info(f"Loaded {len(rules_dict)} default rules")
                else:
                    logger.warning("No rules found for company")

            # Also load custom rules
            custom_rules_result = self.client.table("custom_rules").select(
                "*").eq("company_id", company_id).eq("active", True).execute()

            if custom_rules_result.data:
                for rule in custom_rules_result.data:
                    rules_dict[rule['coluna_nome']] = rule['pontos']
                logger.info(
                    f"Added {len(custom_rules_result.data)} custom rules")

            return rules_dict
        except Exception as e:
            logger.error(f"Failed to load rules: {str(e)}")
            return {}

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
            brokers_df_filtered = brokers_df[brokers_df['cargo'] ==
                                             'Corretor'].copy()

            if brokers_df_filtered.empty:
                logger.warning("No brokers with 'Corretor' role found")
                return

            # Convert DataFrame to list of dicts
            brokers_data = brokers_df_filtered.to_dict(orient="records")

            # Add updated_at timestamp
            for broker in brokers_data:
                broker["updated_at"] = datetime.now().isoformat()

            # Upsert data to Supabase - inserir novos e atualizar existentes
            result = self.client.table("brokers").upsert(
                brokers_data, on_conflict='id').execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info(
                f"Brokers upserted successfully: {len(brokers_data)} records processed"
            )
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

            logger.info(
                f"Activities upserted successfully: {len(activities_data)} records processed"
            )
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
                            existing = self.client.table(
                                "broker_points").select("id").eq(
                                    "id",
                                    broker_id).eq("company_id",
                                                  company_id).execute()

                            if existing.data:
                                # Update se existe - remove campos que não devem ser atualizados na condição
                                update_record = {
                                    k: v
                                    for k, v in record.items()
                                    if k not in ['id', 'company_id']
                                }
                                response = self.client.table(
                                    "broker_points").update(update_record).eq(
                                        "id",
                                        broker_id).eq("company_id",
                                                      company_id).execute()
                            else:
                                # Insert se não existe
                                response = self.client.table(
                                    "broker_points").insert(record).execute()

                            if hasattr(response, "error") and response.error:
                                logger.error(
                                    f"Error updating broker points: {response.error}"
                                )
                                raise Exception(
                                    f"Error updating broker points: {response.error}"
                                )

                            all_responses.append(response)
                        except Exception as individual_error:
                            logger.warning(
                                f"Error processing record for broker {broker_id}: {individual_error}"
                            )
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
            result = self.client.table("from_webhook").select("*").limit(
                1).execute()
            logger.info("from_webhook table exists and is accessible")
        except Exception as e:
            logger.warning(
                f"from_webhook table may not exist or is not accessible: {str(e)}"
            )
            logger.info(
                "Please ensure the from_webhook table is created in Supabase with the following structure:"
            )
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
            if (webhook_message.get('author_id')
                    and webhook_message.get('message_type') == 'outgoing'):

                # Verificar se o author_id é um broker válido
                broker_result = self.client.table("brokers").select(
                    "id, nome").eq("id",
                                   webhook_message['author_id']).execute()

                if broker_result.data:
                    broker_id = webhook_message['author_id']
                    logger.info(
                        f"Mensagem vinculada ao broker {broker_id} (mensagem enviada)"
                    )

            # 2. Para mensagens recebidas, buscar pelo lead responsável
            elif webhook_message.get('entity_id') and webhook_message.get(
                    'entity_type') == 'lead':
                lead_result = self.client.table(
                    "leads").select("id, responsavel_id").eq(
                        "id", webhook_message['entity_id']).execute()

                if lead_result.data:
                    lead_data = lead_result.data[0]
                    lead_id = lead_data['id']
                    broker_id = lead_data['responsavel_id']
                    logger.info(
                        f"Mensagem vinculada ao broker {broker_id} via lead {lead_id}"
                    )

            # 3. Se ainda não encontrou, tentar pelo contact_id
            elif webhook_message.get('contact_id'):
                # Buscar leads que tenham esse contact como contato principal
                contact_leads = self.client.table("leads").select(
                    "id, responsavel_id, contato_nome").ilike(
                        "contato_nome",
                        f"%{webhook_message.get('author_name', '')}%").execute(
                        )

                if contact_leads.data:
                    # Pegar o lead mais recente deste contato
                    latest_lead = contact_leads.data[0]
                    lead_id = latest_lead['id']
                    broker_id = latest_lead['responsavel_id']
                    logger.info(
                        f"Mensagem vinculada ao broker {broker_id} via contact matching"
                    )

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
                        "payload_id",
                        webhook_message.get('payload_id')).execute()

                webhook_message.update(update_data)
                logger.info(
                    f"Webhook atualizado com broker_id: {broker_id}, lead_id: {lead_id}"
                )

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
                "broker_id",
                broker_id).order("inserted_at",
                                 desc=True).limit(limit).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(
                f"Erro ao buscar mensagens do broker {broker_id}: {str(e)}")
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
                "lead_id", lead_id).order("inserted_at",
                                          desc=True).limit(limit).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(
                f"Erro ao buscar mensagens do lead {lead_id}: {str(e)}")
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
                existing_ids = {
                    record['id']
                    for record in existing_result.data
                }

            # Filtrar apenas corretores que não têm registros
            brokers_to_insert = [
                b for b in brokers if b['id'] not in existing_ids
            ]

            if not brokers_to_insert:
                logger.info(
                    f"Todos os corretores já têm registros em broker_points para company_id {company_id}"
                )
                return True

            # Criar registros com pontuação zero e company_id (apenas campos do novo schema)
            now = datetime.now().isoformat()
            new_records = [{
                "id": b["id"],
                "company_id": company_id,
                "nome": b["nome"],
                "leads_visitados": 0,
                "propostas_enviadas": 0,
                "vendas_realizadas": 0,
                "leads_perdidos": 0,
                "pontos": 0,
                "updated_at": now
            } for b in brokers_to_insert]

            # Inserir registros novos
            if new_records:
                result = self.client.table("broker_points").insert(
                    new_records).execute()

                if hasattr(result, "error") and result.error:
                    logger.error(
                        f"Erro ao inserir broker_points: {result.error}")
                    return False

                logger.info(
                    f"Broker points inicializados para {len(new_records)} corretores."
                )
            return True

        except Exception as e:
            logger.error(f"Erro ao inicializar broker_points: {str(e)}")
            # Não fazer raise para não quebrar o fluxo principal
            return False

    def setup_company_rules(self, company_id, default_rules=None):
        """
        Setup default rules for a company if they don't exist
        """
        try:
            if not default_rules:
                default_rules = {
                    'leads_visitados': 40,
                    'propostas_enviadas': 8,
                    'vendas_realizadas': 100,
                    'leads_perdidos': -10
                }

            # Check if company already has rules
            existing_rules = self.client.table("rules").select("*").eq(
                "company_id", company_id).execute()

            if not existing_rules.data:
                # Create default rules for company
                rules_to_insert = []
                for rule_name, points in default_rules.items():
                    rules_to_insert.append({
                        'nome':
                        rule_name.replace('_', ' ').title(),
                        'coluna_nome':
                        rule_name,
                        'pontos':
                        points,
                        'company_id':
                        company_id,
                        'descricao':
                        f'Regra para {rule_name.replace("_", " ")}'
                    })

                result = self.client.table("rules").insert(
                    rules_to_insert).execute()
                if hasattr(result, "error") and result.error:
                    raise Exception(
                        f"Error creating default rules: {result.error}")

                logger.info(
                    f"Created {len(rules_to_insert)} default rules for company {company_id}"
                )

            return True
        except Exception as e:
            logger.error(f"Error setting up company rules: {str(e)}")
            return False

    def get_sync_status(self, company_id):
        """
        Get sync status from sync_control table
        """
        try:
            result = self.client.table("sync_control").select("*").eq(
                "company_id", company_id).execute()
            if result.data:
                return result.data[0]
            return None
        except Exception as e:
            logger.error(f"Error getting sync status: {str(e)}")
            return None

    def update_sync_status(self, company_id, status, error=None):
        """
        Update sync status in sync_control table
        """
        try:
            update_data = {
                'company_id': company_id,
                'status': status,
                'last_sync': datetime.now().isoformat()
            }

            if error:
                update_data['error'] = error

            result = self.client.table("sync_control").upsert(
                update_data, on_conflict='company_id').execute()
            if hasattr(result, "error") and result.error:
                raise Exception(f"Error updating sync status: {result.error}")

            return True
        except Exception as e:
            logger.error(f"Error updating sync status: {str(e)}")
            return False

    def _calculate_rule_points(self, rule_name, rule_config, broker_leads,
                               broker_activities, all_leads, all_activities,
                               company_id):
        """Calculate count for a specific rule - returns the number of occurrences, not points"""
        try:
            # Ensure datetime columns are properly converted with better error handling
            try:
                if not broker_activities.empty and 'criado_em' in broker_activities.columns:
                    broker_activities = broker_activities.copy()
                    broker_activities.loc[:, 'criado_em'] = pd.to_datetime(
                        broker_activities['criado_em'],
                        errors='coerce',
                        utc=True)

                if not broker_leads.empty:
                    broker_leads = broker_leads.copy()
                    if 'criado_em' in broker_leads.columns:
                        broker_leads.loc[:, 'criado_em'] = pd.to_datetime(
                            broker_leads['criado_em'],
                            errors='coerce',
                            utc=True)
                    if 'atualizado_em' in broker_leads.columns:
                        broker_leads.loc[:, 'atualizado_em'] = pd.to_datetime(
                            broker_leads['atualizado_em'],
                            errors='coerce',
                            utc=True)
            except Exception as date_error:
                logger.warning(
                    f"Error converting datetime columns in rule {rule_name}: {date_error}"
                )
                # Continue with original data if conversion fails

            if rule_name == "leads_visitados":
                # Leads visitados - usando mudanças de status específicas (já filtradas por data)
                if broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    return 0

                visits = broker_activities[
                    (broker_activities.get('tipo', '') == 'mudança_status')
                    & (broker_activities.get('status_novo',
                                             pd.Series()).notna())]
                unique_leads_visited = visits['lead_id'].nunique(
                ) if not visits.empty else 0
                return unique_leads_visited

            elif rule_name == "propostas_enviadas":
                # Propostas enviadas - usando mudanças para status específico ou notas (já filtradas por data)
                if broker_activities.empty:
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    return 0

                try:
                    # Buscar por mudanças de status para "Proposta" ou notas contendo "proposta"
                    status_proposals = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status')
                        & (broker_activities.get('valor_novo', pd.Series()).
                           astype(str).str.contains(
                               'proposta', case=False, na=False))]

                    note_proposals = broker_activities[
                        (broker_activities.get('tipo', '') == 'nota_adicionada'
                         ) & (broker_activities.get('texto_mensagem',
                                                    pd.Series()).astype(str).
                              str.contains('proposta', case=False, na=False))]

                    proposal_activities = pd.concat(
                        [status_proposals, note_proposals],
                        ignore_index=True).drop_duplicates()
                    unique_proposals = proposal_activities['lead_id'].nunique(
                    ) if not proposal_activities.empty else 0
                    return unique_proposals
                except Exception as e:
                    logger.warning(
                        f"Error in propostas_enviadas calculation: {e}")
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
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    # Usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

                try:
                    # Buscar atividades de mudança de status para "Ganho" no período filtrado
                    sales_activities = broker_activities[
                        (broker_activities.get('tipo', '') == 'mudança_status')
                        & (broker_activities.get('valor_novo', pd.Series()).
                           astype(str).str.contains(
                               'ganho|won|vendido', case=False, na=False))]

                    # Se não encontrar por atividade, usar os leads com status Ganho que foram criados no período
                    if sales_activities.empty and not broker_leads.empty:
                        sales = broker_leads[broker_leads.get('status', '') ==
                                             'Ganho']
                        return len(sales)

                    unique_sales = sales_activities['lead_id'].nunique(
                    ) if not sales_activities.empty else 0
                    return unique_sales
                except Exception as e:
                    logger.warning(
                        f"Error in vendas_realizadas calculation: {e}")
                    # Fallback para leads com status Ganho
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        sales = broker_leads[broker_leads['status'] == 'Ganho']
                        return len(sales)
                    return 0

            # Remove legacy rules that don't exist in new schema
            elif rule_name in [
                    "leads_atualizados_mesmo_dia", "resposta_rapida_3h",
                    "todos_leads_respondidos", "cadastro_completo",
                    "acompanhamento_pos_venda", "leads_sem_interacao_24h",
                    "leads_ignorados_48h", "leads_respondidos_1h",
                    "feedbacks_positivos", "leads_respondidos_apos_18h",
                    "leads_tempo_resposta_acima_12h",
                    "leads_5_dias_sem_mudanca"
            ]:
                logger.info(
                    f"Skipping legacy rule {rule_name} - not in new schema")
                return 0

            elif rule_name == "leads_perdidos":
                # Penalização para leads perdidos - buscar atividades de mudança para status "Perdido" no período
                if broker_activities.empty:
                    # Se não há atividades, usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        lost_leads = broker_leads[broker_leads['status'] ==
                                                  'Perdido']
                        return len(lost_leads)
                    return 0

                # Verificar se a coluna lead_id existe nas atividades
                if 'lead_id' not in broker_activities.columns:
                    logger.warning(
                        f"Column 'lead_id' not found in broker_activities for rule {rule_name}"
                    )
                    # Usar fallback dos leads
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        lost_leads = broker_leads[broker_leads['status'] ==
                                                  'Perdido']
                        return len(lost_leads)
                    return 0

                try:
                    # Buscar atividades de mudança de status para "Perdido" no período filtrado
                    lost_activities = broker_activities[(
                        broker_activities.get('tipo', '') == 'mudança_status'
                    ) & (broker_activities.get('valor_novo', pd.Series(
                    )).astype(str).str.contains(
                        'perdido|lost|fechado|cancelado', case=False, na=False)
                         )]

                    # Se não encontrar por atividade, usar os leads com status Perdido que foram criados no período
                    if lost_activities.empty and not broker_leads.empty:
                        lost_leads = broker_leads[broker_leads.get(
                            'status', '') == 'Perdido']
                        return len(lost_leads)

                    unique_lost = lost_activities['lead_id'].nunique(
                    ) if not lost_activities.empty else 0
                    return unique_lost
                except Exception as e:
                    logger.warning(f"Error in leads_perdidos calculation: {e}")
                    # Fallback para leads com status Perdido
                    if not broker_leads.empty and 'status' in broker_leads.columns:
                        lost_leads = broker_leads[broker_leads['status'] ==
                                                  'Perdido']
                        return len(lost_leads)
                    return 0

            else:
                logger.warning(f"Unknown rule: {rule_name}")
                return 0

        except Exception as e:
            logger.error(f"Error calculating rule {rule_name}: {str(e)}")
            return 0
