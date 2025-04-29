import os
from supabase import create_client
import pandas as pd
import numpy as np
import logging
from datetime import datetime

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
            
            # Load Kommo API configuration
            self.kommo_config = self.load_kommo_config()
            # Load gamification rules
            self.rules = self.load_rules()
            
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            raise
            
    def load_kommo_config(self):
        """Load Kommo API configuration from Supabase"""
        try:
            result = self.client.table("kommo_config").select("*").execute()
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")
                
            if not result.data:
                raise ValueError("No Kommo API configuration found")
                
            return result.data[0]
        except Exception as e:
            logger.error(f"Failed to load Kommo config: {str(e)}")
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

            # Upsert data to Supabase
            result = self.client.table("brokers").upsert(
                brokers_data).execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info("Brokers upserted successfully")
            return result

        except Exception as e:
            logger.error(f"Failed to upsert brokers: {str(e)}")
            raise

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

            # Primeiro filtro: apenas pipeline_id = 8865067
            leads_df_clean = leads_df_clean[leads_df_clean['pipeline_id'] ==
                                            8865067]

            if leads_df_clean.empty:
                logger.warning(
                    "Nenhum lead com pipeline_id 8865067 encontrado")
                return

            # Segundo filtro: responsavel_id válido
            leads_df_clean = leads_df_clean[
                leads_df_clean['responsavel_id'].isin(valid_broker_ids)
                | leads_df_clean['responsavel_id'].isna()]

            logger.info(
                f"Total de leads após filtragem: {len(leads_df_clean)}")

            # Convert to dict format
            leads_data = leads_df_clean.to_dict(orient="records")

            for lead in leads_data:
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

            # Limpar registros antigos com pipeline_id diferente
            self.client.table("leads").delete().not_.eq(
                "pipeline_id", 8865067).execute()

            # Upsert to Supabase
            result = self.client.table("leads").upsert(leads_data).execute()

            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            logger.info("Leads upserted successfully")
            return result

        except Exception as e:
            logger.error(f"Failed to upsert leads: {str(e)}")
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

            logger.info(
                f"Upsert de {len(points_df)} registros na tabela broker_points."
            )

            # Faz uma cópia limpa para evitar modificações indesejadas
            df_clean = points_df.copy()

            # Trata valores infinitos ou inválidos
            numeric_cols = df_clean.select_dtypes(
                include=['float', 'int']).columns
            for col in numeric_cols:
                mask = ~np.isfinite(df_clean[col])
                if mask.any():
                    df_clean.loc[mask, col] = None

            # Realiza o upsert na tabela broker_points
            records = df_clean.to_dict("records")
            for record in records:
                for key, value in record.items():
                    if isinstance(value, pd.Timestamp):
                        record[key] = value.isoformat()

            # Realiza o upsert com os dados tratados
            response = self.client.table("broker_points").upsert(
                records).execute()

            return response

        except Exception as e:
            logger.error(f"Erro ao fazer upsert em broker_points: {e}")
            raise

    def initialize_broker_points(self):
        """
        Cria registros na tabela broker_points para todos os corretores cadastrados,
        com os campos de pontuação zerados. Evita duplicações.
        """
        try:
            # Buscar corretores com cargo "Corretor"
            brokers_result = self.client.table("brokers").select(
                "id, nome").eq("cargo", "Corretor").execute()
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

    def update_broker_points(self, brokers=None, leads=None, activities=None):
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

            # Filtra apenas corretores ativos
            active_brokers = brokers[brokers['cargo'] == 'Corretor']
            if active_brokers.empty:
                logger.warning("Nenhum corretor ativo encontrado.")
                return

            # Calcula os pontos
            points_df = calculate_broker_points(active_brokers, leads,
                                                activities)

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

            # Atualiza o banco com retry em caso de erro
            for attempt in range(max_retries):
                try:
                    self.upsert_broker_points(points_df)
                    logger.info(
                        f"Tabela broker_points atualizada com sucesso. {len(points_df)} registros atualizados."
                    )
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
