import logging
import hashlib
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd

logger = logging.getLogger(__name__)


class SyncManager:

    def __init__(self,
                 kommo_api,
                 supabase_client,
                 company_config,
                 batch_size=250):
        self.kommo_api = kommo_api
        self.supabase = supabase_client
        self.batch_size = batch_size
        self.cache = {'brokers': {}, 'leads': {}, 'activities': {}}
        self.config = company_config

    def _generate_hash(self, data: Dict) -> str:
        """Generate a hash for data comparison"""
        return hashlib.md5(json.dumps(data,
                                      sort_keys=True).encode()).hexdigest()

    def _get_existing_records(self, table: str) -> Dict:
        """Get existing records from database with their hashes"""
        try:
            result = self.supabase.client.table(table).select("*").execute()
            if hasattr(result, "error") and result.error:
                raise Exception(f"Supabase error: {result.error}")

            existing = {}
            for record in result.data:
                record_id = record['id']
                existing[record_id] = {
                    'hash': self._generate_hash(record),
                    'data': record
                }
            return existing
        except Exception as e:
            logger.error(
                f"Error fetching existing records for {table}: {str(e)}")
            raise

    def _prepare_record(self, record: Dict) -> Dict:
        """Prepare record for database insertion/update"""
        processed = record.copy()

        # Convert datetime objects to ISO format
        for key, value in processed.items():
            if hasattr(value, 'isoformat'):
                processed[key] = value.isoformat()
            elif pd.isna(value):
                processed[key] = None
            elif key in ['lead_id', 'user_id'] and isinstance(
                    value, (int, float)):
                processed[key] = int(value) if pd.notna(value) else None

        return processed

    def _process_batch(self, records: List[Dict], table: str,
                       existing_records: Dict) -> None:
        """Process a batch of records"""
        try:
            to_upsert = []

            for record in records:
                processed = self._prepare_record(record)
                record_id = processed.get('id')
                new_hash = self._generate_hash(processed)

                # Skip if record hasn't changed
                if record_id in existing_records and existing_records[
                        record_id]['hash'] == new_hash:
                    continue

                processed['updated_at'] = datetime.now().isoformat()
                to_upsert.append(processed)

            if to_upsert:
                result = self.supabase.client.table(table).upsert(
                    to_upsert).execute()
                if hasattr(result, "error") and result.error:
                    raise Exception(f"Supabase error: {result.error}")

                logger.info(f"Processed {len(to_upsert)} records for {table}")

        except Exception as e:
            logger.error(f"Error processing batch for {table}: {str(e)}")
            raise

    def get_safe_batch_size(self, data_type):
        """Get safe batch size based on data type to prevent overload"""
        batch_sizes = {
            'brokers': 100,    # Brokers são poucos, batch maior
            'leads': 250,      # Leads podem ser muitos, batch médio
            'activities': 200  # Activities são muitas, batch controlado
        }
        return batch_sizes.get(data_type, 200)

    def create_data_snapshot(self, company_id, snapshot_type="manual"):
        """Create a snapshot of current data for archival purposes"""
        try:
            # Get current data for snapshot
            points_result = self.supabase.client.table("broker_points").select(
                "*").eq("company_id", company_id).execute()
            leads_result = self.supabase.client.table("leads").select("*").eq(
                "company_id", company_id).execute()

            total_points = sum(
                record.get('pontos', 0) for record in points_result.data)
            total_leads = len(leads_result.data)

            # Store in data_snapshots table
            self.supabase.client.table("data_snapshots").insert({
                "snapshot_date": datetime.now().isoformat(),
                "company_id": company_id,
                "total_leads": total_leads,
                "total_points": total_points,
                "snapshot_type": snapshot_type,
                "created_at": datetime.now().isoformat()
            }).execute()

            logger.info(
                f"Data snapshot created for company {company_id}: {total_leads} leads, {total_points} points")

        except Exception as e:
            logger.error(f"Error creating data snapshot: {str(e)}")
            # Don't raise - snapshots are optional

    def sync_data_incremental(self,
                              brokers=None,
                              leads=None,
                              activities=None,
                              company_id=None):
        """
        Synchronize only changed data with hash comparison.
        Returns dict indicating which data types had changes.
        """
        try:
            if not company_id:
                raise ValueError("company_id is required for incremental sync")

            changes_detected = {
                'brokers': False,
                'leads': False,
                'activities': False
            }

            # Processar Brokers PRIMEIRO (para garantir foreign keys)
            if isinstance(brokers, pd.DataFrame) and not brokers.empty:
                existing_brokers = self._get_existing_records('brokers')
                changes_found = False
                
                for i in range(0, len(brokers), self.batch_size):
                    batch = brokers.iloc[i:i + self.batch_size].to_dict('records')
                    batch_changes = self._process_batch_incremental(batch, 'brokers', existing_brokers)
                    if batch_changes:
                        changes_found = True
                
                changes_detected['brokers'] = changes_found
                if changes_found:
                    logger.info(f"Changes detected in brokers data")
                    self.supabase.initialize_broker_points(company_id)

            # Obter IDs válidos de brokers ANTES de processar leads
            try:
                broker_ids = self.supabase.client.table("brokers").select("id").eq("company_id", company_id).execute()
                valid_broker_ids = {item['id'] for item in broker_ids.data} if broker_ids.data else set()
                logger.info(f"Found {len(valid_broker_ids)} valid broker IDs for company {company_id}")
            except Exception as e:
                logger.error(f"Error getting valid broker IDs: {e}")
                valid_broker_ids = set()

            # Processar Leads com validação de foreign key
            if isinstance(leads, pd.DataFrame) and not leads.empty:
                # Filtrar leads apenas com responsavel_id válido
                original_count = len(leads)
                if valid_broker_ids:
                    leads_filtered = leads[
                        leads['responsavel_id'].isin(valid_broker_ids) | 
                        leads['responsavel_id'].isna()
                    ].copy()
                else:
                    # Se não há brokers, só manter leads sem responsavel_id
                    leads_filtered = leads[leads['responsavel_id'].isna()].copy()
                
                filtered_count = len(leads_filtered)
                if filtered_count < original_count:
                    logger.warning(f"Filtered out {original_count - filtered_count} leads with invalid responsavel_id")
                
                if not leads_filtered.empty:
                    existing_leads = self._get_existing_records('leads')
                    changes_found = False
                    
                    for i in range(0, len(leads_filtered), self.batch_size):
                        batch = leads_filtered.iloc[i:i + self.batch_size].to_dict('records')
                        batch_changes = self._process_batch_incremental(batch, 'leads', existing_leads)
                        if batch_changes:
                            changes_found = True
                    
                    changes_detected['leads'] = changes_found
                    if changes_found:
                        logger.info(f"Changes detected in leads data")
                else:
                    logger.warning("No valid leads found after filtering by responsavel_id")

            # Processar Activities
            if isinstance(activities, pd.DataFrame) and not activities.empty:
                # Filtrar por IDs válidos
                broker_ids = self.supabase.client.table("brokers").select("id").eq("company_id", company_id).execute()
                valid_broker_ids = {item['id'] for item in broker_ids.data} if broker_ids.data else set()

                lead_ids = self.supabase.client.table("leads").select("id").eq("company_id", company_id).execute()
                valid_lead_ids = {item['id'] for item in lead_ids.data} if lead_ids.data else set()

                filtered_activities = activities[(
                    (activities['lead_id'].isin(valid_lead_ids) | activities['lead_id'].isna()) &
                    (activities['user_id'].isin(valid_broker_ids) | activities['user_id'].isna())
                )].copy()

                if not filtered_activities.empty:
                    existing_activities = self._get_existing_records('activities')
                    changes_found = False
                    
                    for i in range(0, len(filtered_activities), self.batch_size):
                        batch = filtered_activities.iloc[i:i + self.batch_size].to_dict('records')
                        batch_changes = self._process_batch_incremental(batch, 'activities', existing_activities)
                        if batch_changes:
                            changes_found = True
                    
                    changes_detected['activities'] = changes_found
                    if changes_found:
                        logger.info(f"Changes detected in activities data")

            # Atualizar timestamps apenas se houve mudanças
            if any(changes_detected.values()):
                now = datetime.now()
                sync_interval = self.config.get('sync_interval', 60) if self.config else 60
                next_sync = now + timedelta(minutes=sync_interval)
                
                self.supabase.client.table("kommo_config").update({
                    "last_sync": now.isoformat(),
                    "next_sync": next_sync.isoformat()
                }).eq("company_id", company_id).execute()

            return changes_detected

        except Exception as e:
            logger.error(f"Incremental sync failed: {e}", exc_info=True)
            raise

    def _process_batch_incremental(self, records: List[Dict], table: str, existing_records: Dict) -> bool:
        """Process batch with change detection. Returns True if changes were found."""
        try:
            to_upsert = []
            changes_found = False

            for record in records:
                processed = self._prepare_record(record)
                record_id = processed.get('id')
                new_hash = self._generate_hash(processed)

                # Verificar se o registro mudou
                if record_id in existing_records:
                    if existing_records[record_id]['hash'] != new_hash:
                        processed['updated_at'] = datetime.now().isoformat()
                        to_upsert.append(processed)
                        changes_found = True
                        logger.debug(f"Change detected in {table} record ID: {record_id}")
                else:
                    # Novo registro
                    processed['updated_at'] = datetime.now().isoformat()
                    to_upsert.append(processed)
                    changes_found = True
                    logger.debug(f"New record in {table} ID: {record_id}")

            if to_upsert:
                result = self.supabase.client.table(table).upsert(to_upsert).execute()
                if hasattr(result, "error") and result.error:
                    raise Exception(f"Supabase error: {result.error}")

                logger.info(f"Updated {len(to_upsert)} changed records in {table}")

            return changes_found

        except Exception as e:
            logger.error(f"Error processing incremental batch for {table}: {str(e)}")
            raise

    def sync_data(self,
                  brokers=None,
                  leads=None,
                  activities=None,
                  company_id=None):
        """
        Synchronize data efficiently with batch processing.
        Args:
            brokers (pd.DataFrame): Optional pre-loaded brokers data
            leads (pd.DataFrame): Optional pre-loaded leads data
            activities (pd.DataFrame): Optional pre-loaded activities data
            company_id (str): Company ID to sync data for
        """
        # Calculate tempo_medio for each lead
        if leads is not None and not leads.empty:
            for idx, lead in leads.iterrows():
                tempo_medio = self.supabase.calculate_response_time(
                    lead['id'], lead['criado_em'])
                leads.at[idx, 'tempo_medio'] = tempo_medio

            # Calculate ticket_medio
            self.supabase.calculate_ticket_medio(leads)
        try:
            if not company_id:
                raise ValueError("company_id is required for sync_data")

            now = datetime.now()

            # Verifica se api_config está inicializada
            if self.kommo_api and getattr(self.kommo_api, 'api_config', None) is None:
                self.kommo_api.api_config = {}

            # Obter configuração da empresa, se necessário
            if not self.config:
                config_result = self.supabase.client.table(
                    "kommo_config").select("*").eq("company_id", company_id).execute()
                self.config = config_result.data[0] if config_result.data else {
                    "sync_interval": 60
                }

            sync_interval = self.config.get('sync_interval', 60)

            # Create periodic snapshots instead of monthly resets
            snapshots = self.supabase.client.table("data_snapshots").select(
                "*").eq("company_id", company_id).order("created_at", desc=True).limit(1).execute()
            if snapshots.data:
                last_snapshot = datetime.fromisoformat(
                    str(snapshots.data[0].get('created_at')))
                # Create weekly snapshots instead of monthly resets
                if (now - last_snapshot).days >= 7:
                    self.create_data_snapshot(company_id, "weekly_auto")

            config_data = self.supabase.client.table("kommo_config").select(
                "*").eq("company_id", company_id).execute().data
            if not config_data:
                logger.error(
                    f"No configuration found for company {company_id}")
                return
            
            # Carregar dados, se não fornecidos
            if brokers is None:
                brokers = self.kommo_api.get_users()
            if leads is None:
                leads = self.kommo_api.get_leads()
            if activities is None:
                activities = self.kommo_api.get_activities()

            # Processar Brokers
            if isinstance(brokers, pd.DataFrame) and not brokers.empty:
                brokers['company_id'] = company_id
                broker_batch_size = self.get_safe_batch_size('brokers')
                existing_brokers = self._get_existing_records('brokers')
                
                logger.info(f"Processing {len(brokers)} brokers in batches of {broker_batch_size}")
                for i in range(0, len(brokers), broker_batch_size):
                    batch = brokers.iloc[i:i + broker_batch_size].to_dict('records')
                    self._process_batch(batch, 'brokers', existing_brokers)
                    # Small delay between batches
                    time.sleep(0.1)
                    
                logger.info(f"Processed {len(brokers)} brokers")
                self.supabase.initialize_broker_points(company_id)
            else:
                logger.warning("No brokers data available for sync")
                return

            # Obter IDs válidos de brokers ANTES de processar leads
            try:
                broker_ids = self.supabase.client.table("brokers").select("id").eq("company_id", company_id).execute()
                valid_broker_ids = {item['id'] for item in broker_ids.data} if broker_ids.data else set()
                logger.info(f"Found {len(valid_broker_ids)} valid broker IDs for company {company_id}")
            except Exception as e:
                logger.error(f"Error getting valid broker IDs: {e}")
                valid_broker_ids = set()

            # Processar Leads com validação de foreign key
            if isinstance(leads, pd.DataFrame) and not leads.empty:
                leads['company_id'] = company_id
                
                # Filtrar leads apenas com responsavel_id válido
                original_count = len(leads)
                if valid_broker_ids:
                    leads_filtered = leads[
                        leads['responsavel_id'].isin(valid_broker_ids) | 
                        leads['responsavel_id'].isna()
                    ].copy()
                else:
                    # Se não há brokers, só manter leads sem responsavel_id
                    leads_filtered = leads[leads['responsavel_id'].isna()].copy()
                
                filtered_count = len(leads_filtered)
                if filtered_count < original_count:
                    logger.warning(f"Filtered out {original_count - filtered_count} leads with invalid responsavel_id")
                
                if not leads_filtered.empty:
                    leads_batch_size = self.get_safe_batch_size('leads')
                    existing_leads = self._get_existing_records('leads')
                    
                    logger.info(f"Processing {len(leads_filtered)} leads in batches of {leads_batch_size}")
                    for i in range(0, len(leads_filtered), leads_batch_size):
                        batch = leads_filtered.iloc[i:i + leads_batch_size].to_dict('records')
                        self._process_batch(batch, 'leads', existing_leads)
                        # Small delay between batches
                        time.sleep(0.1)
                        
                    logger.info(f"Processed {len(leads_filtered)} leads")
                else:
                    logger.warning("No valid leads found after filtering by responsavel_id")

            # Processar Activities
            if isinstance(activities, pd.DataFrame) and not activities.empty:
                # Obter IDs válidos
                broker_ids = self.supabase.client.table("brokers").select(
                    "id").eq("company_id", company_id).execute()
                valid_broker_ids = {item['id'] for item in broker_ids.data} if broker_ids.data else set()

                lead_ids = self.supabase.client.table("leads").select("id").eq(
                    "company_id", company_id).execute()
                valid_lead_ids = {item['id'] for item in lead_ids.data} if lead_ids.data else set()

                activities['company_id'] = company_id

                # Filter activities to only those with valid references
                filtered_activities = activities[(
                    (activities['lead_id'].isin(valid_lead_ids) | activities['lead_id'].isna()) &
                    (activities['user_id'].isin(valid_broker_ids) | activities['user_id'].isna())
                )].copy()

                if filtered_activities.empty:
                    logger.warning("No valid activities found after filtering")
                else:
                    activities_batch_size = self.get_safe_batch_size('activities')
                    existing_activities = self._get_existing_records('activities')
                    
                    logger.info(f"Processing {len(filtered_activities)} activities in batches of {activities_batch_size}")
                    for i in range(0, len(filtered_activities), activities_batch_size):
                        batch = filtered_activities.iloc[i:i + activities_batch_size].to_dict('records')
                        self._process_batch(batch, 'activities', existing_activities)
                        # Small delay between batches
                        time.sleep(0.1)
                        
                    logger.info(f"Processed {len(filtered_activities)} activities")

            # Atualizar timestamps de sincronização
            next_sync = now + timedelta(minutes=sync_interval)
            self.supabase.client.table("kommo_config").update({
                "last_sync":
                now.isoformat(),
                "next_sync":
                next_sync.isoformat()
            }).eq("active", True).execute()

            logger.info("Data synchronization completed successfully")

        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            raise

    def needs_sync(self, resource: str) -> bool:
        """Verifica se sincronização é necessária baseada em timestamp"""
        try:
            # Verifica último sync na configuração
            config_result = self.supabase.client.table("kommo_config").select(
                "last_sync, sync_interval"
            ).eq("active", True).execute()
            
            if not config_result.data:
                return True  # Sem configuração, force sync
                
            config = config_result.data[0]
            last_sync_str = config.get('last_sync')
            sync_interval = config.get('sync_interval', 30)  # Default 30 min
            
            if not last_sync_str:
                return True  # Nunca sincronizou
                
            last_sync = datetime.fromisoformat(last_sync_str)
            time_since_sync = (datetime.now() - last_sync).total_seconds() / 60  # em minutos
            
            return time_since_sync >= sync_interval
            
        except Exception as e:
            logger.error(f"Error checking sync necessity: {e}")
            return True  # Em caso de erro, force sync

    def update_sync_time(self, resource: str):
        #This method is not needed anymore because the sync time is updated directly in sync_data method.
        pass

    def force_sync(self) -> bool:
        """Force immediate sync of all data"""
        #The last_sync attribute is removed in edited code, so this method is updated.
        return self.sync_data()
