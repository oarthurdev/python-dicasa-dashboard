
import logging
import time
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

logger = logging.getLogger(__name__)

class SyncManager:
    def __init__(self, kommo_api, supabase_client, batch_size=100):
        self.kommo_api = kommo_api
        self.supabase = supabase_client
        self.batch_size = batch_size
        self.last_sync = {
            'brokers': None,
            'leads': None,
            'activities': None
        }
        self.cache = {
            'brokers': {},
            'leads': {},
            'activities': {}
        }
        self.sync_interval = 300  # seconds (5 minutes)

    def _generate_hash(self, data: Dict) -> str:
        """Generate a hash for data comparison"""
        return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def _process_batch(self, data_list: list, data_type: str) -> None:
        """Process a batch of records"""
        try:
            if not data_list:
                return

            # Convert records for database
            processed_records = []
            for record in data_list:
                # Convert Timestamp objects to ISO format strings and handle NaN values
                for key, value in record.items():
                    if hasattr(value, 'isoformat'):  # Check if it's datetime-like
                        record[key] = value.isoformat()
                    elif pd.isna(value):  # Handle NaN values
                        record[key] = None
                    elif key in ['lead_id', 'user_id'] and isinstance(value, (int, float)):
                        record[key] = int(value) if pd.notna(value) else None
                
                record_hash = self._generate_hash(record)
                
                # Skip if record hasn't changed
                if record.get('id') in self.cache[data_type] and \
                   self.cache[data_type][record['id']] == record_hash:
                    continue
                
                # Add updated_at timestamp
                record['updated_at'] = datetime.now().isoformat()
                processed_records.append(record)
                
                # Update cache
                self.cache[data_type][record['id']] = record_hash

            if processed_records:
                # Perform batch upsert
                result = self.supabase.client.table(data_type).upsert(
                    processed_records).execute()
                
                if hasattr(result, "error") and result.error:
                    self.supabase.insert_log("ERROR", f"Supabase error: {result.error}")
                    raise Exception(f"Supabase error: {result.error}")
                
                msg = f"Batch of {len(processed_records)} {data_type} processed"
                logger.info(msg)
                self.supabase.insert_log("INFO", msg)

        except Exception as e:
            error_msg = f"Error processing batch of {data_type}: {str(e)}"
            logger.error(error_msg)
            self.supabase.insert_log("ERROR", error_msg)
            raise

    def sync_from_cache(self, brokers, leads, activities):
        """Synchronize data from cache with batch processing"""
        try:
            if self.needs_sync('brokers') and brokers is not None:
                brokers_records = brokers.to_dict('records')
                for i in range(0, len(brokers_records), self.batch_size):
                    batch = brokers_records[i:i + self.batch_size]
                    self._process_batch(batch, 'brokers')
                self.update_sync_time('brokers')

            if self.needs_sync('leads') and leads is not None:
                # Get existing broker IDs
                result = self.supabase.client.table("brokers").select("id").execute()
                if hasattr(result, "error") and result.error:
                    raise Exception(f"Supabase error: {result.error}")
                    
                valid_broker_ids = {broker['id'] for broker in result.data}
                
                # Filter leads with valid responsavel_id
                leads_records = leads.to_dict('records')
                valid_leads = [
                    lead for lead in leads_records 
                    if lead.get('responsavel_id') in valid_broker_ids or lead.get('responsavel_id') is None
                ]
                
                for i in range(0, len(valid_leads), self.batch_size):
                    batch = valid_leads[i:i + self.batch_size]
                    self._process_batch(batch, 'leads')
                self.update_sync_time('leads')

            if self.needs_sync('activities') and activities is not None:
                # Get existing broker IDs
                broker_result = self.supabase.client.table("brokers").select("id").execute()
                if hasattr(broker_result, "error") and broker_result.error:
                    raise Exception(f"Supabase error: {broker_result.error}")
                valid_broker_ids = {broker['id'] for broker in broker_result.data}

                # Get existing lead IDs
                lead_result = self.supabase.client.table("leads").select("id").execute()
                if hasattr(lead_result, "error") and lead_result.error:
                    raise Exception(f"Supabase error: {lead_result.error}")
                valid_lead_ids = {lead['id'] for lead in lead_result.data}
                
                # Filter activities with valid user_id and lead_id
                activities_records = activities.to_dict('records')
                valid_activities = [
                    activity for activity in activities_records 
                    if (activity.get('user_id') in valid_broker_ids or activity.get('user_id') is None) and
                       (activity.get('lead_id') in valid_lead_ids or activity.get('lead_id') is None)
                ]
                
                for i in range(0, len(valid_activities), self.batch_size):
                    batch = valid_activities[i:i + self.batch_size]
                    self._process_batch(batch, 'activities')
                self.update_sync_time('activities')

            logger.info("Data sync completed successfully.")

        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            raise

    def needs_sync(self, resource: str) -> bool:
        # Verifica se já existem dados no banco
        result = self.supabase.client.table(resource).select("id").limit(1).execute()
        has_data = bool(result.data)

        last = self.last_sync.get(resource)
        if not last:
            return True
            
        # Só aplica delay se já existirem dados
        if has_data:
            return (datetime.now() - last) > timedelta(seconds=self.sync_interval)
        return True

    def update_sync_time(self, resource: str):
        self.last_sync[resource] = datetime.now()

    def force_sync(self) -> bool:
        """Force immediate sync of all data"""
        self.last_sync = {k: None for k in self.last_sync.keys()}
        return self.sync_from_cache()
