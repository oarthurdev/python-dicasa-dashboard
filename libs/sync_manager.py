
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
            'users': None,
            'leads': None,
            'activities': None
        }
        self.cache = {
            'users': {},
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
                    raise Exception(f"Supabase error: {result.error}")
                
                logger.info(f"Batch of {len(processed_records)} {data_type} processed")

        except Exception as e:
            logger.error(f"Error processing batch of {data_type}: {str(e)}")
            raise

    def sync_from_cache(self, brokers, leads, activities):
        """Synchronize data from cache with batch processing"""
        try:
            if self.needs_sync('users') and brokers is not None:
                brokers_records = brokers.to_dict('records')
                for i in range(0, len(brokers_records), self.batch_size):
                    batch = brokers_records[i:i + self.batch_size]
                    self._process_batch(batch, 'brokers')
                self.update_sync_time('users')

            if self.needs_sync('leads') and leads is not None:
                leads_records = leads.to_dict('records')
                for i in range(0, len(leads_records), self.batch_size):
                    batch = leads_records[i:i + self.batch_size]
                    self._process_batch(batch, 'leads')
                self.update_sync_time('leads')

            if self.needs_sync('activities') and activities is not None:
                activities_records = activities.to_dict('records')
                for i in range(0, len(activities_records), self.batch_size):
                    batch = activities_records[i:i + self.batch_size]
                    self._process_batch(batch, 'activities')
                self.update_sync_time('activities')

            logger.info("Data sync completed successfully.")

        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            raise

    def needs_sync(self, resource: str) -> bool:
        last = self.last_sync.get(resource)
        if not last:
            return True
        return (datetime.now() - last) > timedelta(seconds=self.sync_interval)

    def update_sync_time(self, resource: str):
        self.last_sync[resource] = datetime.now()

    def force_sync(self) -> bool:
        """Force immediate sync of all data"""
        self.last_sync = {k: None for k in self.last_sync.keys()}
        return self.sync_from_cache()
