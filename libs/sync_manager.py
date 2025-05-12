import logging
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd

logger = logging.getLogger(__name__)


class SyncManager:

    def __init__(self, kommo_api, supabase_client, batch_size=250):
        self.kommo_api = kommo_api
        self.supabase = supabase_client
        self.batch_size = batch_size
        self.cache = {'brokers': {}, 'leads': {}, 'activities': {}}
        self.config = self.supabase.load_kommo_config()

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

    def sync_data(self,
                  brokers=None,
                  leads=None,
                  activities=None,
                  company_id=None):
        """Synchronize data efficiently with batch processing"""
        try:
            now = datetime.now()
            sync_interval = self.config.get('sync_interval',
                                            60)  # default 60 minutes

            # Get company_id from config if not provided
            if not company_id:
                config_data = self.supabase.client.table(
                    "kommo_config").select("*").eq("active",
                                                   True).execute().data
                if not config_data:
                    logger.error("No active configuration found")
                    return
                company_id = config_data[0].get('company_id')
                if not company_id:
                    logger.error("No company_id found in configuration")
                    return

                if config_data[0].get('last_sync'):
                    try:
                        last_sync = datetime.fromisoformat(
                            str(config_data[0].get('last_sync')))
                        if (now - last_sync).total_seconds() < (sync_interval *
                                                                60):
                            logger.info("Sync not needed yet")
                            return
                    except (ValueError, TypeError):
                        logger.warning(
                            "Invalid last_sync format, proceeding with sync")

            # Get data if not provided
            if brokers is None:
                brokers = self.kommo_api.get_users()
            if leads is None:
                leads = self.kommo_api.get_leads()
            if activities is None:
                activities = self.kommo_api.get_activities()

            # Process in sequence to maintain referential integrity
            if not brokers.empty:
                # Add company_id to brokers
                brokers['company_id'] = company_id
                existing_brokers = self._get_existing_records('brokers')
                brokers_records = brokers.to_dict('records')
                for i in range(0, len(brokers_records), self.batch_size):
                    batch = brokers_records[i:i + self.batch_size]
                    self._process_batch(batch, 'brokers', existing_brokers)

                # Initialize broker points with company_id
                self.supabase.initialize_broker_points(company_id)

            if not leads.empty:
                # Add company_id to leads without filtering by account_id
                leads['company_id'] = company_id
                existing_leads = self._get_existing_records('leads')
                leads_records = leads.to_dict('records')
                for i in range(0, len(leads_records), self.batch_size):
                    batch = leads_records[i:i + self.batch_size]
                    self._process_batch(batch, 'leads', existing_leads)

            # Validate foreign keys before processing activities
            if not activities.empty:
                # Add company_id to activities
                activities['company_id'] = company_id
                # Get valid IDs
                valid_leads = set(
                    leads['id'].unique()) if not leads.empty else set()
                valid_brokers = set(
                    brokers['id'].unique()) if not brokers.empty else set()

                # Filter activities
                activities = activities[
                    (activities['lead_id'].isin(valid_leads)
                     | activities['lead_id'].isna())
                    & (activities['user_id'].isin(valid_brokers)
                       | activities['user_id'].isna())]

                if not activities.empty:
                    existing_activities = self._get_existing_records(
                        'activities')
                    activities_records = activities.to_dict('records')
                    for i in range(0, len(activities_records),
                                   self.batch_size):
                        batch = activities_records[i:i + self.batch_size]
                        self._process_batch(batch, 'activities',
                                            existing_activities)

            # Update sync timestamps
            next_sync = now + timedelta(minutes=sync_interval)
            self.supabase.client.table("kommo_config").update({
                "last_sync":
                now.isoformat(),
                "next_sync":
                next_sync.isoformat()
            }).eq("active", True).execute()

            logger.info("Data synchronization completed successfully")

        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            raise

    def needs_sync(self, resource: str) -> bool:
        # Verifica se já existem dados no banco
        result = self.supabase.client.table(resource).select("id").limit(
            1).execute()
        has_data = bool(result.data)

        #There is no last_sync attribute in edited code, so this check is removed.
        # last = self.last_sync.get(resource)
        # if not last:
        #     return True

        # Só aplica delay se já existirem dados
        # if has_data:
        #     return (datetime.now() -
        #             last) > timedelta(seconds=self.sync_interval)
        return True  #Always sync if there's data.

    def update_sync_time(self, resource: str):
        #This method is not needed anymore because the sync time is updated directly in sync_data method.
        pass

    def force_sync(self) -> bool:
        """Force immediate sync of all data"""
        #The last_sync attribute is removed in edited code, so this method is updated.
        return self.sync_data()
