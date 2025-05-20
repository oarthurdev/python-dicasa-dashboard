import logging
import hashlib
import json
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

    def get_week_dates(self):
        """Get start and end dates for current week"""
        today = datetime.now()
        week_start = today - timedelta(days=7)
        return week_start, today

    def reset_weekly_data(self, company_id):
        """Reset weekly data and store in logs"""
        try:
            # Get current data before reset
            points_result = self.supabase.client.table("broker_points").select(
                "*").eq("company_id", company_id).execute()
            leads_result = self.supabase.client.table("leads").select("*").eq(
                "company_id", company_id).execute()

            total_points = sum(
                record.get('pontos', 0) for record in points_result.data)
            total_leads = len(leads_result.data)

            week_start, week_end = self.get_week_dates()

            # Store in weekly_logs
            self.supabase.client.table("weekly_logs").insert({
                "week_start":
                week_start.isoformat(),
                "week_end":
                week_end.isoformat(),
                "company_id":
                company_id,
                "total_leads":
                total_leads,
                "total_points":
                total_points,
                "created_at":
                datetime.now().isoformat()
            }).execute()

            # Reset tables
            tables = ["activities", "leads", "brokers", "broker_points"]
            for table in tables:
                self.supabase.client.table(table).delete().eq(
                    "company_id", company_id).execute()

            logger.info(
                f"Weekly data reset completed for company {company_id}")

        except Exception as e:
            logger.error(f"Error resetting weekly data: {str(e)}")
            raise

    def sync_data(self,
                  brokers=None,
                  leads=None,
                  activities=None,
                  company_id=None):
        """
        Synchronize data efficiently with batch processing
        Args:
            brokers (pd.DataFrame): Optional pre-loaded brokers data
            leads (pd.DataFrame): Optional pre-loaded leads data
            activities (pd.DataFrame): Optional pre-loaded activities data
            company_id (str): Company ID to sync data for
        """
        try:
            if not company_id:
                raise ValueError("company_id is required for sync_data")

            now = datetime.now()
            week_start, week_end = self.get_week_dates()

            # Check if it's time for weekly reset
            last_reset_result = self.supabase.client.table(
                "weekly_logs").select("*").eq("company_id", company_id).order(
                    "created_at", desc=True).limit(1).execute()
            if last_reset_result.data:
                last_reset = datetime.fromisoformat(
                    str(last_reset_result.data[0].get('created_at')))
                if (now - last_reset).days >= 7:
                    self.reset_weekly_data(company_id)

            # Set date range for data fetch
            if self.kommo_api:
                self.kommo_api.set_date_range(week_start, week_end)

            # Get company configuration if not already set
            if not self.config:
                config_result = self.supabase.client.table("kommo_config").select("*").eq("company_id", company_id).execute()
                if config_result.data:
                    self.config = config_result.data[0]
                else:
                    self.config = {"sync_interval": 60}  # Default configuration
            
            # Safely get sync interval
            sync_interval = self.config.get('sync_interval', 60)

            # Verificar último sync dessa company
            last_sync_result = self.supabase.client.table(
                "sync_control").select("*").eq("company_id",
                                               company_id).execute()
            if last_sync_result.data:
                last_sync = datetime.fromisoformat(
                    str(last_sync_result.data[0].get('last_sync')))
                if (now - last_sync).total_seconds() < (sync_interval * 60):
                    logger.info(
                        f"Sync not needed yet for company {company_id}")
                    return

            # Já validamos o company_id no início, não precisamos buscar novamente
            config_data = self.supabase.client.table("kommo_config").select(
                "*").eq("company_id", company_id).execute().data
            if not config_data:
                logger.error(
                    f"No configuration found for company {company_id}")
                return

            if config_data[0].get('last_sync'):
                try:
                    last_sync = datetime.fromisoformat(
                        str(config_data[0].get('last_sync')))
                    if (now - last_sync).total_seconds() < (sync_interval *
                                                            60):
                        logger.info(
                            f"Sync not needed yet for company {company_id}")
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

            # Always process brokers first
            if isinstance(brokers, pd.DataFrame) and not brokers.empty:
                # Add company_id to brokers
                brokers['company_id'] = company_id
                existing_brokers = self._get_existing_records('brokers')
                brokers_records = brokers.to_dict('records')
                for i in range(0, len(brokers_records), self.batch_size):
                    batch = brokers_records[i:i + self.batch_size]
                    self._process_batch(batch, 'brokers', existing_brokers)
                logger.info(f"Processed {len(brokers)} brokers")

                # Initialize broker points after broker sync
                self.supabase.initialize_broker_points(company_id)
            else:
                logger.warning("No brokers data available for sync")
                return

            # Process leads for specific company
            if isinstance(leads, pd.DataFrame) and not leads.empty:
                # Filter and add company_id
                leads['company_id'] = company_id
                existing_leads = self._get_existing_records('leads')
                leads_records = leads.to_dict('records')

                for i in range(0, len(leads_records), self.batch_size):
                    batch = leads_records[i:i + self.batch_size]
                    self._process_batch(batch, 'leads', existing_leads)
                logger.info(f"Processed {len(leads)} leads")

            # Process activities with proper validation
            if isinstance(activities, pd.DataFrame) and not activities.empty:
                # Get valid broker IDs from database for this company
                brokers_result = self.supabase.client.table("brokers").select(
                    "id").eq("company_id", company_id).execute()
                valid_broker_ids = {
                    broker['id']
                    for broker in brokers_result.data
                } if brokers_result.data else set()

                # Get valid lead IDs from database for this company
                leads_result = self.supabase.client.table("leads").select(
                    "id").eq("company_id", company_id).execute()
                valid_lead_ids = {lead['id']
                                  for lead in leads_result.data
                                  } if leads_result.data else set()

                # Add company_id to activities
                activities['company_id'] = company_id

                # Filter activities keeping only those with valid lead_ids and user_ids
                filtered_activities = activities[(
                    (activities['lead_id'].isin(valid_lead_ids)
                     | activities['lead_id'].isna()) &
                    (activities['user_id'].isin(valid_broker_ids)
                     | activities['user_id'].isna()))].copy()

                if filtered_activities.empty:
                    logger.warning("No valid activities found after filtering")
                    return

                existing_activities = self._get_existing_records('activities')
                activities_records = filtered_activities.to_dict('records')

                for i in range(0, len(activities_records), self.batch_size):
                    batch = activities_records[i:i + self.batch_size]
                    self._process_batch(batch, 'activities',
                                        existing_activities)
                logger.info(f"Processed {len(filtered_activities)} activities")

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
