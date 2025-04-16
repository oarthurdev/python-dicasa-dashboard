import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

class SyncManager:
    def __init__(self, kommo_api, supabase_client):
        self.kommo_api = kommo_api
        self.supabase = supabase_client
        self.last_sync = {
            'users': None,
            'leads': None,
            'activities': None
        }
        self.cache = {
            'users': None,
            'leads': None,
            'activities': None
        }
        self.sync_interval = 300  # seconds (5 minutes)

    def needs_sync(self, resource: str) -> bool:
        last = self.last_sync.get(resource)
        if not last:
            return True
        return (datetime.now() - last) > timedelta(seconds=self.sync_interval)

    def update_sync_time(self, resource: str):
        self.last_sync[resource] = datetime.now()

    def sync_data(self) -> bool:
        try:
            retry_count = 3
            for attempt in range(retry_count):
                try:
                    # Sync users/brokers with retry and cache
                    if self.needs_sync('users'):
                        brokers = self.kommo_api.get_users()
                        if not brokers.empty:
                            # First ensure brokers are saved
                            self.supabase.upsert_brokers(brokers)
                            # Then initialize broker points if needed
                            self.supabase.initialize_broker_points()
                            self.cache['users'] = brokers
                        self.update_sync_time('users')

                    # Sync leads with cache
                    if self.needs_sync('leads'):
                        leads = self.kommo_api.get_leads()
                        if not leads.empty and (self.cache['leads'] is None or not leads.equals(self.cache['leads'])):
                            self.supabase.upsert_leads(leads)
                            self.cache['leads'] = leads
                        self.update_sync_time('leads')

                    # Sync activities with cache
                    if self.needs_sync('activities'):
                        activities = self.kommo_api.get_activities()
                        if not activities.empty and (self.cache['activities'] is None or not activities.equals(self.cache['activities'])):
                            self.supabase.upsert_activities(activities)
                            self.cache['activities'] = activities
                        self.update_sync_time('activities')

                    return True

                except Exception as e:
                    if attempt == retry_count - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1} after error: {str(e)}")
                    time.sleep(1)

        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            return False

    def force_sync(self) -> bool:
        """Force immediate sync of all data"""
        self.last_sync = {k: None for k in self.last_sync.keys()}
        return self.sync_data()