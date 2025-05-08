
from fastapi import FastAPI, BackgroundTasks
from typing import Dict, Optional
import logging
import threading
from libs import KommoAPI, SupabaseClient, SyncManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
supabase_client = SupabaseClient()

# Store active sync threads
sync_threads: Dict[int, threading.Thread] = {}

def sync_company_data(company_id: int, config: dict):
    """Background task for company data synchronization"""
    try:
        logger.info(f"Starting sync thread for company {company_id}")
        kommo_api = KommoAPI(api_url=config['api_url'], access_token=config['access_token'])
        sync_manager = SyncManager(kommo_api, supabase_client)
        
        while True:
            try:
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
                
                # Update broker points
                supabase_client.update_broker_points(brokers=brokers,
                                                   leads=leads,
                                                   activities=activities)
                
                threading.Event().wait(300)  # Wait 5 minutes before next sync
                
            except Exception as e:
                logger.error(f"Error in sync loop for company {company_id}: {str(e)}")
                threading.Event().wait(60)  # Wait 1 minute before retry
                
    except Exception as e:
        logger.error(f"Fatal error in sync thread for company {company_id}: {str(e)}")
        if company_id in sync_threads:
            del sync_threads[company_id]

@app.post("/start_sync/{company_id}")
async def start_sync(company_id: int, background_tasks: BackgroundTasks):
    try:
        if company_id in sync_threads and sync_threads[company_id].is_alive():
            return {"status": "running", "message": f"Sync already running for company {company_id}"}
            
        config = supabase_client.client.table("kommo_config").select("*").eq("company_id", company_id).execute()
        if not config.data:
            return {"status": "error", "message": "Company configuration not found"}
            
        sync_thread = threading.Thread(
            target=sync_company_data,
            args=(company_id, config.data[0]),
            name=f"sync_thread_{company_id}",
            daemon=True
        )
        sync_threads[company_id] = sync_thread
        sync_thread.start()
        
        return {"status": "started", "message": f"Sync started for company {company_id}"}
        
    except Exception as e:
        logger.error(f"Error starting sync for company {company_id}: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/sync_status/{company_id}")
async def get_sync_status(company_id: int):
    if company_id in sync_threads:
        return {
            "status": "running" if sync_threads[company_id].is_alive() else "stopped",
            "company_id": company_id
        }
    return {"status": "not_found", "company_id": company_id}

@app.post("/stop_sync/{company_id}")
async def stop_sync(company_id: int):
    if company_id in sync_threads:
        del sync_threads[company_id]
        return {"status": "stopped", "message": f"Sync stopped for company {company_id}"}
    return {"status": "not_found", "message": f"No sync running for company {company_id}"}
