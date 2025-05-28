from flask import Flask, jsonify
import threading
import logging
import time
from datetime import datetime, timedelta
from libs.supabase_db import SupabaseClient
from libs.kommo_api import KommoAPI
from libs.sync_manager import SyncManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global state
threads_status = {}
supabase = SupabaseClient()
COMPANY_LIST = []  # Atualizada no início e em cada ciclo

SYNC_INTERVAL_MINUTES = 30


def load_companies():
    """Load all companies from kommo_config"""
    try:
        result = supabase.client.table("kommo_config").select("*").execute()
        return result.data if result.data else []
    except Exception as e:
        logger.error(f"Error loading companies: {e}")
        return []


def sync_data(company_id):
    """Execute sync function once for a specific company"""
    try:
        local_supabase = SupabaseClient()

        local_supabase.check_config_changes();
        
        company_result = local_supabase.client.table("companies").select(
            "subdomain").eq("id", company_id).execute()
        if not company_result.data:
            logger.error(f"Company {company_id} not found")
            return

        subdomain = company_result.data[0]['subdomain']
        logger.info(f"Starting sync for company {company_id} (subdomain: {subdomain})")

        threads_status[company_id] = {
            'status': 'running',
            'last_sync': datetime.now(),
            'next_sync': None,
            'subdomain': subdomain
        }

        configs = local_supabase.load_kommo_config(company_id=company_id)

        if isinstance(configs, list) and configs:
            company_config = configs[0]
        else:
            logger.info(f"Nenhuma configuração encontrada para a empresa {company_id}")
            return

        kommo_api = KommoAPI(api_config=company_config)
        sync_manager = SyncManager(kommo_api, local_supabase, company_config)

        brokers = kommo_api.get_users()
        if not brokers.empty:
            brokers['company_id'] = company_id

        sync_manager.sync_data(brokers=brokers, company_id=company_id)

        leads = kommo_api.get_leads()
        activities = kommo_api.get_activities()

        if not leads.empty:
            leads['company_id'] = company_id
        if not activities.empty:
            activities['company_id'] = company_id

        sync_manager.sync_data(leads=leads, activities=activities, company_id=company_id)

        if not brokers.empty:
            broker_data = brokers[(brokers['cargo'] == 'Corretor') & (brokers['company_id'] == company_id)].copy()
            if not broker_data.empty:
                broker_data['company_id'] = company_id
                local_supabase.update_broker_points(
                    brokers=broker_data,
                    leads=leads,
                    activities=activities,
                    company_id=company_id
                )
            else:
                logger.warning("No brokers with 'Corretor' role found for this company")
        else:
            logger.warning("Skipping points update - no broker data available")

        threads_status[company_id].update({
            'status': 'waiting',
            'last_sync': datetime.now(),
            'next_sync': datetime.now() + timedelta(minutes=SYNC_INTERVAL_MINUTES)
        })

        logger.info(f"Sync completed for company {company_id}.")

    except Exception as e:
        logger.error(f"Error in sync for company {company_id}: {e}")
        threads_status[company_id] = {'status': 'error'}


def sync_cycle():
    """Run sync for all companies once every SYNC_INTERVAL_MINUTES"""
    global COMPANY_LIST

    while True:
        COMPANY_LIST = load_companies()
        if not COMPANY_LIST:
            logger.warning("No companies to sync. Retrying in 1 minute...")
            time.sleep(60)
            continue

        logger.info("Starting sync cycle for all companies")
        threads = []

        for company in COMPANY_LIST:
            company_id = str(company['company_id'])
            t = threading.Thread(target=sync_data, args=(company_id,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        logger.info(f"All companies have completed sync. Sleeping {SYNC_INTERVAL_MINUTES} minutes...")
        time.sleep(SYNC_INTERVAL_MINUTES * 60)


@app.route('/status')
def get_status():
    """Get status of all sync operations"""
    status = {}
    for company_id, info in threads_status.items():
        status[company_id] = {
            'status': info.get('status', 'unknown'),
            'last_sync': info.get('last_sync'),
            'next_sync': info.get('next_sync'),
            'subdomain': info.get('subdomain')
        }
    return jsonify(status)


@app.route('/start', methods=['POST'])
def start_sync():
    """Start the global sync loop if not already running"""
    global sync_thread
    if sync_thread and sync_thread.is_alive():
        return jsonify({'status': 'already_running'})

    sync_thread = threading.Thread(target=sync_cycle, daemon=True)
    sync_thread.start()
    return jsonify({'status': 'started'})


@app.route('/stop', methods=['POST'])
def stop_sync():
    """Not implemented: Use process control to stop daemon thread"""
    return jsonify({'status': 'not_implemented', 'message': 'Stop via system process control'})


if __name__ == '__main__':
    sync_thread = threading.Thread(target=sync_cycle, daemon=True)
    sync_thread.start()
    app.run(host='0.0.0.0', port=5002)
