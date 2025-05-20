from flask import Flask, jsonify
import threading
import logging
import time
from datetime import datetime
from libs.supabase_db import SupabaseClient
from libs.kommo_api import KommoAPI
from libs.sync_manager import SyncManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global state management
threads_status = {}
sync_threads = {}
supabase = SupabaseClient()


def sync_data(company_id, sync_interval):
    """Execute sync function for a specific company"""
    while True:
        try:
            local_supabase = SupabaseClient()

            logger.info(f"Starting sync for company {company_id}")
            threads_status[company_id] = {
                'status': 'running',
                'last_sync': datetime.now(),
                'next_sync': None
            }

            # Initialize APIs and sync manager
            configs = local_supabase.load_kommo_config(company_id=company_id)

            if isinstance(configs, list) and configs:
                company_config = configs[0]
            else:
                logger.info(
                    f"Nenhuma configuração encontrada para a empresa {company_id}"
                )
                return  # ou continue, dependendo do contexto

            kommo_api = KommoAPI(api_config=company_config)
            sync_manager = SyncManager(kommo_api, local_supabase,
                                       company_config)

            # Execute sync with proper order
            brokers = kommo_api.get_users()

            # First sync brokers
            sync_manager.sync_data(brokers=brokers, company_id=company_id)

            # Then get and sync other data
            leads = kommo_api.get_leads()
            activities = kommo_api.get_activities()

            sync_manager.sync_data(brokers=brokers,
                                   leads=leads,
                                   activities=activities,
                                   company_id=company_id)

            # Only update points if we have broker data
            if not brokers.empty:
                broker_data = brokers[brokers['cargo'] == 'Corretor'].copy()
                if not broker_data.empty:
                    broker_data['company_id'] = company_id
                    if not leads.empty:
                        leads['company_id'] = company_id
                    if not activities.empty:
                        activities['company_id'] = company_id

                    # Primeiro garantir que os brokers estão no banco
                    local_supabase.upsert_brokers(broker_data)
                    time.sleep(
                        1)  # Pequena pausa para garantir conclusão do upsert

                    # Depois atualizar os pontos
                    local_supabase.update_broker_points(brokers=broker_data,
                                                        leads=leads,
                                                        activities=activities,
                                                        company_id=company_id)
                else:
                    logger.warning("No brokers with 'Corretor' role found")
            else:
                logger.warning(
                    "Skipping points update - no broker data available")

            next_sync = datetime.now()
            threads_status[company_id].update({
                'status': 'waiting',
                'last_sync': datetime.now(),
                'next_sync': next_sync
            })

            logger.info(
                f"Sync completed for company {company_id}. Next sync in {sync_interval} minutes"
            )
            time.sleep(sync_interval * 60)

        except Exception as e:
            logger.error(f"Error in sync thread for company {company_id}: {e}")
            threads_status[company_id]['status'] = 'error'
            time.sleep(5)  # Wait before retry


def load_companies():
    """Load all companies from kommo_config"""
    try:
        result = supabase.client.table("kommo_config").select("*").execute()
        return result.data if result.data else []
    except Exception as e:
        logger.error(f"Error loading companies: {e}")
        return []


def start_sync_thread(company):
    """Start a new sync thread for a company"""
    company_id = str(company['company_id'])
    sync_interval = company.get('sync_interval', 5)  # Default 5 minutes

    if company_id in sync_threads and sync_threads[company_id].is_alive():
        logger.info(f"Thread already running for company {company_id}")
        return

    thread = threading.Thread(target=sync_data,
                              args=(company_id, sync_interval),
                              daemon=True,
                              name=f"sync_thread_{company_id}")

    sync_threads[company_id] = thread
    thread.start()
    logger.info(f"Started sync thread for company {company_id}")


def monitor_threads():
    """Monitor and restart dead threads"""
    while True:
        try:
            companies = load_companies()

            for company in companies:
                company_id = str(company['company_id'])

                # Start thread if not exists or dead
                if (company_id not in sync_threads
                        or not sync_threads[company_id].is_alive()):
                    start_sync_thread(company)

            time.sleep(30)  # Check every 30 seconds

        except Exception as e:
            logger.error(f"Error in thread monitor: {e}")
            time.sleep(5)


@app.route('/status')
def get_status():
    """Get status of all sync threads"""
    status = {}
    for company_id, thread in sync_threads.items():
        status[company_id] = {
            'active': thread.is_alive(),
            'status': threads_status.get(company_id,
                                         {}).get('status', 'unknown'),
            'last_sync': threads_status.get(company_id, {}).get('last_sync'),
            'next_sync': threads_status.get(company_id, {}).get('next_sync')
        }
    return jsonify(status)


@app.route('/start', methods=['POST'])
def start_sync():
    """Start/restart all sync threads"""
    try:
        companies = load_companies()
        for company in companies:
            start_sync_thread(company)
        return jsonify({
            'status': 'success',
            'message': 'Sync threads started'
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/stop', methods=['POST'])
def stop_sync():
    """Stop all sync threads (for testing)"""
    sync_threads.clear()
    threads_status.clear()
    return jsonify({'status': 'success', 'message': 'All threads stopped'})


def initialize_app():
    """Initialize the application"""
    # Start monitor thread
    monitor_thread = threading.Thread(target=monitor_threads, daemon=True)
    monitor_thread.start()

    # Load initial companies and start their threads
    companies = load_companies()
    for company in companies:
        start_sync_thread(company)


if __name__ == '__main__':
    initialize_app()
    app.run(host='0.0.0.0', port=5000)
