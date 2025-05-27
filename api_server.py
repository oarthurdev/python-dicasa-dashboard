from fastapi import FastAPI, BackgroundTasks
import logging
import threading
from typing import Dict
from uuid import UUID
from datetime import datetime, timedelta
from libs import KommoAPI, SupabaseClient, SyncManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Threads e controle de status por empresa
sync_threads: Dict[str, threading.Thread] = {}
sync_statuses: Dict[str, datetime] = {}

# Tempo máximo de inatividade antes de matar thread (em minutos)
MAX_INACTIVE_MINUTES = 6


def sync_company_data(company_id: UUID, config: dict):
    """Thread de sincronização contínua para uma empresa."""
    company_key = str(company_id)

    try:
        logger.info(f"[{company_key}] Starting sync thread")

        local_supabase = SupabaseClient()
        kommo_api = KommoAPI(api_url=config['api_url'],
                             access_token=config['access_token'],
                             supabase_client=local_supabase)
        sync_manager = SyncManager(kommo_api, local_supabase)

        while True:
            try:
                # Atualiza status da thread
                sync_statuses[company_key] = datetime.utcnow()

                brokers = kommo_api.get_users()
                leads = kommo_api.get_leads()
                activities = kommo_api.get_activities()

                if not brokers.empty:
                    brokers = brokers[brokers['cargo'] == 'Corretor']
                    brokers['company_id'] = company_key

                if not leads.empty and not brokers.empty:
                    valid_broker_ids = set(brokers['id'].unique())
                    leads = leads[leads['responsavel_id'].isin(
                        valid_broker_ids)]
                    leads['company_id'] = company_key

                if not activities.empty and not brokers.empty:
                    valid_broker_ids = set(brokers['id'].unique())
                    activities = activities[activities['user_id'].isin(
                        valid_broker_ids)]
                    activities['company_id'] = company_key

                # Sincroniza dados e atualiza pontos
                sync_manager.sync_data(company_id=company_key)
                local_supabase.update_broker_points(brokers=brokers,
                                                    leads=leads,
                                                    activities=activities)

                # Espera 5 min para respeitar rate limits da API Kommo
                threading.Event().wait(300)

            except Exception as e:
                logger.error(f"[{company_key}] Error in sync loop: {e}")
                threading.Event().wait(60)

    except Exception as fatal:
        logger.critical(f"[{company_key}] Fatal error in thread: {fatal}")
    finally:
        # Limpa registros quando a thread terminar
        sync_threads.pop(company_key, None)
        sync_statuses.pop(company_key, None)
        logger.info(f"[{company_key}] Sync thread terminated")


def monitor_thread_health():
    """Thread supervisora que mata threads inativas."""
    while True:
        now = datetime.utcnow()
        for company_id in list(sync_threads.keys()):
            last_ping = sync_statuses.get(company_id)
            if last_ping and now - last_ping > timedelta(
                    minutes=MAX_INACTIVE_MINUTES):
                logger.warning(
                    f"[{company_id}] Inactive for too long. Killing thread.")
                sync_threads.pop(company_id, None)
                sync_statuses.pop(company_id, None)
        threading.Event().wait(120)  # Verifica a cada 2 minutos


# Inicia o monitoramento automático de inatividade
monitor_thread = threading.Thread(target=monitor_thread_health, daemon=True)
monitor_thread.start()


@app.post("/start_sync/{company_id}")
async def start_sync(company_id: str, background_tasks: BackgroundTasks):
    try:
        company_uuid = UUID(company_id)
        company_key = str(company_uuid)

        # Já existe uma thread viva?
        if company_key in sync_threads and sync_threads[company_key].is_alive(
        ):
            return {
                "status": "running",
                "message": f"Sync already running for company {company_key}"
            }

        # Busca a configuração no Supabase
        temp_supabase = SupabaseClient()
        response = temp_supabase.client.table("kommo_config").select("*").eq(
            "company_id", company_uuid).execute()

        if not response.data:
            return {
                "status": "error",
                "message": f"No config found for company {company_key}"
            }

        config = response.data[0].copy()

        # Cria e inicia a thread isolada
        sync_thread = threading.Thread(target=sync_company_data,
                                       args=(company_uuid, config),
                                       name=f"sync_thread_{company_key}",
                                       daemon=True)
        sync_threads[company_key] = sync_thread
        sync_statuses[company_key] = datetime.utcnow()
        sync_thread.start()

        return {
            "status": "started",
            "message": f"Sync started for company {company_key}"
        }

    except ValueError:
        return {
            "status": "error",
            "message": "Invalid UUID format for company_id"
        }
    except Exception as e:
        logger.error(f"[{company_id}] Error starting sync: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/sync_status/{company_id}")
async def get_sync_status(company_id: str):
    try:
        company_key = str(UUID(company_id))
        if company_key in sync_threads:
            is_alive = sync_threads[company_key].is_alive()
            last_active = sync_statuses.get(company_key)
            return {
                "status": "running" if is_alive else "stopped",
                "last_active":
                last_active.isoformat() if last_active else None,
                "company_id": company_key
            }
        return {"status": "not_found", "company_id": company_key}
    except ValueError:
        return {
            "status": "error",
            "message": "Invalid UUID format for company_id"
        }


@app.post("/stop_all_syncs")
async def stop_all_syncs():
    stopped_companies = list(sync_threads.keys())

    # Limpa todas as threads e status
    sync_threads.clear()
    sync_statuses.clear()

    logger.info(f"Manually stopped all sync threads: {stopped_companies}")

    return {
        "status": "all_stopped",
        "message": f"{len(stopped_companies)} sync threads stopped.",
        "companies": stopped_companies
    }


@app.post("/stop_sync/{company_id}")
async def stop_sync(company_id: str):
    try:
        company_key = str(UUID(company_id))
        if company_key in sync_threads:
            sync_threads.pop(company_key, None)
            sync_statuses.pop(company_key, None)
            return {
                "status": "stopped",
                "message": f"Sync manually stopped for company {company_key}"
            }
        return {
            "status": "not_found",
            "message": f"No sync running for company {company_key}"
        }
    except ValueError:
        return {
            "status": "error",
            "message": "Invalid UUID format for company_id"
        }
