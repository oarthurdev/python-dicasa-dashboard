import os
import time
import streamlit as st
from dotenv import load_dotenv
import threading
import logging

# Import custom modules
from libs import KommoAPI, SupabaseClient, SyncManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

hide_streamlit_style = """
            <style>
                /* Hide the Streamlit header and menu */
                header {visibility: hidden;}
            </style>
            """

st.set_page_config(page_title="Dicasa - Sync with kommo", layout="wide")

st.markdown(hide_streamlit_style, unsafe_allow_html=True)


@st.cache_resource
def init_kommo_api():
    supabase = init_supabase_client()
    return KommoAPI(supabase_client=supabase)


def init_supabase_client():
    return SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                          key=os.getenv("VITE_SUPABASE_ANON_KEY"))


supabase = init_supabase_client()


def background_data_loader():
    """
    This function runs in the background to continuously monitor Kommo API
    for changes and update the database accordingly
    """
    try:
        global supabase

        # Initialize components only when configuration exists
        while True:
            supabase._load_initial_config()  # Recarrega a configuração
            if supabase.kommo_config:
                kommo_api = KommoAPI(api_url=supabase.kommo_config['api_url'],
                                     access_token=supabase.kommo_config['access_token'],
                                     supabase_client=supabase)
                sync_manager = SyncManager(kommo_api, supabase)
                break
            time.sleep(5)  # Wait for configuration to be added

        # Initial check for broker_points data
        existing = supabase.client.table("broker_points").select("*").limit(
            1).execute()
        if not existing.data:
            logger.info("Inicializando broker_points...")
            supabase.initialize_broker_points()

        while True:
            try:
                supabase.check_config_changes()  # Check for config changes

                if sync_manager.needs_sync('brokers') or sync_manager.needs_sync('leads') or sync_manager.needs_sync('activities'):
                    brokers = kommo_api.get_users()
                    leads = kommo_api.get_leads()
                    activities = kommo_api.get_activities()

                    logger.info(
                        "Iniciando sincronização e atualização de pontos...")

                    if not brokers.empty and not leads.empty and not activities.empty:
                        sync_manager.sync_data(brokers=brokers, leads=leads, activities=activities)
                        # Força atualização de pontos e aguarda finalização
                        auto_update_broker_points(brokers=brokers,
                                                leads=leads,
                                                activities=activities,
                                                force=True)

                logger.info("Aguardando próximo ciclo de sincronização...")
                time.sleep(300)  # Wait 5 minutes before next check

            except Exception as e:
                logger.error(f"Error in background sync: {str(e)}")
                time.sleep(5)

    except Exception as e:
        logger.error(f"Failed to initialize background sync: {str(e)}")


# Lock para sincronização do state
state_lock = threading.Lock()


def auto_update_broker_points(brokers=None, leads=None, activities=None, force=False):
    while True:
        try:
            logger.info("[Auto Update] Atualizando pontos dos corretores")
            supabase.update_broker_points(brokers=brokers,
                                          leads=leads,
                                          activities=activities)
            logger.info("[Auto Update] Pontos atualizados com sucesso")

            if not force:
                logger.info("[Auto Update] Aguardando 5 minutos para a próxima atualização")
                time.sleep(300)
                st.rerun()
            else:
                break

        except Exception as e:
            logger.error(f"[Auto Update] Erro ao atualizar pontos: {str(e)}")
            if not force:
                time.sleep(10)
            else:
                break


def sync_data():
    try:
        supabase = SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                                key=os.getenv("VITE_SUPABASE_ANON_KEY"))
        config = supabase.load_kommo_config()
        if not config:
            return {"status": "error", "message": "No Kommo configuration found"}
        config = config[0]
        kommo_api = KommoAPI(api_url=config['api_url'],
                            access_token=config['access_token'],
                            supabase_client=supabase)

        sync_manager = SyncManager(kommo_api, supabase)

        # Reset last sync times to force immediate sync
        sync_manager.last_sync = {k: None for k in sync_manager.last_sync.keys()}

        brokers = kommo_api.get_users()
        leads = kommo_api.get_leads()
        activities = kommo_api.get_activities()

        if not brokers.empty and not leads.empty and not activities.empty:
            # Using original sync_from_cache but with reset last_sync times
            sync_manager.sync_from_cache(brokers, leads, activities)
            return {"status": "success", "message": "Forced sync completed successfully"}
        else:
            return {"status": "error", "message": "Failed to fetch data"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def main():
    data_thread = threading.Thread(target=background_data_loader, daemon=True)
    data_thread.start()

    # Add sync endpoint
    if st.session_state.get('page') == 'sync':
        force = st.session_state.get('force', False)
        if force:
            with st.spinner('Forcing sync...'):
                result = sync_data()
                st.json(result)


if __name__ == "__main__":
    main()