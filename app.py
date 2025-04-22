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

st.set_page_config(page_title="Dicasa - Sync with kommo",
                   layout="wide")

st.markdown(hide_streamlit_style, unsafe_allow_html=True)


@st.cache_resource
@st.cache_resource
def init_kommo_api():
    return KommoAPI(api_url=os.getenv("KOMMO_API_URL"),
                    access_token=os.getenv("ACCESS_TOKEN_KOMMO"))


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
        kommo_api = KommoAPI(api_url=os.getenv(
            "KOMMO_API_URL", "https://dicasaindaial.kommo.com/api/v4"),
                             access_token=os.getenv("ACCESS_TOKEN_KOMMO"))

        supabase = SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                                  key=os.getenv("VITE_SUPABASE_ANON_KEY"))

        sync_manager = SyncManager(kommo_api, supabase)
        last_sync_time = None

        # Initial check for broker_points data
        existing = supabase.client.table("broker_points").select("*").limit(
            1).execute()
        if not existing.data:
            logger.info("Inicializando broker_points...")
            supabase.initialize_broker_points()

        while True:
            try:
                time.sleep(300)  # Aguarda 5 minutos entre cada sincronização

                brokers = kommo_api.get_users()
                leads = kommo_api.get_leads()
                activities = kommo_api.get_activities()

                logger.info(
                    "Iniciando sincronização e atualização de pontos...")

                if not brokers.empty and not leads.empty and not activities.empty:
                    sync_manager.sync_from_cache(brokers, leads, activities)
                    auto_update_broker_points(brokers=brokers,
                                              leads=leads,
                                              activities=activities)

            except Exception as e:
                logger.error(f"Error in background sync: {str(e)}")
                time.sleep(5)

    except Exception as e:
        logger.error(f"Failed to initialize background sync: {str(e)}")

# Lock para sincronização do state
state_lock = threading.Lock()


def auto_update_broker_points(brokers=None, leads=None, activities=None):
    while True:
        try:
            logger.info("[Auto Update] Atualizando pontos dos corretores")
            supabase.update_broker_points(brokers=brokers,
                                          leads=leads,
                                          activities=activities)
            logger.info("[Auto Update] Pontos atualizados com sucesso")
            logger.info(
                "[Auto Update] Aguardando 5 minutos para a próxima atualização"
            )
            time.sleep(300)

            st.rerun()
        except Exception as e:
            logger.error(f"[Auto Update] Erro ao atualizar pontos: {str(e)}")
        time.sleep(10)

def main():
    data_thread = threading.Thread(
            target=background_data_loader, daemon=True)
    
    data_thread.start()

if __name__ == "__main__":
    main()
