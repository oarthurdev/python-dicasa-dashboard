from flask import Blueprint, request, jsonify
import os
import logging
from dotenv import load_dotenv

from kommo_api import KommoAPI
from supabase_db import SupabaseClient
from gamification import calculate_broker_points
from data_processor import process_data

load_dotenv()

logger = logging.getLogger(__name__)
webhook_bp = Blueprint('webhook', __name__, url_prefix='/webhook')

kommo_api = KommoAPI(
    api_url=os.getenv("KOMMO_API_URL"),
    access_token=os.getenv("ACCESS_TOKEN_KOMMO")
)

supabase = SupabaseClient(
    url=os.getenv("VITE_SUPABASE_URL"),
    key=os.getenv("VITE_SUPABASE_ANON_KEY")
)

@webhook_bp.route('/kommo', methods=['POST'])
def handle_webhook():
    try:
        webhook_data = request.json
        logger.info(f"Received webhook: {webhook_data}")

        brokers = kommo_api.get_users()
        leads = kommo_api.get_leads()
        activities = kommo_api.get_activities()

        broker_data, lead_data, activity_data = process_data(brokers, leads, activities)
        ranking_data = calculate_broker_points(broker_data, lead_data, activity_data)
        supabase.upsert_broker_points(ranking_data)

        return jsonify({"status": "success", "message": "Points updated"}), 200

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

