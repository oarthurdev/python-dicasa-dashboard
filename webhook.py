
from flask import Flask, request, jsonify
import os
import logging
from kommo_api import KommoAPI
from supabase_db import SupabaseClient
from gamification import calculate_broker_points
from data_processor import process_data
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Initialize API and database clients
kommo_api = KommoAPI(
    api_url=os.getenv("KOMMO_API_URL"),
    access_token=os.getenv("ACCESS_TOKEN_KOMMO")
)

supabase = SupabaseClient(
    url=os.getenv("VITE_SUPABASE_URL"),
    key=os.getenv("VITE_SUPABASE_ANON_KEY")
)

@app.route('/webhook/kommo', methods=['POST'])
def handle_webhook():
    try:
        # Verify webhook signature if needed
        webhook_data = request.json
        
        # Log webhook data
        logger.info(f"Received webhook: {webhook_data}")
        
        # Get updated data from Kommo API
        brokers = kommo_api.get_users()
        leads = kommo_api.get_leads()
        activities = kommo_api.get_activities()
        
        # Process data
        broker_data, lead_data, activity_data = process_data(brokers, leads, activities)
        
        # Calculate updated points
        ranking_data = calculate_broker_points(broker_data, lead_data, activity_data)
        
        # Update points in database
        supabase.upsert_broker_points(ranking_data)
        
        return jsonify({"status": "success", "message": "Points updated"}), 200
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)

