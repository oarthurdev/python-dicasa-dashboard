
from flask import Flask, request, jsonify
from libs import KommoAPI, SupabaseClient, SyncManager
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

@app.route('/sync', methods=['POST'])
def sync():
    try:
        force = request.json.get('force', False)
        if force:
            supabase = SupabaseClient(url=os.getenv("VITE_SUPABASE_URL"),
                                    key=os.getenv("VITE_SUPABASE_ANON_KEY"))
            kommo_api = KommoAPI(supabase_client=supabase)
            sync_manager = SyncManager(kommo_api, supabase)
            
            # Reset last sync times to force immediate sync
            sync_manager.last_sync = {k: None for k in sync_manager.last_sync.keys()}
            
            brokers = kommo_api.get_users()
            leads = kommo_api.get_leads()
            activities = kommo_api.get_activities()
            
            if not brokers.empty and not leads.empty and not activities.empty:
                sync_manager.sync_data(brokers=brokers, leads=leads, activities=activities)
                # Atualiza os pontos após sincronização
                supabase.update_broker_points(brokers=brokers, leads=leads, activities=activities)
                return jsonify({"status": "success", "message": "Forced sync and points update completed successfully"})
            else:
                return jsonify({"status": "error", "message": "Failed to fetch data"}), 400
        else:
            return jsonify({"status": "error", "message": "Force parameter is required"}), 400
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
