from flask import Flask, jsonify, request
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

SYNC_INTERVAL_MINUTES = 90  # Aumentado para 90 minutos devido ao volume total de dados


def load_companies():
    """Load all companies from kommo_config"""
    try:
        result = supabase.client.table("kommo_config").select("*").execute()
        return result.data if result.data else []
    except Exception as e:
        logger.error(f"Error loading companies: {e}")
        return []


def sync_data(company_id):
    """Execute sync function once for a specific company with incremental updates"""
    try:
        local_supabase = SupabaseClient()

        local_supabase.check_config_changes();
        
        company_result = local_supabase.client.table("companies").select(
            "subdomain").eq("id", company_id).execute()
        if not company_result.data:
            logger.error(f"Company {company_id} not found")
            return

        subdomain = company_result.data[0]['subdomain']
        logger.info(f"Starting incremental sync for company {company_id} (subdomain: {subdomain}) - syncing ALL data")

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

        # Buscar dados da API
        brokers = kommo_api.get_users()
        leads = kommo_api.get_leads()
        activities = kommo_api.get_activities()

        # Adicionar company_id aos DataFrames
        if not brokers.empty:
            brokers['company_id'] = company_id
        if not leads.empty:
            leads['company_id'] = company_id
        if not activities.empty:
            activities['company_id'] = company_id

        # Sincronização incremental - apenas dados alterados
        changes_detected = sync_manager.sync_data_incremental(
            brokers=brokers, 
            leads=leads, 
            activities=activities, 
            company_id=company_id
        )

        if changes_detected['brokers'] or changes_detected['leads'] or changes_detected['activities']:
            logger.info(f"Changes detected: {changes_detected}")
            
            # Atualizar pontos apenas se houve mudanças relevantes
            if not brokers.empty and (changes_detected['brokers'] or changes_detected['leads'] or changes_detected['activities']):
                broker_data = brokers[(brokers['cargo'] == 'Corretor') & (brokers['company_id'] == company_id)].copy()
                if not broker_data.empty:
                    local_supabase.update_broker_points(
                        brokers=broker_data,
                        leads=leads,
                        activities=activities,
                        company_id=company_id
                    )
                else:
                    logger.warning("No brokers with 'Corretor' role found for this company")
        else:
            logger.info(f"No changes detected for company {company_id}")

        threads_status[company_id].update({
            'status': 'waiting',
            'last_sync': datetime.now(),
            'next_sync': datetime.now() + timedelta(minutes=SYNC_INTERVAL_MINUTES)
        })

        logger.info(f"Incremental sync completed for company {company_id}.")

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

        logger.info("Starting sync cycle for all companies - syncing ALL data with safe pagination")
        threads = []

        for company in COMPANY_LIST:
            company_id = str(company['company_id'])
            t = threading.Thread(target=sync_data, args=(company_id,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        logger.info(f"All companies have completed sync (ALL data). Sleeping {SYNC_INTERVAL_MINUTES} minutes...")
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


def validate_webhook_data(webhook_data):
    """Validate webhook data structure and content"""
    try:
        if not isinstance(webhook_data, dict):
            return False
            
        # Basic validation - check if required fields exist
        required_fields = ['id']
        if not any(field in webhook_data for field in required_fields):
            logger.warning("Webhook data missing required fields")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Error validating webhook data: {str(e)}")
        return False


@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle Kommo webhook requests - process all incoming messages"""
    try:
        # Get raw request data for detailed logging
        raw_data = request.get_data(as_text=True)
        content_type = request.content_type
        headers = dict(request.headers)
        
        logger.info(f"=== WEBHOOK RECEIVED ===")
        logger.info(f"Content-Type: {content_type}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Raw data (first 500 chars): {raw_data[:500]}")
        
        # Handle different content types
        payload = None
        
        if content_type and 'application/json' in content_type:
            payload = request.get_json()
            logger.info("Parsed as JSON from content-type")
        elif content_type and 'application/x-www-form-urlencoded' in content_type:
            # Get form data
            form_data = request.form.to_dict()
            logger.info(f"Form data keys: {list(form_data.keys())}")
            logger.info(f"Form data sample: {dict(list(form_data.items())[:5])}")
            
            if 'payload' in form_data:
                import json
                payload = json.loads(form_data['payload'])
                logger.info("Parsed JSON from 'payload' form field")
            else:
                # Kommo sends data in flat format, convert to nested structure
                payload = form_data
                logger.info("Using form data directly as payload")
        else:
            # Try to parse as JSON anyway (some webhooks don't set proper content-type)
            try:
                import json
                payload = json.loads(raw_data)
                logger.info("Parsed as JSON from raw data")
            except Exception as json_err:
                logger.error(f"Failed to parse as JSON: {json_err}")
                payload = None
        
        if not payload:
            logger.error(f"Could not parse webhook payload from any method")
            return jsonify({'status': 'error', 'message': 'Could not parse payload'}), 400
        
        logger.info(f"Parsed payload structure: {type(payload)}")
        logger.info(f"Payload keys (first 10): {list(payload.keys())[:10] if isinstance(payload, dict) else 'Not a dict'}")
        
        # Detect webhook type and format
        webhook_type = None
        webhook_data = {}
        
        # Check if this is Kommo flat format (form data with keys like "account[subdomain]", "message[add][0][id]")
        if isinstance(payload, dict) and any('[' in key and ']' in key for key in payload.keys()):
            logger.info("Detected Kommo flat format")
            
            # Parse flat format keys to extract webhook type and data
            for key, value in payload.items():
                if '[' not in key:
                    continue
                    
                # Extract the main type (account, message, leads, etc.)
                main_type = key.split('[')[0]
                if not webhook_type:
                    webhook_type = main_type
                
                # Skip account-level keys for now, focus on the actual data
                if main_type == 'account':
                    continue
                
                # Parse nested structure from flat keys
                # Example: "message[add][0][id]" -> message.add[0].id
                parts = key.replace(main_type, '').strip('[]').split('][')
                if len(parts) >= 3:  # [add][0][field]
                    action = parts[0]  # add, update, delete
                    index = int(parts[1]) if parts[1].isdigit() else 0
                    field = parts[2]
                    
                    # Handle nested fields like author[id]
                    if len(parts) > 3:
                        nested_field = parts[3]
                        field = f"{field}.{nested_field}"
                    
                    # Initialize structure
                    if action not in webhook_data:
                        webhook_data[action] = []
                    
                    # Ensure we have enough objects in the array
                    while len(webhook_data[action]) <= index:
                        webhook_data[action].append({})
                    
                    # Set the field value, handling nested fields
                    if '.' in field:
                        main_field, sub_field = field.split('.', 1)
                        if main_field not in webhook_data[action][index]:
                            webhook_data[action][index][main_field] = {}
                        webhook_data[action][index][main_field][sub_field] = value
                    else:
                        webhook_data[action][index][field] = value
            
            logger.info(f"Parsed webhook type: {webhook_type}")
            logger.info(f"Parsed webhook data structure: {list(webhook_data.keys())}")
            
        else:
            # Standard format
            webhook_type = next(iter(payload.keys()))
            webhook_data = payload[webhook_type]
            logger.info(f"Standard format - Webhook type: {webhook_type}")
        
        if not webhook_type:
            logger.error("Could not determine webhook type")
            return jsonify({'status': 'error', 'message': 'Could not determine webhook type'}), 400
        
        logger.info(f"Final webhook type: {webhook_type}")
        logger.info(f"Final webhook data: {webhook_data}")
        
        # Extract data objects
        data_objects = []
        
        if isinstance(webhook_data, dict):
            if 'add' in webhook_data:
                data_objects = webhook_data['add']
                logger.info(f"Found 'add' data with {len(data_objects)} objects")
            elif 'update' in webhook_data:
                data_objects = webhook_data['update']
                logger.info(f"Found 'update' data with {len(data_objects)} objects")
            elif 'delete' in webhook_data:
                data_objects = webhook_data['delete']
                logger.info(f"Found 'delete' data with {len(data_objects)} objects")
            else:
                # Some webhooks might send data directly
                if isinstance(webhook_data, list):
                    data_objects = webhook_data
                    logger.info(f"Using webhook data directly as object list with {len(data_objects)} objects")
                elif isinstance(webhook_data, dict):
                    data_objects = [webhook_data]
                    logger.info("Using webhook data directly as single object")
        elif isinstance(webhook_data, list):
            data_objects = webhook_data
            logger.info(f"Webhook data is already a list with {len(data_objects)} objects")
        
        if not data_objects:
            logger.warning(f"No data objects found in webhook. Saving raw webhook for debugging.")
            # Still save the webhook for debugging purposes
            webhook_record = {
                'webhook_type': webhook_type,
                'payload_id': None,
                'raw_payload': payload
            }
            
            try:
                result = supabase.client.table("from_webhook").insert(webhook_record).execute()
                logger.info("Empty webhook saved for debugging")
            except Exception as db_err:
                logger.error(f"Failed to save empty webhook: {db_err}")
                
            return jsonify({'status': 'success', 'message': 'No data to process, but webhook logged'})
        
        # Process the first object
        first_object = data_objects[0] if isinstance(data_objects, list) else data_objects
        logger.info(f"Processing first object: {first_object}")
        
        # Validate webhook data structure
        if first_object and not validate_webhook_data(first_object):
            logger.warning(f"Invalid webhook data structure, skipping processing")
            return jsonify({'status': 'success', 'message': 'Invalid data structure, ignored'})
        else:
            logger.info(f"Webhook data validated successfully, continuing processing")
        
        # Extract fields for from_webhook table
        webhook_record = {
            'webhook_type': webhook_type,
            'payload_id': first_object.get('id') if isinstance(first_object, dict) else None,
            'chat_id': first_object.get('chat_id') if isinstance(first_object, dict) else None,
            'talk_id': first_object.get('talk_id') if isinstance(first_object, dict) else None,
            'contact_id': first_object.get('contact_id') if isinstance(first_object, dict) else None,
            'text': first_object.get('text') if isinstance(first_object, dict) else None,
            'created_at': first_object.get('created_at') if isinstance(first_object, dict) else None,
            'element_type': first_object.get('element_type') if isinstance(first_object, dict) else None,
            'entity_type': first_object.get('entity_type') if isinstance(first_object, dict) else None,
            'element_id': first_object.get('element_id') if isinstance(first_object, dict) else None,
            'entity_id': first_object.get('entity_id') if isinstance(first_object, dict) else None,
            'message_type': first_object.get('type') if isinstance(first_object, dict) else None,
            'origin': first_object.get('origin') if isinstance(first_object, dict) else None,
            'raw_payload': payload
        }
        
        # Extract author information if present
        if isinstance(first_object, dict):
            author = first_object.get('author', {})
            if author and isinstance(author, dict):
                webhook_record.update({
                    'author_id': author.get('id'),
                    'author_type': author.get('type'),
                    'author_name': author.get('name'),
                    'author_avatar_url': author.get('avatar_url')
                })
        
        logger.info(f"Prepared webhook record for database:")
        for key, value in webhook_record.items():
            if key != 'raw_payload':  # Don't log the full payload again
                logger.info(f"  {key}: {value}")
        
        # Link message to broker before saving
        linked_record = supabase.link_webhook_message_to_broker(webhook_record)
        
        # Save to database
        result = supabase.client.table("from_webhook").insert(linked_record).execute()
        
        if hasattr(result, "error") and result.error:
            logger.error(f"Error saving webhook to database: {result.error}")
            return jsonify({'status': 'error', 'message': 'Database error'}), 500
        
        logger.info(f"Webhook {webhook_type} saved successfully")
        if linked_record.get('broker_id'):
            logger.info(f"Message linked to broker: {linked_record['broker_id']}")
        if linked_record.get('lead_id'):
            logger.info(f"Message linked to lead: {linked_record['lead_id']}")
        logger.info(f"=== WEBHOOK PROCESSING COMPLETE ===")
        return jsonify({'status': 'success'})
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        logger.error(f"Exception type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    # Ensure webhook table exists
    supabase.ensure_webhook_table()
    
    sync_thread = threading.Thread(target=sync_cycle, daemon=True)
    sync_thread.start()
    app.run(host='0.0.0.0', port=5002)
