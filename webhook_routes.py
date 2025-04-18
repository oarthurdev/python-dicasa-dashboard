# Assuming this is webhook_routes.py
from flask import Blueprint, jsonify

# ... other imports ...

webhook_bp = Blueprint('webhook', __name__)

# ... other routes ...


#Removed health check endpoint
# @webhook_bp.route('/health', methods=['GET'])
# def health_check():
#     """Health check endpoint that verifies API and database connectivity"""
#     try:
#         # Check API connection
#         kommo_api._make_request("users", params={"limit": 1})

#         # Check database connection
#         supabase.client.table("brokers").select("id").limit(1).execute()

#         status = {
#             "status": "healthy",
#             "api": "connected",
#             "database": "connected",
#             "last_sync": {
#                 resource: sync_manager.last_sync[resource].isoformat() 
#                 if sync_manager.last_sync[resource] else None
#                 for resource in sync_manager.last_sync
#             }
#         }
#         return jsonify(status), 200
#     except Exception as e:
#         return jsonify({
#             "status": "unhealthy",
#             # ... error handling ...
#         }), 500

# ... rest of webhook_routes.py ...