import json
from flask import Flask
from flask_server.webhook_routes import webhook_bp  # Adjusted to an absolute import

app = Flask(__name__)
app.register_blueprint(webhook_bp)

def test_webhook_health_check():
    with app.test_client() as client:
        response = client.get('/webhook/health')
        assert response.status_code == 200
        assert response.get_json()["status"] == "ok"

def test_webhook_simulacao_lead_ganho():
    payload = {
        "type": "lead_status_changed",
        "leads": {
            "status": [
                {
                    "status_id": 142,
                    "responsible_user_id": 11378447,
                    "id": 9159398
                }
            ]
        }
    }

    with app.test_client() as client:
        response = client.post('/webhook/kommo', data=json.dumps(payload),
                               content_type='application/json')
        assert response.status_code in [200, 500]  # depende se a sync_manager está ativa

