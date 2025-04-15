from flask import Flask
from webhook_routes import webhook_bp
import os
import logging

def create_flask_app():
    app = Flask(__name__)
    app.register_blueprint(webhook_bp)
    return app

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    app = create_flask_app()
    port = int(os.environ.get("PORT", 5000))

    logging.info(f"Running Flask server on port {port}")
    app.run(host="0.0.0.0", port=port)

