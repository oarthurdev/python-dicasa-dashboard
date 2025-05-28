import os
import uvicorn
from api_server import app
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    uvicorn.run(app, host="0.0.0.0", port=5002)