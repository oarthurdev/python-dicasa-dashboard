FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r flask_server/requirements.txt

CMD ["python", "flask_server/main.py"]

