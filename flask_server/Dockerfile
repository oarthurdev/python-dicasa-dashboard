FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir flask

EXPOSE 5000

CMD ["python", "main.py"]

