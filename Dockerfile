FROM python:3.9-slim

WORKDIR /app

# Instala somente o necessário (ajuste os pacotes se precisar compilar dependências C)
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copia apenas o requirements para aproveitar o cache do Docker
COPY requirements.txt .

# Atualiza o pip e instala dependências
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copia o restante da aplicação
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Healthcheck com parâmetros para evitar falsos negativos
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Comando de entrada
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]