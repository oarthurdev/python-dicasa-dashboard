# Usa imagem oficial do Python
FROM python:3.11-slim

# Define diretório de trabalho dentro do container
WORKDIR /app

# Copia o arquivo de dependências e instala os pacotes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos da aplicação
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 8501

# Comando para rodar o app Streamlit
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]

