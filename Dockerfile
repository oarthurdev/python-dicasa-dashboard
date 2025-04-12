# Usa uma imagem Python slim
FROM python:3.11-slim

# Define o diretório de trabalho
WORKDIR /app

# Copia e instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos
COPY . .

# Expõe a porta padrão do Streamlit
EXPOSE 5000

# Comando para rodar o app no container
CMD ["streamlit", "run", "app.py", "--server.port=5000", "--server.address=0.0.0.0"]

