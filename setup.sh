#!/bin/bash

echo "🚀 Criando ambiente virtual..."
python3 -m venv venv

echo "✅ Ativando ambiente virtual..."
source venv/bin/activate

echo "⬆️ Atualizando pip..."
pip install --upgrade pip

echo "📦 Instalando dependências..."
pip install -r requirements.txt

echo "🎉 Ambiente pronto!"
echo "Para ativar novamente depois, use: source venv/bin/activate"

