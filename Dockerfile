FROM python:3.9

# Créer un répertoire spécifique pour l'application
WORKDIR /

# Copier les fichiers de dépendances et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code de l'application
COPY . .

# Exposer le port utilisé par Uvicorn
EXPOSE 8000

# Commande pour démarrer l'application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
