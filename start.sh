#!/bin/bash

# Définir le chemin absolu du répertoire du script
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# Charger les variables d'environnement
ENV_FILE="$SCRIPT_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "Erreur : Fichier .env introuvable."
    exit 1
fi

# Étape 1 : Installer les dépendances nécessaires
echo "Étape 1 : Installation des dépendances..."
bash "$SCRIPT_DIR/install.sh"
if [[ $? -ne 0 ]]; then
    echo "Erreur lors de l'installation des dépendances."
    exit 1
fi

# Étape 2 : Générer le SAS token
echo "Étape 2 : Génération du SAS token..."
bash "$SCRIPT_DIR/sas_generation.sh"
if [[ $? -ne 0 ]]; then
    echo "Erreur lors de la génération du SAS token."
    exit 1
fi

# Étape 3 : Exporter les données de la base de données vers des fichiers CSV
echo "Étape 3 : Export des données de la base de données..."
python3 "$SCRIPT_DIR/export_db.py"
if [[ $? -ne 0 ]]; then
    echo "Erreur lors de l'export des données de la base de données."
    exit 1
fi

# Étape 4 : Télécharger et traiter les fichiers Parquet
echo "Étape 4 : Téléchargement et traitement des fichiers Parquet..."
python3 "$SCRIPT_DIR/parquet.py"
if [[ $? -ne 0 ]]; then
    echo "Erreur lors du traitement des fichiers Parquet."
    exit 1
fi

# Étape 5 : Finaliser en compressant les fichiers CSV en archive ZIP
echo "Étape 5 : Compression des fichiers CSV en ZIP..."
python3 "$SCRIPT_DIR/zip_csv.py"
if [[ $? -ne 0 ]]; then
    echo "Erreur lors de la compression des fichiers."
    exit 1
fi

# Étape 6 : Extraire les blobs Azure
echo "Étape 6 : Extraction des blobs Azure..."
python3 "$SCRIPT_DIR/extract_blob.py"
if [[ $? -ne 0 ]]; then
    echo "Erreur lors de l'extraction des blobs."
    exit 1
fi

echo "Pipeline exécuté avec succès."
