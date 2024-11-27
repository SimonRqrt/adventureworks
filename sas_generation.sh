#!/bin/bash

# Définir le chemin absolu du répertoire du script
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ENV_FILE="$SCRIPT_DIR/.env"

# Charger les variables d'environnement
if [[ -f "$ENV_FILE" ]]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "Erreur : Fichier .env introuvable."
    exit 1
fi

# Vérifier les variables nécessaires
if [[ -z "$STORAGE_ACCOUNT_NAME" || -z "$STORAGE_ACCOUNT_KEY" ]]; then
    echo "Erreur : Les variables STORAGE_ACCOUNT_NAME ou STORAGE_ACCOUNT_KEY sont absentes."
    exit 1
fi

# Définir les variables Azure
STORAGE_ACCOUNT_NAME=${STORAGE_ACCOUNT_NAME}
CONTAINER_NAME="data"
EXPIRY=$(date -u -v+1H '+%Y-%m-%dT%H:%MZ')  # Expire dans 1 heure (compatible macOS)
PERMISSIONS="rl"

# Générer le SAS token
echo "Génération du SAS token"
SAS_TOKEN=$(az storage container generate-sas \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --name "$CONTAINER_NAME" \
    --permissions "$PERMISSIONS" \
    --expiry "$EXPIRY" \
    --account-key "$STORAGE_ACCOUNT_KEY" \
    --output tsv 2>&1)

# Vérifier si le SAS token a été généré
if [[ "$SAS_TOKEN" == *"ERROR"* ]]; then
    echo "Erreur : Impossible de générer le SAS token."
    echo "$SAS_TOKEN"
    exit 1
fi

# Ajouter ou mettre à jour le SAS_TOKEN dans le .env
if grep -q '^SAS_TOKEN=' "$ENV_FILE"; then
    sed -i '' "s|^SAS_TOKEN=.*|SAS_TOKEN=\"$SAS_TOKEN\"|" "$ENV_FILE"
else
    echo -e "\nSAS_TOKEN=\"$SAS_TOKEN\"" >> "$ENV_FILE"
fi

echo "Token SAS ajouté au fichier .env avec succès."
echo "Fichier .env mis à jour :"
cat "$ENV_FILE"
