import os
import pandas as pd
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import base64
import requests
import json
import shutil
import dotenv

# Charger les variables d'environnement depuis le fichier .env
dotenv.load_dotenv()

# Configuration
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
DIRECTORY = os.getenv("DIRECTORY")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# Fonction pour générer un SAS token
def generate_sas_token(container_name, blob_name):
    sas_token = generate_blob_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name=container_name,
        blob_name=blob_name,
        account_key=STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)  # Expiration dans 1 heure
    )
    return sas_token

# Fonction pour télécharger un blob en recréant la structure des répertoires
def download_blob(blob_url, blob_name):
    # Créer la structure des répertoires en local
    local_path = os.path.join("downloads", blob_name)  # Répertoire local de base : "downloads"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Télécharger et sauvegarder le fichier
    response = requests.get(blob_url)
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Fichier téléchargé : {local_path}")

# Fonction pour télécharger les fichiers Parquet
def download_parquet_files(blob_service_client):
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    blobs = container_client.list_blobs(name_starts_with=DIRECTORY)

    for blob in blobs:
        if blob.name.endswith(".parquet"):
            sas_token = generate_sas_token(AZURE_CONTAINER_NAME, blob.name)
            blob_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{blob.name}?{sas_token}"
            print(f"Téléchargement du fichier : {blob.name}")
            download_blob(blob_url, blob.name)  # Transmet le chemin complet du blob


def process_parquet_file(parquet_file):
    try:
        # Lire le fichier Parquet
        table = pq.read_table(parquet_file)
        df = table.to_pandas()

        # Parcourir les lignes pour extraire les images et leurs métadonnées
        for index, row in df.iterrows():
            if "image" in row:
                content = row["image"]

                if content is None:
                    print(f"Ligne {index}: image est None")
                elif isinstance(content, dict):
                    # Vérifier si le dict contient des données en bytes
                    for key, value in content.items():
                        if isinstance(value, bytes):
                            print(f"Ligne {index}: clé '{key}' contient des données en bytes (taille {len(value)} bytes)")

                            # Sauvegarder l'image
                            image_dir = os.path.join("downloads", "images")
                            os.makedirs(image_dir, exist_ok=True)
                            image_path = os.path.join(image_dir, f"image_{index}.png")
                            try:
                                with open(image_path, "wb") as img_file:
                                    img_file.write(value)
                                print(f"Ligne {index}: Image sauvegardée avec succès à {image_path}")
                            except Exception as e:
                                print(f"Ligne {index}: Erreur lors de la sauvegarde de l'image - {e}")
                            
                            # Sauvegarder les métadonnées associées
                            metadata = {
                                "title": row.get("title", "Sans titre"),  # Récupère le titre ou "Sans titre" par défaut
                                "path": image_path
                            }
                            metadata_path = os.path.join(image_dir, f"image_{index}_metadata.json")
                            try:
                                with open(metadata_path, "w") as meta_file:
                                    json.dump(metadata, meta_file, ensure_ascii=False, indent=4)
                                print(f"Ligne {index}: Métadonnées sauvegardées avec succès à {metadata_path}")
                            except Exception as e:
                                print(f"Ligne {index}: Erreur lors de la sauvegarde des métadonnées - {e}")
                        else:
                            print(f"Ligne {index}: clé '{key}' ne contient pas de données en bytes (type {type(value)})")
                else:
                    print(f"Ligne {index}: image est de type {type(content)} avec valeur non supportée")
            else:
                print(f"Ligne {index}: colonne image non trouvée")

    except Exception as e:
        print(f"Erreur lors du traitement du fichier {parquet_file}: {e}")


# Fonction principale
def main():
    try:
        # Initialisation du client Blob
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
            credential=STORAGE_ACCOUNT_KEY
        )

        # Téléchargement des fichiers Parquet
        download_parquet_files(blob_service_client)

        # Traitement des fichiers téléchargés
        local_downloads_dir = "downloads"
        for root, dirs, files in os.walk(local_downloads_dir):
            for file in files:
                if file.endswith(".parquet"):
                    file_path = os.path.join(root, file)
                    print(f"Traitement du fichier : {file_path}")
                    process_parquet_file(file_path)

    except Exception as e:
        print(f"Erreur dans la fonction principale : {e}")

if __name__ == "__main__":
    main()