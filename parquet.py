import os
import pandas as pd
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import requests
import dotenv

# Charger les variables d'environnement depuis le fichier .env
dotenv.load_dotenv()

# Configuration
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
DIRECTORY = os.getenv("DIRECTORY")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
SAS_TOKEN = os.getenv("SAS_TOKEN")

# Fonction pour télécharger un blob en recréant la structure des répertoires
def download_blob(blob_url, blob_name):
    local_path = os.path.join("downloads", "product_eval", blob_name)  # Répertoire local de base : "downloads"
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
            sas_token = SAS_TOKEN
            blob_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{blob.name}?{sas_token}"
            print(f"Téléchargement du fichier : {blob.name}")
            download_blob(blob_url, blob.name)  # Transmet le chemin complet du blob

# Fonction pour traiter un fichier Parquet
def process_parquet_file(parquet_file):
    try:
        # Lire le fichier Parquet
        table = pq.read_table(parquet_file)
        df = table.to_pandas()

        # Liste pour accumuler les métadonnées
        metadata_list = []

        # Répertoire spécifique pour ce fichier Parquet
        parquet_name = os.path.splitext(os.path.basename(parquet_file))[0]
        image_dir = os.path.join("downloads", "product_eval", "images", parquet_name)
        os.makedirs(image_dir, exist_ok=True)

        # Parcourir les lignes pour extraire les images et leurs métadonnées
        for index, row in df.iterrows():
            if "image" in row:
                content = row["image"]

                if isinstance(content, dict):
                    for key, value in content.items():
                        if isinstance(value, bytes):
                            # Sauvegarder l'image
                            image_path = os.path.join(image_dir, f"image_{index}_{key}.png")
                            with open(image_path, "wb") as img_file:
                                img_file.write(value)
                            print(f"Ligne {index}: Image sauvegardée avec succès à {image_path}")

                            # Ajouter les métadonnées associées à la liste
                            metadata_list.append({
                                "line_index": index,
                                "key": key,
                                "title": row.get("title", "Sans titre"),
                                "image_path": image_path,
                            })

        # Sauvegarder les métadonnées directement en CSV
        if metadata_list:
            metadata_dir = os.path.join("downloads", "product_eval", "metadata")
            os.makedirs(metadata_dir, exist_ok=True)
            csv_file = os.path.join(metadata_dir, f"{parquet_name}_metadata.csv")
            pd.DataFrame(metadata_list).to_csv(csv_file, index=False, encoding="utf-8-sig")
            print(f"Métadonnées sauvegardées directement en CSV à : {csv_file}")

        # Supprimer le fichier Parquet après traitement
        os.remove(parquet_file)
        print(f"Fichier Parquet supprimé : {parquet_file}")

    except Exception as e:
        print(f"Erreur lors du traitement du fichier {parquet_file}: {e}")

# Fonction pour supprimer un dossier vide
def remove_empty_directories(path):
    # Vérifier si le dossier est vide
    if os.path.isdir(path) and not os.listdir(path):
        os.rmdir(path)
        print(f"Dossier vide supprimé : {path}")
    elif os.path.isdir(path):
        # Si le dossier n'est pas vide, parcourir ses sous-dossiers
        for subdir in os.listdir(path):
            remove_empty_directories(os.path.join(path, subdir))

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

        # Suppression des dossiers vides
        remove_empty_directories(os.path.join("downloads", "product_eval"))

    except Exception as e:
        print(f"Erreur dans la fonction principale : {e}")

if __name__ == "__main__":
    main()
