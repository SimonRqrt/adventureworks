import os
import requests
import zipfile
import tarfile
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import dotenv

# Charger les variables d'environnement depuis le fichier .env
dotenv.load_dotenv()

# Configuration
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
ZIP_DIRECTORY = os.getenv("ZIP_DIRECTORY")  # Dossier contenant le fichier ZIP
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
SAS_TOKEN = os.getenv("SAS_TOKEN")

# Fonction pour télécharger un blob
def download_blob(blob_url, blob_name, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    response = requests.get(blob_url, stream=True)
    with open(local_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):  # Lire par blocs de 1 Mo
            file.write(chunk)
    print(f"Fichier téléchargé : {local_path}")

# Fonction pour extraire un fichier ZIP
def extract_zip(zip_file, extract_to):
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Contenu extrait dans : {extract_to}")

# Fonction pour extraire un fichier TGZ
def extract_tgz(tgz_file, extract_to):
    os.makedirs(extract_to, exist_ok=True)
    with tarfile.open(tgz_file, "r:gz") as tar_ref:
        tar_ref.extractall(extract_to)
    print(f"Contenu TGZ extrait dans : {extract_to}")

# Fonction pour renommer les fichiers CSV à partir du nom du fichier ZIP
def rename_csv_files(extract_dir, zip_name):
    for file_name in os.listdir(extract_dir):
        if file_name == "train.csv":
            os.rename(os.path.join(extract_dir, file_name), os.path.join(extract_dir, f"{zip_name}_train.csv"))
        elif file_name == "test.csv":
            os.rename(os.path.join(extract_dir, file_name), os.path.join(extract_dir, f"{zip_name}_test.csv"))
    print(f"Fichiers CSV renommés avec le préfixe du fichier ZIP : {zip_name}")

# Fonction pour traiter les fichiers ZIP
def process_zip(blob_service_client):
    try:
        # Localisation du fichier ZIP
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        blobs = container_client.list_blobs(name_starts_with=ZIP_DIRECTORY)

        for blob in blobs:
            if blob.name.endswith(".zip"):
                print(f"Fichier ZIP détecté : {blob.name}")
                
                # Générer le SAS token
                sas_token = SAS_TOKEN
                blob_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_CONTAINER_NAME}/{blob.name}?{sas_token}"
                
                # Télécharger le fichier ZIP
                local_zip_path = os.path.join("downloads", "machine_learning", os.path.basename(blob.name))
                download_blob(blob_url, blob.name, local_zip_path)
                
                # Extraire le contenu du fichier ZIP
                extract_dir = os.path.join("downloads", "machine_learning", os.path.splitext(os.path.basename(blob.name))[0])
                extract_zip(local_zip_path, extract_dir)
                
                # Supprimer le fichier ZIP après extraction
                os.remove(local_zip_path)
                print(f"Fichier ZIP supprimé : {local_zip_path}")

                # Recherche et extraction du fichier TGZ dans le répertoire extrait
                for file_name in os.listdir(extract_dir):
                    if file_name.endswith(".tgz"):
                        tgz_path = os.path.join(extract_dir, file_name)
                        print(f"Fichier TGZ trouvé : {file_name}")
                        tgz_extract_dir = os.path.join(extract_dir, os.path.splitext(file_name)[0])
                        extract_tgz(tgz_path, tgz_extract_dir)

                        # Renommer les fichiers CSV à l'intérieur du TGZ
                        rename_csv_files(tgz_extract_dir, os.path.basename(blob.name))
                        # Supprimer le fichier TGZ après extraction
                        os.remove(tgz_path)
                        print(f"Fichier TGZ supprimé : {tgz_path}")

    except Exception as e:
        print(f"Erreur lors du traitement du fichier ZIP : {e}")

# Fonction principale
def main():
    try:
        # Initialisation du client Blob
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/",
            credential=STORAGE_ACCOUNT_KEY
        )

        # Traiter les fichiers ZIP
        process_zip(blob_service_client)

    except Exception as e:
        print(f"Erreur dans la fonction principale : {e}")

if __name__ == "__main__":
    main()
