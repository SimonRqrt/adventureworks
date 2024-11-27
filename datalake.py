import os
import base64
from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions, ContainerClient
import pandas as pd
from PIL import Image
from io import BytesIO
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

ACCOUNT_NAME = os.getenv("AZURE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
FOLDER_NAME = os.getenv("AZURE_FOLDER_NAME")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")  # Dossier de sortie par défaut

# Génération du SAS token
def generate_sas_token(container_name):
    """Génère un SAS token pour accéder à un container."""
    sas_token = generate_container_sas(
        account_name=ACCOUNT_NAME,
        container_name=container_name,
        account_key=ACCOUNT_KEY,
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=1)
    )
    return f"https://{ACCOUNT_NAME}.blob.core.windows.net/{container_name}?{sas_token}"

# Liste les fichiers dans le container Azure
def list_files_in_container(container_url, folder_name=None):
    try:
        blob_service_client = ContainerClient.from_container_url(container_url)
        blobs = blob_service_client.list_blobs(name_starts_with=folder_name)
        return [blob.name for blob in blobs]
    except Exception as e:
        print(f"Failed to list files in container: {e}")
        raise


# Lecture des fichiers Parquet
def read_parquet_file(container_url, file_name):
    """Lit un fichier Parquet depuis Azure Blob Storage."""
    file_url = f"{container_url}/{file_name}"
    df = pd.read_parquet(file_url, engine='pyarrow')
    return df

# Décodage et sauvegarde des images encodées
def decode_and_save_image(encoded_image, output_dir, image_name):
    """Décode une image encodée en base64 et la sauvegarde au format PNG."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    image_data = base64.b64decode(encoded_image)
    image = Image.open(BytesIO(image_data))
    image.save(os.path.join(output_dir, f"{image_name}.png"))

# Sauvegarde des métadonnées textuelles
def save_metadata_to_csv(df, output_file):
    """Sauvegarde les métadonnées textuelles dans un fichier CSV."""
    df.to_csv(output_file, index=False)

# Processus principal
def process_parquet_data(container_name, folder_name, output_dir):
    container_url = generate_sas_token(container_name)
    file_list = list_files_in_container(container_url, folder_name)

    for file_name in file_list:
        try:
            print(f"Processing file: {file_name}")
            df = read_parquet_file(container_url, file_name)
            
            # Extraction des images
            for index, row in df.iterrows():
                image_name = f"{file_name.split('.')[0]}_{index}"
                decode_and_save_image(row['image_column'], output_dir, image_name)
            
            # Sauvegarde des métadonnées
            metadata_file = os.path.join(output_dir, "metadata.csv")
            save_metadata_to_csv(df.drop(columns=['image_column']), metadata_file)
        except Exception as e:
            print(f"Failed to process {file_name}: {e}")

# Exécution du script
if __name__ == "__main__":
    process_parquet_data(
        container_name=CONTAINER_NAME,
        folder_name=FOLDER_NAME,
        output_dir=OUTPUT_DIR
    )
