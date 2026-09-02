import os
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


def get_blob_service_client():
    # Récupération des credentials pour Azure Blob Storage
    load_dotenv("./.env")
    AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")
    AZURE_CONTAINER_KEY = os.getenv("AZURE_CONTAINER_KEY")
    # Connection ADLS
    account_url = f"https://{AZURE_CONTAINER}.blob.core.windows.net"
    credential = AZURE_CONTAINER_KEY
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url, credential=credential)
    return blob_service_client


# Fonction de telechargement d'un blob
# Telecharge un blob depuis Azure Blob Storage vers un fichier local
# container_name: nom du container dans lequel se trouve le blob
# file_name: nom du fichier a telecharger
# path: chemin du dossier dans lequel le fichier va etre telecharger
# mode = wb pour ecrire en mode binaire (sans transformation et avec ecrasement si deja existant)
def download_blob_to_file(container_name, file_name, path):
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=file_name
    )
    with open(file=os.path.join(path, file_name), mode="wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())


# Création d'une session Spark
def get_spark_session():
    spark = SparkSession.builder.appName("TradeCorp ETL").getOrCreate()
    return spark
