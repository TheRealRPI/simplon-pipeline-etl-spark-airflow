import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_azure_info():
    logger.info("Récupération des informations Azure")
    try:
        # Remonter de src/ vers la racine du projet
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env_path = os.path.join(project_root, ".env")
        load_dotenv(env_path)
        azure_storage = os.getenv("AZURE_STORAGE")
        azure_key = os.getenv("AZURE_KEY")
        return azure_storage, azure_key
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des informations Azure: {e}")
        raise
    finally:
        logger.info("Récupération des informations Azure terminée")


def get_blob_service_client():
    logger.info("Création du client BlobServiceClient")
    try:
        # Récupération des credentials pour Azure Blob Storage
        storage, key = get_azure_info()
        # Connection ADLS
        account_url = f"https://{storage}.blob.core.windows.net"
        credential = key
        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient(account_url, credential=credential)
        return blob_service_client
    except Exception as e:
        logger.error(f"Erreur lors de la création du client BlobServiceClient: {e}")
        raise
    finally:
        logger.info("Création du client BlobServiceClient terminée")


# Fonction de telechargement d'un blob
# Telecharge un blob depuis Azure Blob Storage vers un fichier local
# container_name: nom du container dans lequel se trouve le blob
# file_name: nom du fichier a telecharger
# path: chemin du dossier dans lequel le fichier va etre telecharger
# mode = wb pour ecrire en mode binaire (sans transformation et avec ecrasement si deja existant)
def download_blob_from_file(container_name, file_name, path):
    logger.info(
        f"Téléchargement du blob {file_name} depuis le container {container_name}"
    )
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=file_name
        )
        full_path = os.path.join(path, file_name)
        os.makedirs(
            os.path.dirname(full_path), exist_ok=True
        )  # Créer les dossiers parents
        with open(file=full_path, mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement du blob {file_name}: {e}")
        raise
    finally:
        logger.info(f"Téléchargement du blob {file_name} terminé")


def upload_blob_file(container_name, file_name, local_path, distant_path=None):
    logger.info(f"Upload du blob {file_name} vers le container {container_name}")
    try:
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
        blob_name = (
            file_name
            if distant_path is None
            else distant_path + "/" + file_name  # Ajout du chemin
        )
        with open(file=os.path.join(local_path, file_name), mode="rb") as data:
            blob_client = container_client.upload_blob(
                name=blob_name, data=data, overwrite=True
            )
        return blob_client
    except Exception as e:
        logger.error(f"Erreur lors de l'upload du blob {file_name}: {e}")
        raise
    finally:
        logger.info(f"Upload du blob {file_name} terminé")
