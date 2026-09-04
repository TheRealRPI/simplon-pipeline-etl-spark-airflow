from utils import download_blob_from_file
import logging

logger = logging.getLogger(__name__)


# Telechargement des blobs dans un dossier local depuis une liste
def download_blobs_from_list(blob_list, container_name, path):
    logger.info(
        f"Début du téléchargement de {len(blob_list)} blobs depuis le conteneur '{container_name}' vers le chemin '{path}'"
    )
    try:
        for blob_path in blob_list:
            try:
                logger.info(f"Téléchargement de {blob_path}...")
                download_blob_from_file(container_name, blob_path, path)
                logger.info(f"Téléchargement de {blob_path} terminé avec succès")
            except Exception as e:
                logger.error(
                    f"Échec du téléchargement de {blob_path} : {str(e)}", exc_info=True
                )
                raise
    except Exception as e:
        logger.error(
            f"Erreur lors du processus de téléchargement des blobs : {str(e)}",
            exc_info=True,
        )
        raise
    finally:
        logger.info(
            f"Processus de téléchargement des blobs terminé pour le conteneur '{container_name}'"
        )


# Creation des DataFrames depuis les fichiers telecharges;
# retourne un dictionnaire des Dataframes
def create_dataframes_from_files(file_list, path, spark):
    logger.info(
        f"Début de la création des DataFrames à partir de {len(file_list)} fichiers dans le chemin '{path}'"
    )
    df_dict = {}
    try:
        for file_path_suffix in file_list:
            try:
                full_path = f"{path}/{file_path_suffix}"
                filename = file_path_suffix.split("/")[-1].split(".")[0]

                logger.info(f"Création du DataFrame pour {filename}...")

                if file_path_suffix.endswith(".json"):
                    df = spark.read.json(full_path)
                else:
                    df = spark.read.csv(full_path, header=True, inferSchema=True)

                globals()[f"df_{filename}"] = df
                df_dict[f"{filename}"] = df
                logger.info(f"DataFrame pour {filename} créé avec succès")
            except Exception as e:
                logger.error(
                    f"Échec de la création du DataFrame pour {file_path_suffix} : {str(e)}",
                    exc_info=True,
                )
                raise
    except Exception as e:
        logger.error(
            f"Erreur lors du processus de création des DataFrames : {str(e)}",
            exc_info=True,
        )
        raise
    finally:
        logger.info(
            f"Processus de création des DataFrames terminé. {len(df_dict)} DataFrames créés"
        )
    return df_dict
