import os
import logging
from spark_utils import get_spark_session
from azure_utils import get_azure_info

logger = logging.getLogger(__name__)


def write_df_to_adls(df, file_name, container_name, storage_name):
    logger.info(
        f"Début de l'écriture du DataFrame {file_name} dans le conteneur {container_name}"
    )
    try:
        df.write.format("parquet").mode("overwrite").option(
            "spark.sql.parquet.enableVectorizedWriter", "false"
        ).save(
            f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{file_name}"
        )
        logger.info(f"DataFrame {file_name} écrit avec succès")
    except Exception as e:
        logger.error(f"Erreur lors de l'écriture du DataFrame {file_name}: {e}")
        raise
    finally:
        logger.info(f"Écriture du DataFrame {file_name} terminée")


if __name__ == "__main__":
    logger.info("Début du script Writer.py")
    try:
        # Récupération du nom du stockage ADLS
        storage_name = get_azure_info()[0]
        dest_container = "clean"
        spark = get_spark_session()
        df = spark.read.parquet("./data/tmp/enriched.parquet")
        write_df_to_adls(df, "enriched.parquet", dest_container, storage_name)
        logger.info("Script Writer.py exécuté avec succès")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du script Writer.py: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Fin du script Writer.py")
