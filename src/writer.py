import os
import logging

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
