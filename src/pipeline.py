from utils import get_azure_info, get_spark_session
from reader import download_blobs_from_list, create_dataframes_from_files
from transformer import build_enriched
from writer import write_df_to_adls
from fetch_exchange_rates import fetch_and_upload_exchange_rates
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__ if __name__ != "__main__" else "pipeline")

# Nom sans extensions; les fichiers sont tous au format CSV; noms seront utilisés pour les DataFrames et les tables de la base de donnes
files_list = {
    "categories.csv",
    "customers.csv",
    "employees.csv",
    "order_details.csv",
    "orders.csv",
    "products.csv",
    "shippers.csv",
    "suppliers.csv",
    "reference/country_currency.csv",
    "reference/exchange_rates.json",
}

# Nom du conteneur source dans le stockage ADLS
src_container = "raw"
# Chemin du dossier de destination des CSV
path = r"./data/"
# Récupération du nom du stockage ADLS
storage_name = get_azure_info()[0]
dest_container = "clean"

logger.info("==========================================")
logger.info("DEMARRAGE DE LA PIPELINE")
logger.info("==========================================")

try:
    # Creation de la session Spark
    logger.info("----------------------------------------")
    logger.info("CREATION DE LA SESSION SPARK")
    logger.info("----------------------------------------")
    spark = get_spark_session()
    logger.info("Session Spark creee")

    # Etape 1 - Telecharger les Fichiers CSV & JSON depuis le stockage ADLS
    logger.info("==========================================")
    logger.info("ETAPE 1 : TELECHARGEMENT DES FICHIERS DEPUIS ADLS")
    logger.info("==========================================")
    download_blobs_from_list(files_list, src_container, path)
    logger.info("----------------------------------------")
    logger.info("Telechargement des fichiers termine")
    logger.info("----------------------------------------")

    # Etape 2 - Création des DataFrames a partir des fichiers telecharges
    # Retourne un dictionnaire de DataFrames
    logger.info("==========================================")
    logger.info("ETAPE 2 : CREATION DES DATAFRAMES")
    logger.info("==========================================")
    df_dict = create_dataframes_from_files(files_list, path, spark)
    logger.info("----------------------------------------")
    logger.info("Creation des DataFrames terminee")
    logger.info("----------------------------------------")

    # Etape 3 : Creation du DataFrame enrichi
    logger.info("==========================================")
    logger.info("ETAPE 3 : CREATION DU DATAFRAME ENRICHI")
    logger.info("==========================================")
    df_enriched = build_enriched(df_dict)

    df_enriched.printSchema()
    logger.info("----------------------------------------")
    logger.info("Creation du DataFrame enrichi terminee")
    logger.info("----------------------------------------")

    # Etape 4 : Ecriture du DataFrame enrichi dans le stockage ADLS
    logger.info("==========================================")
    logger.info("ETAPE 4 : ECRITURE DU DATAFRAME ENRICHI DANS ADLS")
    logger.info("==========================================")
    write_df_to_adls(df_enriched, "orders_enriched", dest_container, storage_name)
    logger.info("----------------------------------------")
    logger.info("Ecriture du DataFrame enrichi terminee")
    logger.info("----------------------------------------")

    logger.info("==========================================")
    logger.info("PIPELINE TERMINEE AVEC SUCCES")
    logger.info("==========================================")

except Exception as e:
    logger.error(f"Erreur dans la pipeline: {e}")
    raise
finally:
    # Fermeture de la session Spark
    logger.info("----------------------------------------")
    logger.info("FERMETURE DE LA SESSION SPARK")
    logger.info("----------------------------------------")
    spark.stop()
    logger.info("Session Spark fermee")
    logger.info("----------------------------------------")
    logger.info("FIN DE LA PIPELINE")
    logger.info("----------------------------------------")
