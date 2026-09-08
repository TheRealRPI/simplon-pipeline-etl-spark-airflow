from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import logging

logger = logging.getLogger(__name__)


# Création d'une session Spark, avec intégration des credentials Azure pour le writer
def get_spark_session():
    logger.info("Création de la session Spark")
    try:
        from azure_utils import get_azure_info

        storage, key = get_azure_info()
        spark = SparkSession.builder.appName("TradeCorp ETL").getOrCreate()
        spark.conf.set(f"fs.azure.account.key.{storage}.dfs.core.windows.net", key)
        return spark
    except Exception as e:
        logger.error(f"Erreur lors de la création de la session Spark: {e}")
        raise
    finally:
        logger.info("Création de la session Spark terminée")


# Fonction generique


# Supprime les espaces sur une colonne dans un df
def trim_col(df, col_name):
    df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df


# Supprme les espaces et met en majuscule les premieres lettres de chaque mots sur une colonne dans un df
def camel_trim_col(df, col_name):
    df = df.withColumn(col_name, F.initcap(F.trim(F.col(col_name))))
    return df


# Supprime les espaces et met tout en majuscule sur une colonne dans un df
def upper_trim_col(df, col_name):
    df = df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))
    return df


# Supprime les doublons sur une colonne dans un df
def deduplicate(df, col_name):
    df = df.dropDuplicates([col_name])
    return df


# Supprimer les lignes si la colonne donnée est nulle
def delete_col_if_null(df, col_name):
    df = df.dropna(subset=[col_name])
    return df


# Cast une colonne selon le type defini
def cast_to_type(df, col_name, type):
    match type:
        case "date":
            df = df.withColumn(col_name, F.col(col_name).cast(T.DateType()))
        case "double":
            df = df.withColumn(col_name, F.col(col_name).cast(T.DoubleType()))
        case "integer":
            df = df.withColumn(col_name, F.col(col_name).cast(T.IntegerType()))
    return df


# Renommer une colonne
def rename_col(df, col_name, new_name):
    df = df.withColumnRenamed(col_name, new_name)
    return df


# Renommer plusieurs colonnes grace a un dictionnaire {"col1" : "col1_new_name", ...}
def rename_cols(df, dict_names):
    df = df.withColumnsRenamed(dict_names)
    return df
