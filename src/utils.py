import os
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import pyspark.sql.functions as F
import pyspark.sql.types as T


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


def get_spark_session(log_level="WARN"):
    spark = SparkSession.builder.appName("TradeCorp ETL").getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


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


# Ajouter une colonne avec une condition
def add_col_with_condition(df, col_name, condition):
    df = df.withColumn(col_name, condition)
