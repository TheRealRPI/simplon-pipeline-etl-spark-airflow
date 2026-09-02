from utils import download_blob_to_file, get_spark_session

# Telechargement des documents necessaires

# Nom sans extensions; les fichiers sont tous au format CSV; noms seront utilisés pour les DataFrames et les tables de la base de donnes
files_to_download = {
    "categories",
    "country_currency",
    "customers",
    "employees",
    "order_details",
    "orders",
    "products",
    "shippers",
    "suppliers",
}


# Telechargement des blobs dans un dossier local depuis une liste
def download_blobs_from_list(blob_list, container_name, path):
    for filename in blob_list:
        print(f"Downloading {filename}.csv...")
        download_blob_to_file(container_name, f"{filename}.csv", path)


# Creation des DataFrames depuis les fichiers telecharges;
# retourne un dictionnaire des Dataframes
def create_dataframes_from_files():
    spark = get_spark_session()
    file_list = files_to_download
    path = r"./data/"
    df_dict = {}
    for filename in file_list:
        print(f"Creating DataFrame for {filename}...")
        globals()[f"df_{filename}"] = spark.read.csv(
            f"{path}/{filename}.csv", header=True, inferSchema=True
        )
        df_dict[f"{filename}"] = globals()[f"df_{filename}"]
    return df_dict


# download_blobs_from_list(files_to_download, "raw", r"./data/")
