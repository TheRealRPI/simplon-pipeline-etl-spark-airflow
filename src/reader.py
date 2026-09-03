from utils import download_blob_from_file, get_spark_session


# Telechargement des blobs dans un dossier local depuis une liste
def download_blobs_from_list(blob_list, container_name, path):
    for blob_path in blob_list:
        print(f"Downloading {blob_path}...")
        download_blob_from_file(container_name, blob_path, path)


# Creation des DataFrames depuis les fichiers telecharges;
# retourne un dictionnaire des Dataframes
# Creation des DataFrames depuis les fichiers telecharges;
# retourne un dictionnaire des Dataframes
def create_dataframes_from_files(file_list, path):
    spark = get_spark_session()
    df_dict = {}
    for file_path_suffix in file_list:
        full_path = f"{path}/{file_path_suffix}"
        filename = file_path_suffix.split("/")[-1].split(".")[0]

        print(f"Creating DataFrame for {filename}...")

        if file_path_suffix.endswith(".json"):
            df = spark.read.json(full_path)
        else:
            df = spark.read.csv(full_path, header=True, inferSchema=True)

        globals()[f"df_{filename}"] = df
        df_dict[f"{filename}"] = df
    return df_dict
