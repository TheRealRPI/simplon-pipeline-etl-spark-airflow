from utils import get_azure_info
from reader import download_blobs_from_list, create_dataframes_from_files
from transformer import build_enriched
from writer import write_df_to_adls

# Nom sans extensions; les fichiers sont tous au format CSV; noms seront utilisés pour les DataFrames et les tables de la base de donnes
files_list = {
    "categories",
    "customers",
    "employees",
    "order_details",
    "orders",
    "products",
    "shippers",
    "suppliers",
}

# Nom du conteneur source dans le stockage ADLS
src_container = "raw"
# Chemin du dossier de destination des CSV
path = r"./data/"
# Récupération du nom du stockqge ADLS
storage_name = get_azure_info()[0]
dest_container = "clean"

# Etape 1 - Telecharger les Fichiers CSV

download_blobs_from_list(files_list, src_container, path)

# Etape 2 - Création des DataFrames a partir des fichiers telecharges
# Retourne un dictionnaire de DataFrames

df_dict = create_dataframes_from_files(files_list, path)

# Etape 3 : Creation du DataFrame enrichi

df_enriched = build_enriched(df_dict)

# Etape 4 : Ecriture du DataFrame enrichi dans le stockage ADLS

write_df_to_adls(df_enriched, "orders_enriched", dest_container, storage_name)
