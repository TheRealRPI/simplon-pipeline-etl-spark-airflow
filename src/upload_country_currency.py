from utils import upload_blob_file


# Telecharge le fichier ./data/src/country_currency.csv vers le container ADLS raw dans le dossier reference
def upload_country_currency():
    # Chemin vers le dossier local où se trouve notre fichier source (r [raw string] pour ne rien échanpper)
    local_folder_path = r"./data/src/"
    # Nom du fichier à uploader
    local_filename = "country_currency.csv"
    # Nom du conteneur de destination dans ADLS
    dest_container = "raw"
    # Dossier de destination dans le conteneur
    dest_folder = "reference"

    upload_blob_file(dest_container, local_filename, local_folder_path, dest_folder)


if __name__ == "__main__":
    upload_country_currency()
