import requests
import json
from utils import get_blob_service_client


def fetch_and_upload_exchange_rates():
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    data = requests.get(url).json()

    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container="raw", blob="reference/exchange_rates.json"
    )
    try:
        blob_client.upload_blob(json.dumps(data), overwrite=True)
        print(f"JSON uploadé avec succès vers : {blob_client.url}")
    except Exception as e:
        print(f"Erreur lors de l'upload vers ADLS: {e}")


# Lance la fonction si le script est lancé directement
if __name__ == "__main__":
    fetch_and_upload_exchange_rates()
