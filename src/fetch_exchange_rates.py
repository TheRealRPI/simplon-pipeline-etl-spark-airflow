import requests
import json
from utils import get_blob_service_client


def fetch_and_upload_exchange_rates():
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    data = requests.get(url).json()

    # Transformer en liste simple de {code_pays, rate}
    rates_list = [
        {"code_pays": code, "rate": float(rate)} for code, rate in data["rates"].items()
    ]

    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(
        container="raw", blob="reference/exchange_rates.json"
    )
    try:
        blob_client.upload_blob(json.dumps(rates_list), overwrite=True)
        print(f"JSON uploadé avec succès vers : {blob_client.url}")
    except Exception as e:
        print(f"Erreur lors de l'upload vers ADLS: {e}")


if __name__ == "__main__":
    fetch_and_upload_exchange_rates()
