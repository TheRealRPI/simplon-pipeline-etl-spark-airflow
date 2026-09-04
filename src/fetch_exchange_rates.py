import requests
import json
from utils import get_blob_service_client
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(
    __name__ if __name__ != "__main__" else "fetch_exchange_rates"
)


def fetch_and_upload_exchange_rates():
    try:
        logger.info("Recuperation des taux de change depuis l'API...")
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        data = requests.get(url).json()
        logger.info("Taux de change recuperes avec succes")

        logger.info("Transformation des taux en format simple...")
        rates_list = [
            {"code_pays": code, "rate": float(rate)}
            for code, rate in data["rates"].items()
        ]
        logger.info("Transformation terminee")

        logger.info("Upload vers ADLS...")
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container="raw", blob="reference/exchange_rates.json"
        )
        blob_client.upload_blob(json.dumps(rates_list), overwrite=True)
        logger.info(f"JSON uploade avec succes vers : {blob_client.url}")

    except requests.RequestException as e:
        logger.error(f"Erreur lors de la requete API: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Erreur de decodage JSON: {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la mise a jour des taux de change: {e}")
        raise
    finally:
        logger.info("Fin de la mise a jour des taux de changes")


if __name__ == "__main__":
    fetch_and_upload_exchange_rates()
