from pyspark.sql.functions import col, round
import logging

logger = logging.getLogger(__name__)


def add_currency_column(df_orders, df_currency, df_rates):
    logger.info("Début de l'ajout de la colonne sous_total_local")
    try:
        df = df_orders.join(
            df_currency, df_orders["customer_country"] == df_currency["country"]
        ).join(df_rates, df_currency["currency"] == df_rates["code_pays"])

        df = df.withColumn(
            "sous_total_local", round(col("sous_total") * col("rate"), 2)
        )

        return df.select(df_orders.columns + ["sous_total_local"])
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout de la colonne sous_total_local: {e}")
        raise
    finally:
        logger.info("Ajout de la colonne sous_total_local terminé")
