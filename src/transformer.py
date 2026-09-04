from utils import *
from reader import create_dataframes_from_files
from writer import write_df_to_adls
from enrichment import add_currency_column
from pyspark.sql.functions import col, round, concat, lit
import logging

logger = logging.getLogger(__name__)

# Note sur le fonctionnement de .transform() :
# .transform(fonction, argument1, argument2)
# => fonction(DataFrame, argument1, argument2)
# le .transform envoie par lui meme le df en cours de transformation en premier argument de la fonction


def clean_customers(df):
    logger.info("Début du nettoyage du DataFrame customers")
    try:
        col_renamed_customers = {
            "company_name": "customer_company_name",
            "city": "customer_city",
            "country": "customer_country",
            "phone": "customer_phone",
            "contact_name": "customer_name",
        }
        return (
            df.transform(trim_col, "company_name")
            .transform(camel_trim_col, "contact_name")
            .transform(upper_trim_col, "country")
            .transform(deduplicate, "customer_id")
            .transform(rename_cols, col_renamed_customers)
        )
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage de customers: {e}")
        raise
    finally:
        logger.info("Nettoyage de customers terminé")


def clean_orders(df):
    logger.info("Début du nettoyage du DataFrame orders")
    try:
        return (
            df.transform(delete_col_if_null, "shipped_date")
            .transform(cast_to_type, "order_date", "date")
            .transform(cast_to_type, "required_date", "date")
            .transform(cast_to_type, "shipped_date", "date")
            .transform(cast_to_type, "freight", "double")
            .transform(rename_col, "ship_via", "shipper_id")
            .withColumn("is_shipped", col("shipped_date").isNotNull())
        )
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage de orders: {e}")
        raise
    finally:
        logger.info("Nettoyage de orders terminé")


def clean_order_details(df):
    logger.info("Début du nettoyage du DataFrame order_details")
    try:
        col_renamed_orders_details = {
            "unit_price": "prix_unitaire",
            "quantity": "quantite",
            "discount": "remise",
        }
        df = (
            df.transform(cast_to_type, "unit_price", "double")
            .transform(cast_to_type, "quantity", "integer")
            .transform(rename_cols, col_renamed_orders_details)
        )
        return add_sous_total(df)
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage de order_details: {e}")
        raise
    finally:
        logger.info("Nettoyage de order_details terminé")


def add_sous_total(df):
    logger.info("Début du calcul du sous-total")
    try:
        return df.withColumn(
            "sous_total",
            round((col("prix_unitaire") * col("quantite") * (1 - col("remise"))), 2),
        )
    except Exception as e:
        logger.error(f"Erreur lors du calcul du sous-total: {e}")
        raise
    finally:
        logger.info("Calcul du sous-total terminé")


# Nettoie le df employees
def clean_employees(df):
    logger.info("Début du nettoyage du DataFrame employees")
    try:
        col_to_keep = [
            "employee_id",
            "first_name",
            "last_name",
            "title",
            "hire_date",
            "city",
            "country",
        ]
        col_renamed_employees = {"city": "employee_city", "country": "employee_country"}
        return (
            df.select(col_to_keep)
            .withColumn(
                "full_name", concat(col("first_name"), lit(" "), col("last_name"))
            )
            .transform(rename_cols, col_renamed_employees)
        )
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage de employees: {e}")
        raise
    finally:
        logger.info("Nettoyage de employees terminé")


# Nettoie le df Products
def clean_products(df):
    logger.info("Début du nettoyage du DataFrame products")
    try:
        return df.transform(cast_to_type, "unit_price", "double").withColumn(
            "en_stock", col("units_in_stock") > 0
        )
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage de products: {e}")
        raise
    finally:
        logger.info("Nettoyage de products terminé")


def clean_shipper(df):
    logger.info("Début du nettoyage du DataFrame shippers")
    try:
        col_renamed_shippers = {
            "company_name": "shipper_name",
            "phone": "shipper_phone",
        }
        return df.transform(rename_cols, col_renamed_shippers).transform(
            camel_trim_col, "shipper_name"
        )
    except Exception as e:
        logger.error(f"Erreur lors du nettoyage de shippers: {e}")
        raise
    finally:
        logger.info("Nettoyage de shippers terminé")


def build_enriched(dd):
    logger.info("Début de la construction du DataFrame enrichi")
    try:
        col_to_keep = [
            "order_id",
            "customer_id",
            "employee_id",
            "product_id",
            "order_date",
            "required_date",
            "shipped_date",
            "freight",
            "is_shipped",
            "prix_unitaire",
            "quantite",
            "remise",
            "sous_total",
            "customer_name",
            "customer_country",
            "customer_city",
            "product_name",
            "category_name",
            "en_stock",
            "full_name",
            "shipper_name",
        ]
        df = (
            clean_order_details(dd["order_details"])
            .join(clean_orders(dd["orders"]), on="order_id")
            .join(clean_customers(dd["customers"]), on="customer_id")
            .join(clean_products(dd["products"]), on="product_id")
            .join(dd["categories"], on="category_id")
            .join(clean_employees(dd["employees"]), on="employee_id")
            .join(clean_shipper(dd["shippers"]), on="shipper_id")
            .select(col_to_keep)
        )
        df = add_currency_column(df, dd["country_currency"], dd["exchange_rates"])
        return df
    except Exception as e:
        logger.error(f"Erreur lors de la construction du DataFrame enrichi: {e}")
        raise
    finally:
        logger.info("Construction du DataFrame enrichi terminée")
