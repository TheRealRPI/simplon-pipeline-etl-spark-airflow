from utils import *
import reader as R
from pyspark.sql.functions import col, round


def clean_customers(df):
    return (
        df.transform(trim_col, "company_name")
        .transform(camel_trim_col, "contact_name")
        .transform(upper_trim_col, "country")
        .transform(deduplicate, "customer_id")
    )


def clean_orders(df):
    return (
        df.transform(delete_col_if_null, "shipped_date")
        .transform(cast_to_type, "order_date", "date")
        .transform(cast_to_type, "required_date", "date")
        .transform(cast_to_type, "shipped_date", "date")
        .transform(cast_to_type, "freight", "decimal")
        .transform(rename_col, "ship_via", "shipper_id")
        .withColumn("is_shipped", col("shipped_date").isNotNull())
    )


def clean_order_details(df):
    return (
        df.transform(cast_to_type, "unit_price", "decimal")
        .transform(cast_to_type, "quantity", "integer")
        .transform(rename_col, "unit_price", "prix_unitaire")
        .transform(rename_col, "quantity", "quantite")
        .transform(rename_col, "discount", "remise")
        .withColumn(
            "sous_total",
            round((col("prix_unitaire") * col("quantite") * (1 - col("remise"))), 2),
        )
    )


df_dict = R.create_dataframes_from_files()

df_dict["customers"] = clean_customers(df_dict["customers"])
df_dict["customers"].show(5)

df_dict["orders"] = clean_orders(df_dict["orders"])
df_dict["orders"].show(5)

df_dict["order_details"] = clean_order_details(df_dict["order_details"])
df_dict["order_details"].show(5)
