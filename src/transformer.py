from utils import *
from reader import create_dataframes_from_files
from writer import write_df_to_adls
from pyspark.sql.functions import col, round, concat, lit

# Note sur le fonctionnement de .transform() :
# .transform(fonction, argument1, argument2)
# => fonction(DataFrame, argument1, argument2)
# le .transform envoie par lui meme le df en cours de transformation en premier argument de la fonction


def clean_customers(df):
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


def clean_orders(df):
    return (
        df.transform(delete_col_if_null, "shipped_date")
        .transform(cast_to_type, "order_date", "date")
        .transform(cast_to_type, "required_date", "date")
        .transform(cast_to_type, "shipped_date", "date")
        .transform(cast_to_type, "freight", "double")
        .transform(rename_col, "ship_via", "shipper_id")
        .withColumn("is_shipped", col("shipped_date").isNotNull())
    )


def clean_order_details(df):
    return (
        df.transform(cast_to_type, "unit_price", "double")
        .transform(cast_to_type, "quantity", "integer")
        .transform(rename_col, "unit_price", "prix_unitaire")
        .transform(rename_col, "quantity", "quantite")
        .transform(rename_col, "discount", "remise")
        .withColumn(
            "sous_total",
            round((col("prix_unitaire") * col("quantite") * (1 - col("remise"))), 2),
        )
    )


# Nettoie le df employees
def clean_employees(df):
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
        .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
        .transform(rename_cols, col_renamed_employees)
    )


# Nettoie le df Products et ajoute la colonne category_name
def clean_products(df):
    return (
        df.transform(cast_to_type, "unit_price", "double")
        .join(df_dict["categories"], on="category_id")
        .select(df.columns + ["category_name"])
        .withColumn("en_stock", col("units_in_stock") > 0)
    )


def clean_shipper(df):
    col_renamed_shippers = {
        "company_name": "shipper_name",
        "phone": "shipper_phone",
    }
    return df.transform(rename_cols, col_renamed_shippers).transform(
        camel_trim_col, "shipper_name"
    )


def build_enriched(dd):
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
        .join(clean_employees(dd["employees"]), on="employee_id")
        .join(clean_shipper(dd["shippers"]), on="shipper_id")
        .select(col_to_keep)
    )
    return df


df_dict = create_dataframes_from_files()
df_orders_enriched = build_enriched(df_dict)
df_orders_enriched.printSchema()
df_orders_enriched.show(5)

c, k = get_azure_info()

write_df_to_adls(df_orders_enriched, "orders_enriched", "clean", c)
