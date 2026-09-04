import sys
import os
from pathlib import Path

# Ajoute SPARK/ et SPARK/src/ au path
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent.parent / "src"))

import pytest
from pyspark.sql import SparkSession, Row
from src.transformer import clean_orders, add_sous_total, clean_customers
from src.enrichment import add_currency_column


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-transformers")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_clean_orders_removes_null_shipped_date(spark_session):
    data = [
        Row(
            order_id=1,
            order_date="2023-01-01",
            required_date="2023-01-10",
            shipped_date="2023-01-05",
            freight="10.5",
            ship_via=1,
        ),
        Row(
            order_id=2,
            order_date="2023-01-02",
            required_date="2023-01-11",
            shipped_date=None,
            freight="20.0",
            ship_via=2,
        ),
        Row(
            order_id=3,
            order_date="2023-01-03",
            required_date="2023-01-12",
            shipped_date="2023-01-07",
            freight="15.0",
            ship_via=1,
        ),
        Row(
            order_id=4,
            order_date="2023-01-04",
            required_date="2023-01-13",
            shipped_date=None,
            freight="30.0",
            ship_via=3,
        ),
    ]

    df = spark_session.createDataFrame(data)

    result_df = clean_orders(df)

    null_count = result_df.filter(result_df.shipped_date.isNull()).count()
    assert null_count == 0, "Il reste des lignes avec shipped_date NULL !"


def test_add_sous_total(spark_session):
    data = [
        Row(
            prix_unitaire=1299.99, quantite=1, remise=0.20, expected_sous_total=1039.99
        ),
        Row(prix_unitaire=19.99, quantite=2, remise=0.30, expected_sous_total=27.99),
        Row(prix_unitaire=2.50, quantite=50, remise=0.15, expected_sous_total=106.25),
        Row(
            prix_unitaire=45.75, quantite=100, remise=0.10, expected_sous_total=4117.50
        ),
        Row(
            prix_unitaire=9999.99, quantite=1, remise=0.05, expected_sous_total=9499.99
        ),
        Row(prix_unitaire=50.00, quantite=10, remise=1.0, expected_sous_total=0.00),
        Row(prix_unitaire=123.45, quantite=3, remise=0.10, expected_sous_total=333.32),
    ]

    df = spark_session.createDataFrame(data)
    result_df = add_sous_total(df)

    # Compare les colonnes calculée et attendue
    results = result_df.select("sous_total", "expected_sous_total").collect()
    for row in results:
        assert (
            row.sous_total == row.expected_sous_total
        ), f"Attendu {row.expected_sous_total}, obtenu {row.sous_total}"


def test_clean_customers_trims_and_capitalizes(spark_session):
    data = [
        Row(
            customer_id=1,
            company_name="  company A  ",
            contact_name="  john doe  ",
            city="paris",
            country="  france  ",
            phone="123456",
            expected_customer_name="John Doe",
            expected_customer_country="FRANCE",
        ),
        Row(
            customer_id=2,
            company_name="company B",
            contact_name="  jane smith  ",
            city="lyon",
            country="  usa  ",
            phone="789012",
            expected_customer_name="Jane Smith",
            expected_customer_country="USA",
        ),
    ]

    df = spark_session.createDataFrame(data)
    result_df = clean_customers(df)

    # Vérifie en une seule boucle
    results = result_df.select(
        "customer_name", "expected_customer_name",
        "customer_country", "expected_customer_country"
    ).collect()
    for row in results:
        assert (
            row.customer_name == row.expected_customer_name
        ), f"Attendu {row.expected_customer_name}, obtenu {row.customer_name}"
        assert (
            row.customer_country == row.expected_customer_country
        ), f"Attendu {row.expected_customer_country}, obtenu {row.customer_country}"


def test_add_currency_column(spark_session):
    # Données réelles basées sur country_currency.csv et exchange_rates.json
    # Trois pays différents avec de vraies valeurs de change + cas exotiques
    orders_data = [
        # Cas standard avec EUR (rate: 0.863)
        Row(
            customer_country="FRANCE",
            sous_total=100.00,
            order_id=1,
            expected_sous_total_local=86.30,
        ),
        # Cas standard avec USD (rate: 1.0)
        Row(
            customer_country="USA",
            sous_total=200.00,
            order_id=2,
            expected_sous_total_local=200.00,
        ),
        # Cas standard avec GBP (rate: 0.742)
        Row(
            customer_country="UK",
            sous_total=300.00,
            order_id=3,
            expected_sous_total_local=222.60,
        ),
        # Cas exotique: montant très petit pour tester l'arrondi (0.01 * 0.863 = 0.00863 -> 0.01)
        Row(
            customer_country="FRANCE",
            sous_total=0.01,
            order_id=4,
            expected_sous_total_local=0.01,
        ),
        # Cas exotique: montant très grand
        Row(
            customer_country="USA",
            sous_total=999999.99,
            order_id=5,
            expected_sous_total_local=999999.99,
        ),
        # Cas exotique: montant zéro
        Row(
            customer_country="UK",
            sous_total=0.00,
            order_id=6,
            expected_sous_total_local=0.00,
        ),
        # Cas exotique: pays sans correspondance dans currency (JAPAN n'est pas dans country_currency.csv)
        Row(
            customer_country="JAPAN",
            sous_total=50.00,
            order_id=7,
            expected_sous_total_local=None,
        ),
    ]

    # Données réelles depuis country_currency.csv
    currency_data = [
        Row(country="FRANCE", currency="EUR"),
        Row(country="USA", currency="USD"),
        Row(country="UK", currency="GBP"),
        Row(country="GERMANY", currency="EUR"),
        Row(country="SPAIN", currency="EUR"),
    ]

    # Données réelles depuis exchange_rates.json (taux par rapport à USD)
    rates_data = [
        Row(code_pays="EUR", rate=0.863),
        Row(code_pays="USD", rate=1.0),
        Row(code_pays="GBP", rate=0.742),
        Row(code_pays="JPY", rate=159.09),
    ]

    # Création des DataFrames
    df_orders = spark_session.createDataFrame(orders_data)
    df_currency = spark_session.createDataFrame(currency_data)
    df_rates = spark_session.createDataFrame(rates_data)

    # Appel de la fonction
    result_df = add_currency_column(df_orders, df_currency, df_rates)

    # Vérifications
    # 1. La colonne sous_total_local existe
    assert (
        "sous_total_local" in result_df.columns
    ), "La colonne sous_total_local est manquante"

    # 2. Toutes les colonnes originales sont conservées
    original_columns = [f.name for f in df_orders.schema.fields]
    for col in original_columns:
        assert (
            col in result_df.columns
        ), f"La colonne {col} est manquante dans le résultat"

    # 3. Vérification des valeurs calculées avec de vraies données
    results = result_df.select(
        "sous_total_local", "expected_sous_total_local"
    ).collect()
    for row in results:
        assert (
            row.sous_total_local == row.expected_sous_total_local
        ), f"Attendu {row.expected_sous_total_local}, obtenu {row.sous_total_local}"
