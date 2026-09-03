import sys
import os
from pathlib import Path

# Ajoute SPARK/ et SPARK/src/ au path
sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent.parent / "src"))

import pytest
from pyspark.sql import SparkSession, Row
from src.transformer import clean_orders, add_sous_total, clean_customers


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
        ),
        Row(
            customer_id=2,
            company_name="company B",
            contact_name="  jane smith  ",
            city="lyon",
            country="  usa  ",
            phone="789012",
        ),
    ]

    df = spark_session.createDataFrame(data)
    result_df = clean_customers(df)

    # Vérifie suppression espaces + majuscule première lettre sur contact_name (renommé en customer_name)
    contact_names = result_df.select("customer_name").collect()
    assert (
        contact_names[0].customer_name == "John Doe"
    ), f"Attendu 'John Doe', obtenu '{contact_names[0].customer_name}'"
    assert (
        contact_names[1].customer_name == "Jane Smith"
    ), f"Attendu 'Jane Smith', obtenu '{contact_names[1].customer_name}'"

    # Vérifie suppression espaces + majuscules sur country (renommé en customer_country)
    countries = result_df.select("customer_country").collect()
    assert (
        countries[0].customer_country == "FRANCE"
    ), f"Attendu 'FRANCE', obtenu '{countries[0].customer_country}'"
    assert (
        countries[1].customer_country == "USA"
    ), f"Attendu 'USA', obtenu '{countries[1].customer_country}'"
