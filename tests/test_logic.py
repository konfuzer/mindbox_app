import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from mindbox.logic import get_product_category_pairs
from mindbox.data import get_dataframes

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .getOrCreate()

def test_product_category_pairs(spark):
    products_df, categories_df, product_categories_df = get_dataframes(spark)

    result_df = get_product_category_pairs(products_df, categories_df, product_categories_df)

    expected_data = [
        ("Молоко", "Молочные"),
        ("Хлеб", "Выпечка"),
        ("Яблоко", "Фрукты"),
        ("Груша", "Фрукты"),
        ("Масло", None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["product_name", "category_name"])

    assert_df_equality(result_df, expected_df, ignore_row_order=True)
