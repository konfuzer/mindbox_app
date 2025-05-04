from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def get_dataframes(spark):
    # Продукты
    products_data = [
        ("1", "Молоко"),
        ("2", "Хлеб"),
        ("3", "Яблоко"),
        ("4", "Груша"),
        ("5", "Масло")
    ]
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True)
    ])
    products_df = spark.createDataFrame(products_data, schema=products_schema)

    # Категории
    categories_data = [
        ("10", "Молочные"),
        ("20", "Фрукты"),
        ("30", "Выпечка")
    ]
    categories_schema = StructType([
        StructField("category_id", StringType(), True),
        StructField("category_name", StringType(), True)
    ])
    categories_df = spark.createDataFrame(categories_data, schema=categories_schema)

    # Связи продуктов и категорий
    product_categories_data = [
        ("1", "10"),  # Молоко — Молочные
        ("2", "30"),  # Хлеб — Выпечка
        ("3", "20"),  # Яблоко — Фрукты
        ("4", "20")   # Груша — Фрукты
        # Масло (5) — без категории
    ]
    product_categories_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True)
    ])
    product_categories_df = spark.createDataFrame(product_categories_data, schema=product_categories_schema)

    return products_df, categories_df, product_categories_df
