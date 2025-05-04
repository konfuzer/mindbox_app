from pyspark.sql import SparkSession
from mindbox.data import get_dataframes
from mindbox.logic import get_product_category_pairs

def main():
    spark = SparkSession.builder \
        .appName("Mindbox Product-Category App") \
        .master("local[*]") \
        .getOrCreate()

    products_df, categories_df, product_categories_df = get_dataframes(spark)

    result_df = get_product_category_pairs(products_df, categories_df, product_categories_df)
    result_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
