def get_product_category_pairs(products_df, categories_df, product_categories_df):
    product_cat_df = product_categories_df \
        .join(categories_df, on="category_id", how="left")

    result_df = products_df \
        .join(product_cat_df, on="product_id", how="left") \
        .select("product_name", "category_name")

    return result_df
