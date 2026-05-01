from processors.dynamic_pipeline import DynamicPipeline
# ============================================
# CONFIG
# ============================================
resource_path = "/Volumes/bikestore/resource/01-origem/"
silver_path = "bikestore.silver"
quarantine_path = "bikestore.silver_quarentena"

bronze_map = {
    "tmp_bronze_brands":      f"{resource_path}brands.csv",
    "tmp_bronze_categories":  f"{resource_path}categories.csv",
    "tmp_bronze_customers":   f"{resource_path}customers.csv",
    "tmp_bronze_order_items": f"{resource_path}order_items.csv",
    "tmp_bronze_orders":      f"{resource_path}orders.csv",
    "tmp_bronze_products":    f"{resource_path}products.csv",
    "tmp_bronze_staffs":      f"{resource_path}staffs.csv",
    "tmp_bronze_stocks":      f"{resource_path}stocks.csv",
    "tmp_bronze_stores":      f"{resource_path}stores.csv",
}

# ============================================
# Regras de validação
# ============================================
validation_map = {
    "brands": ["brand_name"],
    "categories": ["category_name"],
    "customers": ["first_name", "last_name","phone", "email"],
    "order_items": ["item_id","product_id","quantity","list_price","discount"],
    "orders": ["customer_id","order_status","order_date","required_date","shipped_date","store_id","staff_id"],
    "products": ["product_name","brand_id","category_id","model_year","list_price"],
    "staffs": ["first_name","last_name","email","phone","active","store_id","manager_id"],
    "stocks": ["store_id","product_id","quantity"],
    "stores": ["store_id","store_name","phone","email","street","city","state","zip_code"],
}

# ============================================
# EXECUÇÃO
# ============================================
pipeline = DynamicPipeline(
    spark=spark,  # já existe no Databricks
    bronze_map=bronze_map,
    validation_map=validation_map,
    silver_path=silver_path,
    quarantine_path=quarantine_path
)

pipeline.run()

print("🏁 Pipeline finalizado com sucesso!")
