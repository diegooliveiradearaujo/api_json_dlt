import dlt
import pandas as pd
from sqlalchemy import create_engine

#connection to bronze
engine_bronze = create_engine(
    f"postgresql+psycopg2://{dlt.secrets['local_postgres_bronze.destination.postgres.credentials']['username']}:"
    f"{dlt.secrets['local_postgres_bronze.destination.postgres.credentials']['password']}@"
    f"{dlt.secrets['local_postgres_bronze.destination.postgres.credentials']['host']}:5432/"
    f"{dlt.secrets['local_postgres_bronze.destination.postgres.credentials']['database']}"
)

#silver customers source
@dlt.source
def silver_customers():
    df = pd.read_sql("SELECT * FROM bronze.customers", con=engine_bronze)
        
    #merge strategy to update 
    @dlt.resource(name="customers", write_disposition="merge", primary_key="hash_key")
    def transform_customers():
        df_copy = df.copy()
        if "region" in df_copy.columns:
            df_copy["region"] = df_copy["region"].str.lower().str.capitalize()
        if "birth_date" in df_copy.columns:
            df_copy["birth_date"] = pd.to_datetime(df_copy["birth_date"], errors="coerce")
        for record in df_copy.to_dict(orient="records"):
            yield record
    return transform_customers

#silver products source
@dlt.source
def silver_products():
    df = pd.read_sql("SELECT * FROM bronze.products", con=engine_bronze)

    #merge strategy to update  
    @dlt.resource(name="products", write_disposition="merge", primary_key="hash_key")
    def transform_products():
        df_copy = df.copy()
        if "sk_u" in df_copy.columns:
            df_copy.rename(columns={"sk_u": "sku"}, inplace=True)
        if "category" in df_copy.columns:
            df_copy["category"] = df_copy["category"].str.capitalize()
        for record in df_copy.to_dict(orient="records"):
            yield record
    return transform_products

#silver sales source
@dlt.source
def silver_sales():
    df = pd.read_sql("SELECT * FROM bronze.sales", con=engine_bronze)

    #merge strategy to update 
    @dlt.resource(name="sales", write_disposition="merge",primary_key='hash_key')
    def transform_sales():
        df_copy = df.copy()
        if "price" in df_copy.columns:
            df_copy["price"] = df_copy["price"].astype(float)
        for col in ["sale_id","product_id", "customer_id", "quantity"]:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].astype("Int64")
        for record in df_copy.to_dict(orient="records"):
            yield record
    return transform_sales

#silver stock source
@dlt.source
def silver_stock():
    df = pd.read_sql("SELECT * FROM bronze.stock", con=engine_bronze)

    #merge strategy to update
    @dlt.resource(name="stock", write_disposition="merge", primary_key="hash_key")
    def transform_stock():
        df_copy = df.copy()
        df_copy = df_copy.drop(columns=["_id"])
        for record in df_copy.to_dict(orient="records"):
            yield record
    return transform_stock

#silver pipeline conf
pipeline_silver = dlt.pipeline(
    pipeline_name="local_postgres_silver",
    destination="postgres",
    dataset_name="silver"
)

#function for orchestrator
def run_silver_pipeline():
    load_info = pipeline_silver.run([
        silver_customers(),
        silver_products(),
        silver_sales(),
        silver_stock()
    ])
    print("Pipeline executed successfully — data stored in schema 'silver'")
    return load_info

#standalone execution
if __name__ == "__main__":
    run_silver_pipeline()