import dlt
import pandas as pd
from sqlalchemy import create_engine

#connection to bronze
engine_bronze = create_engine(
    f"postgresql+psycopg2://{dlt.secrets['local_postgres.destination.postgres.credentials']['username']}:"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['password']}@"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['host']}:5432/"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['database']}"
)

#silver customers source
@dlt.source
def bronze_customers():
    df = pd.read_sql("SELECT * FROM bronze.customers", con=engine_bronze)
    df = df.drop(columns=[c for c in ["dlt_id", "dlt_load"] if c in df.columns])
    df = df.drop_duplicates(subset=["customer_id"])
    
    #merge strategy to update 
    @dlt.resource(name="customers", write_disposition="merge", primary_key="customer_id")
    def transform_customers():
        df_copy = df.copy()
        if "region" in df_copy.columns:
            df_copy["region"] = df_copy["region"].str.lower().str.capitalize()
        if "birth_date" in df_copy.columns:
            df_copy["birth_date"] = pd.to_datetime(df_copy["birth_date"], errors="coerce")
        df_copy = df_copy.drop(columns=[c for c in ["dlt_id", "dlt_load"] if c in df_copy.columns])
        df_copy = df_copy.drop_duplicates(subset=["customer_id"])
        for record in df_copy.to_dict(orient="records"):
            yield record

    return transform_customers

#silver products source
@dlt.source
def bronze_products():
    df = pd.read_sql("SELECT * FROM bronze.products", con=engine_bronze)
    df = df.drop(columns=[c for c in ["dlt_id", "dlt_load"] if c in df.columns])
    df = df.drop_duplicates(subset=["product_id"])
    
    #merge strategy to update  
    @dlt.resource(name="products", write_disposition="merge", primary_key="product_id")
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
def bronze_sales():
    df = pd.read_sql("SELECT * FROM bronze.sales", con=engine_bronze)

    #merge strategy to update 
    @dlt.resource(name="sales", write_disposition="merge",primary_key='sale_id')
    def transform_sales():
        df_copy = df.copy()
        if "price" in df_copy.columns:
            df_copy["price"] = df_copy["price"].astype(float)
        for col in ["sale_id","product_id", "customer_id", "quantity"]:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].astype("Int64")
        df_copy = df_copy.drop(columns=[c for c in ["dlt_id", "dlt_load"] if c in df_copy.columns])
        for record in df_copy.to_dict(orient="records"):
            yield record

    return transform_sales

#silver stock source
@dlt.source
def bronze_stock():
    df = pd.read_sql("SELECT * FROM bronze.stock", con=engine_bronze)

    #merge strategy to update
    @dlt.resource(name="stock", write_disposition="merge", primary_key="sku")
    def transform_stock():
        df_copy = df.copy()
        df_copy = df_copy.drop(columns=[c for c in ["_id", "dlt_id", "dlt_load"] if c in df_copy.columns])
        for record in df_copy.to_dict(orient="records"):
            yield record

    return transform_stock

#silver pipeline conf
pipeline_silver = dlt.pipeline(
    pipeline_name="local_postgres",
    destination="postgres",
    dataset_name="silver"
)


#silver pipeline run
load_info = pipeline_silver.run([
    bronze_customers(),
    bronze_products(),
    bronze_sales(),
    bronze_stock()
])


#function for orchestrator
def run_silver_pipeline():
    load_info = pipeline_silver.run([
        bronze_customers(),
        bronze_products(),
        bronze_sales(),
        bronze_stock()
    ])
    print("pipeline executed successfully — data stored in schema 'silver'")
    return load_info

#standalone execution
if __name__ == "__main__":
    run_silver_pipeline()
