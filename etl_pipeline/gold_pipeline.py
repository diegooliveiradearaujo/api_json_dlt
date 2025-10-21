import dlt
import pandas as pd
from sqlalchemy import create_engine

#connection to silver
engine_silver = create_engine(
    f"postgresql+psycopg2://{dlt.secrets['local_postgres.destination.postgres.credentials']['username']}:"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['password']}@"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['host']}:5432/"
    f"{dlt.secrets['local_postgres.destination.postgres.credentials']['database']}"
)

#gold unsold products source
@dlt.source
def silver_unsold_products():
    query = """
        SELECT DISTINCT p.sku, p.product_id, p.product, 
        p.category, p.brand, p.model_year
        FROM silver.sales s
        RIGHT JOIN silver.products p
        ON s.product_id = p.product_id
        WHERE s.product_id IS null;
    """
    df = pd.read_sql(query, con=engine_silver)

    #replace strategy to load all again
    @dlt.resource(name="unsold_products", write_disposition="replace", primary_key="product_id")
    def transform_unsold_products():
        for record in df.to_dict(orient="records"):
            yield record

    return transform_unsold_products

#gold stock source
@dlt.source
def silver_stock():
    query = """
        WITH sales_product AS (
            SELECT p.sku, s.product_id, p.product, p.category, p.brand,
                   p.model_year, SUM(s.quantity) AS amount
            FROM silver.sales s
            INNER JOIN silver.products p ON s.product_id = p.product_id
            GROUP BY p.sku, s.product_id, p.product, p.category, p.brand, p.model_year
        )
        SELECT sp.sku, sp.product_id, sp.product, sp.category, sp.brand,
               sp.model_year, sp.amount, st.current_stock,
               (st.current_stock - sp.amount) AS avaliable
        FROM sales_product sp
        INNER JOIN silver.stock st ON st.sku = sp.sku;
    """
    df = pd.read_sql(query, con=engine_silver)

    #merge strategy to update 
    @dlt.resource(name="stock", write_disposition="merge", primary_key="sku")
    def transform_stock():
        for record in df.to_dict(orient="records"):
            yield record

    return transform_stock

#gold sales source
@dlt.source
def silver_sales():
    query = """
        SELECT s.sale_id, c.customer_id, c.customer, c.gender, c.region,
               p.sku, s.product_id, p.product, p.category,
               p.sub_category, p.brand, p.model_year, s.quantity, s.price,
               (s.quantity * s.price) AS amount, s.sale_date
        FROM silver.sales s
        INNER JOIN silver.customers c ON c.customer_id = s.customer_id
        INNER JOIN silver.products p ON p.product_id = s.product_id;
    """
    df = pd.read_sql(query, con=engine_silver)

    #merge strategy to update
    @dlt.resource(name="sales", write_disposition="merge", primary_key="sale_id")
    def transform_sales():
        for record in df.to_dict(orient="records"):
            yield record

    return transform_sales

#gold pipeline conf
pipeline_gold = dlt.pipeline(
    pipeline_name="local_postgres",
    destination="postgres",
    dataset_name="gold"
)

#silver pipeline run
load_info = pipeline_gold.run([
    silver_unsold_products(),
    silver_stock(),
    silver_sales()
])

#function for orchestrator
def run_gold_pipeline():
    load_info = pipeline_gold.run([
        silver_unsold_products(),
        silver_stock(),
        silver_sales()
    ])
    print("Pipeline executed successfully — data stored in schema 'gold'")
    return load_info

#standalone execution
if __name__ == "__main__":
    run_gold_pipeline()
