import dlt
import requests
import pandas as pd
import hashlib
import json
from pymongo import MongoClient
from sqlalchemy import create_engine


#hash generation
def compute_hash(record):
    return hashlib.md5(json.dumps(record, sort_keys=True, default=str).encode()).hexdigest()

#api source (github)
@dlt.source
def api_source(
    products_url=dlt.secrets["sources.api.products_url"],
    customers_url=dlt.secrets["sources.api.customers_url"]
):
    #function to fetch data from api
    def fetch_resource(name, url):
        @dlt.resource(name=name, write_disposition="merge", primary_key="hash_key")
        def resource():
            #request data
            resp = requests.get(url)
            resp.raise_for_status()  #raise error if request fails
            data = resp.json()
            #convert single dict response to list
            if isinstance(data, dict):
                data = [data]

            #yield each record for dlt ingestion
            for record in data:
                record["hash_key"] = compute_hash(record)  
                #record["load_timestamp"] = datetime.utcnow().isoformat()
                yield record
        return resource
    
    #yield both products and customers resources
    yield fetch_resource("products", products_url)
    yield fetch_resource("customers", customers_url)

#mongodb source
def mongo_source():
    @dlt.resource(name="stock", write_disposition="merge", primary_key="hash_key")
    def load_stock():
        #mongodb connection
        client = MongoClient(dlt.secrets["sources.mongodb.credentials"]["uri"])
        db = client["stock_data"]
        collection = db["stock"]
        documents = list(collection.find())
        
        #convert objectid to string
        for doc in documents:
            doc["_id"] = str(doc["_id"])
            doc["hash_key"] = compute_hash(doc)
        #yield documents for dlt ingestio
        yield documents
    return load_stock

#cloud postgresdb source (render)
@dlt.source
def cloud_postgres_source():
    cloud = dlt.secrets["sources.postgres_cloud.credentials"]

    #cloud postgresdb connection
    engine = create_engine(
        f"postgresql+psycopg2://{cloud['username']}:{cloud['password']}@"
        f"{cloud['host']}:5432/{cloud['database']}",
        connect_args={"sslmode": cloud["sslmode"]}
    )
    #read the sales table
    df = pd.read_sql("SELECT * FROM sales", con=engine)

    @dlt.resource(name="sales", write_disposition="merge", primary_key="hash_key")
    def load_sales():
        #yield each record for dlt ingestion
        for record in df.to_dict(orient="records"):
            record["hash_key"] = compute_hash(record)
            yield record
    return load_sales


#bronze pipeline conf
pipeline_bronze = dlt.pipeline(
    pipeline_name="local_postgres_bronze", #name of the pipeline
    destination="postgres",                #destination database
    dataset_name="bronze"                  #dataset (schema) name
)

#function for orchestrator
def run_bronze_pipeline():
    load_info = pipeline_bronze.run([
        api_source(),
        mongo_source(),
        cloud_postgres_source()
    ])
    print("Pipeline executed successfully — data stored in schema 'bronze'")
    return load_info

if __name__ == "__main__":
    run_bronze_pipeline()
