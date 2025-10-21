import streamlit as st
import pandas as pd
import datetime
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pytz

# load .env
load_dotenv()

# .env variables
db_user = os.getenv("postgres_user")
db_password = os.getenv("postgres_password")
db_host = os.getenv("postgres_host")
db_port = os.getenv("postgres_port")
db_name = os.getenv("postgres_db")

engine = create_engine(
    f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# csv customer and product
csv_customers = "customer_product/customer.csv"
csv_sales = "customer_product/product.csv" 

st.set_page_config(page_title="Sales App", layout="centered")


#customer function
@st.cache_data
def load_customers():
    if os.path.exists(csv_customers):
        return pd.read_csv(csv_customers, encoding='ISO-8859-1')
    return pd.DataFrame(columns=['customer_id', 'customer_name'])


#product function
@st.cache_data
def load_products():
    if os.path.exists(csv_sales):
        df = pd.read_csv(csv_sales, encoding='ISO-8859-1')
        return df[['product_id', 'product_name', 'price']].drop_duplicates()
    return pd.DataFrame(columns=['product_id', 'product_name', 'price'])


#retrieve_sales function
@st.cache_data
def load_sales_db():
    try:
        df = pd.read_sql("SELECT * FROM sales ORDER BY sale_date DESC", engine)
        return df
    except:
        columns = [
            'sale_id', 'customer_id', 'product_id', 'quantity', 'price',
            'sale_date'
        ]
        df = pd.DataFrame(columns=columns)
        df.to_sql('sales', engine, index=False, if_exists='replace')
        return df


#store_sales function
def save_sale(customer_name, product_name, quantity):
    df_customers = load_customers()
    df_products = load_products()

    # Buscar cliente
    customer_row = df_customers[df_customers['customer_name'] == customer_name]
    if customer_row.empty:
        return False, "Customer not found!"
    customer_id = int(customer_row.iloc[0]['customer_id'])

    # Buscar produto
    product_row = df_products[df_products['product_name'] == product_name]
    if product_row.empty:
        return False, "Product not found!"
    product_id = int(product_row.iloc[0]['product_id'])
    price = float(product_row.iloc[0]['price'])

    #retrieve last_sale_id in the database
    with engine.connect() as conn:
        last_id_result = conn.execute(
            text("SELECT MAX(sale_id) FROM sales")).fetchone()
    sale_id = int(
        last_id_result[0]) + 1 if last_id_result and last_id_result[0] else 1
    #germany timezone
    germany_tz = pytz.timezone("Europe/Berlin")
    sale_date = datetime.datetime.now(germany_tz).strftime("%Y-%m-%d %H:%M:%S")
    new_sale = {
        'sale_id': sale_id,
        'customer_id': customer_id,
        'product_id': product_id,
        'quantity': quantity,
        'price': price,
        'sale_date': sale_date
    }

    #persistance in the database
    pd.DataFrame([new_sale]).to_sql('sales',
                                    engine,
                                    index=False,
                                    if_exists='append')
    return True, "Sale registered successfully!"


#streamlit interface
st.title("🛒 Sales Registration")
st.markdown("---")

df_customers = load_customers()
df_products = load_products()
df_sales_db = load_sales_db()

customer_options = ["Select a customer"
                    ] + df_customers['customer_name'].tolist()
product_options = ["Select a product"] + df_products['product_name'].tolist()

col1, col2, col3 = st.columns([3, 3, 1])
with col1:
    customer_selected = st.selectbox("Customer Name", customer_options)
with col2:
    product_selected = st.selectbox("Product Name", product_options)
with col3:
    quantity = st.number_input("Quantity", min_value=1, value=1, step=1)

#price and amount
if product_selected != "Select a product" and not df_products.empty:
    price = df_products[df_products['product_name'] ==
                        product_selected]['price'].iloc[0]
    total = price * quantity
    st.text_input("Unit Price", f"{price:.2f}", disabled=True)
    st.text_input("Total Value", f"{total:.2f}", disabled=True)
    st.text_input(
        "Date & Time",
        datetime.datetime.now(pytz.timezone("Europe/Berlin")).strftime("%Y-%m-%d %H:%M:%S"),
        disabled=True
    )

#register sale button
if st.button("💾 Register Sale"):
    if customer_selected == "Select a customer" or product_selected == "Select a product":
        st.error("Please select customer and product.")
    else:
        success, msg = save_sale(customer_selected, product_selected, quantity)
        if success:
            st.success(msg)
            #update sale after inserting
            df_sales_db = load_sales_db()
        else:
            st.error(msg)

st.markdown("---")

#download sale csv
st.download_button("Download sales.csv",
                   df_sales_db.to_csv(index=False),
                   file_name="sales.csv")
