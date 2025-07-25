import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --------------------- CONFIGURATION ---------------------
excel_file = "Untitled spreadsheet.ods"

db_config = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "root"
}

table_name = "products"

column_mapper = {
    "Title": "title",
    "Description": "description",
    "Category": "category",
    "SubCategory": "subcategory",
    "Price": "price",
    "Discount Percentage": "discount_percentage",
    "Rating": "rating",
    "Stock": "stock",
    "Brand": "brand",
    "Weight": "weight",
    "Warranty Information": "warranty_information",
    "Shipping Information": "shipping_information",
    "Availability Status": "availability_status",
    "Return Policy": "return_policy",
    "Minimum Order Quantity": "minimum_order_quantity",
    "Thumbnail": "thumbnail",
    "Version": "version"
}

sku_source_column = "Products ID"

integer_fields = ['stock', 'minimum_order_quantity']
float_fields = ['price', 'discount_percentage', 'rating', 'weight']

table_fields = [
    'title', 'description', 'category', 'subcategory', 'price', 'discount_percentage',
    'rating', 'stock', 'brand', 'weight', 'warranty_information',
    'shipping_information', 'availability_status', 'return_policy',
    'minimum_order_quantity', 'thumbnail', 'version', 'sku'
]

default_values = {
    "title": "Untitled Product",
    "description": "No description available.",
    "category": "Uncategorized",
    "subcategory": "General",
    "price": 0.0,
    "discount_percentage": 0.0,
    "rating": 0.0,
    "stock": 0,
    "brand": "Generic",
    "weight": 0.0,
    "warranty_information": "Not specified",
    "shipping_information": "Standard shipping",
    "availability_status": "In Stock",
    "return_policy": "No return policy",
    "minimum_order_quantity": 1,
    "thumbnail": "",
    "version": "1.0",
    "sku": "UNKNOWN"
}

# --------------------- LOAD AND CLEAN DATA ---------------------
try:
    df = pd.read_excel(excel_file, nrows=200)
    df.columns = df.columns.str.strip()

    if sku_source_column not in df.columns:
        raise Exception(f"Missing required column '{sku_source_column}' in Excel.")

    df['sku'] = df[sku_source_column].astype(str)

    mapped_cols = {k: v for k, v in column_mapper.items() if k in df.columns and v in table_fields}
    df = df.rename(columns=mapped_cols)

    for field in table_fields:
        if field not in df.columns:
            df[field] = None

    df = df[table_fields]

    # Fill missing values with defaults
    for col in table_fields:
        df[col] = df[col].apply(
            lambda x: default_values[col] if pd.isna(x) or str(x).strip().lower() in ["", "nan"] else x
        )

    def safe_int(val):
        try:
            val = float(val)
            if val.is_integer() and abs(val) <= 2_147_483_647:
                return int(val)
            return default_values[col]
        except:
            return default_values[col]

    def safe_float(val):
        try:
            return round(float(val), 2)
        except:
            return default_values[col]

    for col in integer_fields:
        df[col] = df[col].apply(safe_int)

    for col in float_fields:
        df[col] = df[col].apply(safe_float)

    # Drop rows where title is missing or default
    df = df[df['title'].str.strip().str.lower() != default_values['title'].lower()]
    df = df[df['title'].str.strip() != ""]

    if df.empty:
        print("❌ No valid records with non-empty 'title' found. Nothing to insert.")
        exit()

    # Construct thumbnail URL dynamically
    def generate_thumbnail_url(row):
        safe_category = str(row['category']).strip().replace(" ", "%20")
        safe_sku = str(row['sku']).strip()
        return f"https://raw.githubusercontent.com/rakeshsharma869/Extro_related_stuffs/refs/heads/master/All%20Tabs/{safe_category}/{safe_sku}.png"

    df['thumbnail'] = df.apply(generate_thumbnail_url, axis=1)

    values = df.where(pd.notnull(df), None).values.tolist()

except Exception as e:
    print("❌ Data Preparation Error:", e)
    exit()

# --------------------- DATABASE INSERT ---------------------
insert_query = f"""
    INSERT INTO {table_name} ({', '.join(table_fields)})
    VALUES %s
"""

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    execute_values(cursor, insert_query, values)
    conn.commit()
    print(f"✅ Successfully inserted {len(values)} records into '{table_name}'.")
except Exception as e:
    print("❌ Database Error:", e)
finally:
    if 'cursor' in locals(): cursor.close()
    if 'conn' in locals(): conn.close()
