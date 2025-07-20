import pandas as pd
import psycopg2
from psycopg2 import sql
import json

# Column mapping
column_mapper = {
    "Products ID": "product_id",  # we’ll use this to create the SKU
    "Title": "title",
    "Description": "description",
    "Category": "category",
    "SubCategory": "subcategory",
    "Price": "price",
    "Discount Percentage": "discount_percentage",
    "Rating": "rating",
    "Stock": "stock",
    "Brand": "brand",
    "Sku": "sku",  # will be overridden with product_id
    "Weight": "weight",
    "Warranty Information": "warranty_information",
    "Shipping Information": "shipping_information",
    "Availability Status": "availability_status",
    "Return Policy": "return_policy",
    "Minimum Order Quantity": "minimum_order_quantity",
    "Thumbnail": "thumbnail",
    "Version": "version"
}

# Default values
default_values = {
    "title": "N/A",
    "sku": "UNKNOWN-SKU",
    "description": "",
    "category": "General",
    "subcategory": "Misc",
    "price": 0.0,
    "discount_percentage": 0.0,
    "rating": 0.0,
    "stock": 0,
    "brand": "Unknown",
    "weight": 0.0,
    "warranty_information": "No warranty info",
    "shipping_information": "Standard shipping",
    "availability_status": "Unavailable",
    "return_policy": "No returns",
    "minimum_order_quantity": 1,
    "thumbnail": "",
    "version": "1.0",
    "images": "[]"
}

# Load Excel
df = pd.read_excel("Untitled spreadsheet.ods")

print(f"⚠️\n Value Title: {df}")


# Filter columns based on mapping
# valid_excel_columns = [col for col in column_mapper.keys() if col in df.columns]
# filtered_mapper = {col: column_mapper[col] for col in valid_excel_columns}
# df = df[valid_excel_columns]
# df.rename(columns=filtered_mapper, inplace=True)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="seller_dashboard",
    user="postgres",
    password="root"
)
cursor = conn.cursor()

# Iterate and insert
for index, row in df.iterrows():
    row_dict = row.where(pd.notnull(row), None).to_dict()
    # print(f"⚠️\n Value Title: {row_dict}")
    # print(f"======================--------------------")

    # Skip rows with empty title
    if not row_dict.get("title"):
        # print(f"⚠️\n Skipped row with empty Title: {row_dict}")
        continue

    # Use product_id as SKU
    product_id = row_dict.get("product_id")
    if product_id:
        row_dict["sku"] = str(product_id)
    else:
        row_dict["sku"] = default_values["sku"]

    # Create images as JSON array string using thumbnail
    thumbnail = row_dict.get("thumbnail") or ""
    row_dict["images"] = json.dumps([thumbnail]) if thumbnail else json.dumps([])

    # Fill in missing default values
    for key in default_values:
        if row_dict.get(key) in [None, ""]:
            row_dict[key] = default_values[key]

    # Drop product_id from insertion since it's not part of DB columns
    row_dict.pop("product_id", None)

    try:
        columns = row_dict.keys()
        values = [row_dict[col] for col in columns]

        insert_query = sql.SQL("INSERT INTO products ({}) VALUES ({})").format(
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        cursor.execute(insert_query, values)
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to insert row: {row_dict} \nError: {e}")
    else:
        conn.commit()

# Cleanup
cursor.close()
conn.close()
print("✅ Import completed.")
