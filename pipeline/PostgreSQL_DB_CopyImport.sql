--CUSTOMERS
CREATE TABLE olist.customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);

copy olist.customers
FROM 'C:\olist\olist_customers_dataset.csv'
DELIMITER ',' 
CSV HEADER 
QUOTE '"';


select * from olist.customers


ORDERS
DROP TABLE IF EXISTS olist.orders;

CREATE TABLE olist.orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

COPY olist.orders
FROM 'C:\olist\olist_orders_dataset.csv"'
DELIMITER ','
CSV HEADER
QUOTE '"';

select * from olist.orders

ORDER ITEMS
DROP TABLE IF EXISTS olist.order_items;

CREATE TABLE olist.order_items (
    order_id TEXT,
    order_item_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price TEXT,
    freight_value TEXT
);

COPY olist.order_items
FROM 'C:\olist\olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER
QUOTE '"';

select * from olist.order_items

ORDER PAYMENTS
DROP TABLE IF EXISTS olist.order_payments;

CREATE TABLE olist.order_payments (
    order_id TEXT,
    payment_sequential TEXT,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value TEXT
);

COPY olist.order_payments
FROM 'C:\olist\olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER
QUOTE '"';

select * from olist.order_payments


--PRODUCT
DROP TABLE IF EXISTS olist.products;

CREATE TABLE olist.products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_length TEXT,
    product_description_length TEXT,
    product_photos_qty TEXT,
    product_weight_g TEXT,
    product_length_cm TEXT,
    product_height_cm TEXT,
    product_width_cm TEXT
);

COPY olist.products
FROM 'C:\olist\olist_products_dataset.csv'
DELIMITER ','
CSV HEADER
QUOTE '"';

select * from olist.products

--PRODUCT CATEGORY NAME
DROP TABLE IF EXISTS olist.product_category_name_translation;

CREATE TABLE olist.product_category_name_translation (
    product_category_name TEXT,
    product_category_name_english TEXT
);

COPY olist.product_category_name_translation
FROM 'C:\olist\product_category_name_translation.csv'
DELIMITER ','
CSV HEADER
QUOTE '"';


select * from olist.product_category_name_translation




-----************************
Top Multi-Edit Methods in VS Code
Change All Occurrences (Most Used): Highlight a word and press Ctrl+Shift+L / Cmd+Shift+L. All occurrences in the file will get a cursor, allowing instant editing.


Add Next Occurrence: Highlight a word and press Ctrl+D / Cmd+D to select the next instance, repeating until all desired instances are selected.

Column/Vertical Editing: Hold Alt / Option and click anywhere, or drag vertically to create a cursor across multiple lines.

Keyboard Multi-Cursor: Press Ctrl+Alt+Up/Down (Windows) or Cmd+Option+Up/Down (Mac) to add a cursor above or below the current position.

Find and Replace: Press Ctrl+H / Cmd+H


---******************************++++++++++++++++
1. How to Check What is Taking Up Space
Open the terminal within your running Codespace and run these commands: 
Check overall disk usage:
bash
df -h
This shows how much space is used and free, primarily on the root (/) filesystem.

Find the largest directories:
Run this command in your project root to see which folders are consuming the most space:
bash
du -sh ./* | sort -h COMMAND ENDS BEFORE ETHIS LINE||----------NOT THERE***/


Find large files (greater than 100MB):
bash
find /workspaces -type f -size +100M -exec ls -lh {} \;