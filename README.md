# pizza_sales_pipeline
Python ETL pipeline and SQL database for pizza sales data

# Pizza Sales ETL Pipeline

## Project Overview
This project demonstrates a complete **data ingestion, cleaning, and loading (ETL) pipeline** using Python and Microsoft SQL Server.  
It simulates a real-world workflow where raw CSV files are processed, transformed, and inserted into a relational database for analysis.

The main objectives are:

- Load raw pizza sales data from CSV files into Python.
- Inspect and clean the data.
- Transform columns (e.g., dates, times, and feature engineering).
- Rename columns to match database schema.
- Create SQL Server tables to store cleaned data.
- Load the transformed data into SQL Server.
---

## Project Structure
pizza-sales-project/
├── data/ # Raw CSV files
│ ├── order_details.csv
│ ├── orders.csv
│ ├── pizza_types.csv
│ └── pizzas.csv
├── python/ # Python ETL scripts
│ └── data_pipeline.py
├── sql/ # SQL schema and table creation
│ └── database_schema.sql
└── README.md


---

## Pipeline Stages

### 1. Data Ingestion
CSV files are read into **Pandas DataFrames**.  
The datasets include:

- `order_details.csv` – pizza items per order  
- `orders.csv` – order timestamps  
- `pizza_types.csv` – pizza recipes and ingredients  
- `pizzas.csv` – pizza size and price  

```python
import pandas as pd
import os

csv_files = [("order_details.csv", "order_details"),
             ("orders.csv", "orders"),
             ("pizza_types.csv", "pizza_types"),
             ("pizzas.csv", "pizzas")]

path = "data/"
dfs = [pd.read_csv(os.path.join(path, f[0]), encoding="latin-1") for f in csv_files]

## 2. Data Inpection
for i in range(len(dfs)):
  dfs[i].head()
  dfs[i].info()
  dfs[i].describe(include="all")
  dfs[i].duplicated().sum()

## 3. Data Cleaning and Transformation

## 4. SQL Server Table Creation
CREATE TABLE order_details (
    order_details_id INT,
    order_id INT,
    pizza_id VARCHAR(20),
    quantity INT
);

CREATE TABLE orders (
    order_id INT,
    order_date DATETIME,
    order_time DATETIME
);

CREATE TABLE pizza_types (
    pizza_type_id VARCHAR(15),
    pizza_name VARCHAR(70),
    category VARCHAR(20),
    ingredients VARCHAR(150),
    total_ingredients INT
);

CREATE TABLE pizzas (
    pizza_id VARCHAR(15),
    pizza_type_id VARCHAR(15),
    size VARCHAR(5),
    price FLOAT
);

## 5. Loading Data into SQL Server
import pyodbc

driver = 'SQL Server'
server = 'YOUR_SERVER_NAME'
database = 'PIZZA'

conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

for df, table_name in zip(dfs, ["order_details","orders","pizza_types","pizzas"]):
    placeholders = ",".join(["?"]*len(df.columns))
    columns = ",".join([f"[{col}]" for col in df.columns])
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        cursor.execute(sql, tuple(row))
    conn.commit()




