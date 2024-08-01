import pandas as pd
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="mydatabase",
    user="postgres",
    password="password",
    host="localhost"
)

# Fetch data from PostgreSQL
df = pd.read_sql_query("SELECT * FROM stock_data", conn)

# Process data (example: adding a new column)
df['price_range'] = df['high'] - df['low']

# Save processed data to CSV
df.to_csv('processed_stock_data.csv', index=False)

print(df.head())
