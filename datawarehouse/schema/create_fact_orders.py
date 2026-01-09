import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="source_airflow",
    user="postgres",
    password="postgres",
    port=5432
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    name TEXT,
    age INT
)
""")



conn.commit()

cursor.execute("SELECT * FROM orders")
print(cursor.fetchall())

cursor.close()
conn.close()