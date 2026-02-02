import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

print(os.getenv('MYSQL_DB_PASSWORD'))

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password=os.getenv('MYSQL_DB_PASSWORD'),
    database="sakila",
    port=3306
)

cursor = conn.cursor()

cursor.execute("SHOW TABLES;")
for table in cursor.fetchall():
    print(table)

cursor.execute("""
    SELECT film_id, title
    FROM film
    LIMIT 5;
""")
results = cursor.fetchall()

for row in results:
    print(row)