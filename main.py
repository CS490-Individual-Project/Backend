from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
CORS(app)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password=os.getenv('MYSQL_DB_PASSWORD'),
    database="sakila",
    port=3306
)

cursor = conn.cursor()

@app.route('/api/top5rented', methods=['GET'])
def get_top5rented():
    #run sql query
    cursor.execute("""
        select f.film_id, f.title, count(f.film_id) as rental_count
        from sakila.rental r
        join sakila.inventory i on r.inventory_id = i.inventory_id 
        join sakila.film f on i.film_id = f.film_id
        group by f.film_id, f.title
        order by rental_count desc limit 5;
    """)
    #store in results
    results = cursor.fetchall()

    #process results into json format
    films = []
    for row in results:
        films.append({
            'film_id': row[0],
            'title': row[1],
            'rental_count': row[2]
        })
    return jsonify(films)

if __name__ == '__main__':
    app.run(debug=True, port=5000)