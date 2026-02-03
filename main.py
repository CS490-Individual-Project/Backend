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

'''
Landing Page (index.html)
'''
#As a user I want to view top 5 rented films of all times
@app.route('/api/top5rented', methods=['GET'])
def get_top_five_rented():
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

#As a user I want to be able to click on any of the top 5 films and view its details


#As a user I want to be able to view top 5 actors that are part of films I have in the store


#As a user I want to be able to view the actor’s details and view their top 5 rented films


'''
Films Page (films.html)
'''
#As a user I want to be able to search a film by name of film, name of an actor, or genre of the film


#As a user I want to be able to view details of the film


#As a user I want to be able to rent a film out to a customer


'''
Customer Page (customer.html)
'''
#As a user I want to view a list of all customers (Pref. using pagination)
@app.route('/api/allcustomers', methods=['GET'])
def get_all_customers():
    #run sql query
    cursor.execute("""select * from sakila.customer;""")

    #store in results
    results = cursor.fetchall()

    #process results into json format
    customers = []
    for row in results:
        customers.append({
            'customer_id': row[0],
            'store_id': row[1],
            'name': row[2] + ' ' + row[3],
            'email': row[4],
            'address': row[5],
            'active': row[6] == 1,
            'create_date': row[7],
            'last_update': row[8]
        })
    return jsonify(customers)

#As a user I want the ability to filter/search customers by their customer id, first name or last name.


#As a user I want to be able to add a new customer


#As a user I want to be able to edit a customer’s details


#As a user I want to be able to delete a customer if they no longer wish to patron at store


#As a user I want to be able to view customer details and see their past and present rental history


#As a user I want to be able to indicate that a customer has returned a rented movie 




if __name__ == '__main__':
    app.run(debug=True, port=5000)