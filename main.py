from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime

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
#TODO

#As a user I want to be able to view top 5 actors that are part of films I have in the store
@app.route('/api/top5actors', methods=['GET'])
def get_top_five_actors():
    #run sql query
    cursor.execute("""
        select a.actor_id, a.first_name, a.last_name, count(fa.film_id) as movies
        from sakila.film_actor fa 
        join sakila.actor a on fa.actor_id = a.actor_id
        group by a.actor_id 
        order by movies desc limit 5;
    """)

    #store in results
    results = cursor.fetchall()

    #process results into json format
    actors = []
    for row in results:
        actors.append({
            'actor_id': row[0],
            'name': row[1] + ' ' + row[2],
            'movies': row[3]
        })
    return jsonify(actors)

#As a user I want to be able to view the actor’s details and view their top 5 rented films
#TODO

'''
Films Page (films.html)
'''
#As a user I want to be able to search a film by name of film, name of an actor, or genre of the film
@app.route('/api/searchfilms', methods=['GET'])
def search_films(search_term):
    query = """select * from sakila.films f
                join sakila.film_actor fa on f.film_id = fa.film_id
                join sakila.actor a on fa.actor_id = a.actor_id
                join sakila.film_category fc on f.film_id = fc.film_id
                join sakila.category c on fc.category_id = c.category_id
                where f.title = %s
                or a.first_name = %s
                or a.last_name = %s
                or c.name = %s;"""

    cursor.execute(query, (search_term, search_term, search_term, search_term))

    results = cursor.fetchall()

    films = []
    for row in results:
        films.append({
            'film_id': row[0],
            'title': row[1],
            'description': row[2],
            'release_year': row[3],
            'language_id': row[4],
            'original_language_id': row[5],
            'rental_duration': row[6],
            'rental_rate': row[7],
            'length': row[8],
            'replacement_cost': row[9],
            'rating': row[10],
            'special_features': row[11],
            'last_update': row[12],
        })
    return jsonify(films)

#As a user I want to be able to view details of the film
@app.route('/api/get_filmdetails', methods=['GET'])
def get_film_details(film_id):
    query = """select * from sakila.film where film_id = %s;"""

    #run sql query
    cursor.execute(query, (film_id))

    #store in results
    results = cursor.fetchall()

    #process results into json format
    films = [{
        'film_id': results[0],
        'title': results[1],
        'description': results[2],
        'release_year': results[3],
        'language_id': results[4],
        'original_language_id': results[5],
        'rental_duration': results[6],
        'rental_rate': results[7],
        'length': results[8],
        'replacement_cost': results[9],
        'rating': results[10],
        'special_features': results[11],
        'last_update': results[12]
    }]

    return jsonify(films)

#As a user I want to be able to rent a film out to a customer
@app.route('/api/rentfilm', methods=['PUT'])
def rent_film():
    pass

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
            'first_name': row[2],
            'last_name': row[3],
            'email': row[4],
            'address': row[5],
            'active': row[6] == 1,
            'create_date': row[7],
            'last_update': row[8]
        })
    return jsonify(customers)

#As a user I want the ability to filter/search customers by their customer id, first name or last name.
@app.route('/api/searchcustomers', methods=['GET'])
def search_customers(search_term):
    query = """select * from sakila.customer
                where customer_id = %s
                or first_name = %s
                or last_name = %s;"""

    cursor.execute(query, (search_term, search_term, search_term))

    results = cursor.fetchall()

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

#As a user I want to be able to add a new customer
@app.route('/api/addcustomer', methods=['POST'])
def add_customer():
    try:
        # Get data from request body
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['store_id', 'first_name', 'last_name', 'email', 'address_id']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        create_date = data.get('create_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
        query = """INSERT INTO sakila.customer (store_id, first_name, last_name, email,
                    address_id, create_date) VALUES (%s, %s, %s, %s, %s, %s)"""

        cursor.execute(query, (
            data['store_id'],
            data['first_name'],
            data['last_name'],
            data['email'],
            data['address_id'],
            create_date
        ))
        
        conn.commit()
        
        return jsonify({
            'message': 'Customer added successfully',
            'customer_id': cursor.lastrowid
        }), 201
    except Exception as e:
        conn.rollback()
        return jsonify({'error': 'Error adding customer. Please try again.'}), 500

#As a user I want to be able to edit a customer’s details
@app.route('/api/editcustomer', methods=['PUT'])
def edit_customer():
    pass

#As a user I want to be able to delete a customer if they no longer wish to patron at store
@app.route('/api/deletecustomer', methods=['PUT'])
def delete_customer(customer_id):
    try:
        query = """DELETE FROM sakila.customer WHERE customer_id = %s;"""

        cursor.execute(query, (customer_id))

        conn.commit()

        return jsonify({'message': 'Customer deleted successfully'}), 200
    except Exception as e:
        # reset database if error occurs
        conn.rollback()
        return jsonify({'error': 'Error deleting customer. Please try again.'}), 500

#As a user I want to be able to view customer details and see their past and present rental history
@app.route('/api/get_customerdetails', methods=['GET'])
def get_customer_details(customer_id):
    query = """select * from sakila.film where film_id = %s;"""

    #run sql query
    cursor.execute(query, (customer_id))

    #store in results
    results = cursor.fetchall()

    #process results into json format
    customer_details = [{
        'customer_id': results[0],
        'store_id': results[1],
        'first_name': results[2],
        'last_name': results[3],
        'email': results[4],
        'address': results[5],
        'active': results[6] == 1,
        'create_date': results[7],
        'last_update': results[8]
    }]
    return jsonify(customer_details)

#As a user I want to be able to indicate that a customer has returned a rented movie 
@app.route('/api/returnfilm', methods=['PUT'])
def return_film():
    pass


if __name__ == '__main__':
    app.run(debug=True, port=5000)