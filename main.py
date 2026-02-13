from flask import Flask, jsonify, request, g
from flask_cors import CORS
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime
from werkzeug.local import LocalProxy

load_dotenv()

app = Flask(__name__)
CORS(app)

def get_db():
    if 'db_conn' not in g:
        g.db_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password=os.getenv('MYSQL_DB_PASSWORD'),
            database="sakila",
            port=3306
        )
        g.db_cursor = g.db_conn.cursor()
    return g.db_conn, g.db_cursor


def _get_conn():
    return get_db()[0]


def _get_cursor():
    return get_db()[1]


conn = LocalProxy(_get_conn)
cursor = LocalProxy(_get_cursor)


@app.teardown_appcontext
def close_db(error=None):
    db_cursor = g.pop('db_cursor', None)
    db_conn = g.pop('db_conn', None)

    if db_cursor is not None:
        db_cursor.close()

    if db_conn is not None and db_conn.is_connected():
        db_conn.close()

'''
Landing Page (index.html)
'''
#Feature 1: As a user I want to view top 5 rented films of all times
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

#Feature 2: As a user I want to be able to click on any of the top 5 films and view its details
#Just call get_film_details(film_id) function

#Feature 3: As a user I want to be able to view top 5 actors that are part of films I have in the store
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

#Feature 4: As a user I want to be able to view the actor’s details and view their top 5 rented films
@app.route('/api/get_actordetails', methods=['GET'])
def get_actor_details():
    actor_id = request.args.get('actor_id', type=int)
    if not actor_id:
        return jsonify({'error': 'actor_id is required'}), 400

    query = """select actor.actor_id, actor.first_name, actor.last_name, film.title, count(rental.rental_id)
                from sakila.actor
                join sakila.film_actor on actor.actor_id = film_actor.actor_id
                join sakila.film on film_actor.film_id = film.film_id
                join sakila.inventory on film.film_id = inventory.film_id
                join sakila.rental on inventory.inventory_id = rental.inventory_id
                where actor.actor_id = %s
                group by film.film_id
                order by count(film.film_id) desc limit 5;
            """

    #run sql query
    cursor.execute(query, (actor_id,))

    #store in results
    results = cursor.fetchall()

    #process results into json format
    actor_details = []
    for row in results:
        actor_details.append({
            'actor_id': row[0],
            'first_name': row[1],
            'last_name': row[2],
            'title': row[3],
            'rental_count': row[4]
        })

    return jsonify(actor_details)

'''
Films Page (films.html)
'''
#Feature 5: As a user I want to be able to search a film by name of film, name of an actor, or genre of the film
@app.route('/api/searchfilms', methods=['GET'])
def search_films():
    search_term = request.args.get('search', '').strip()
    if not search_term:
        return jsonify([])

    query = """select distinct f.film_id, f.title, f.description, f.release_year, f.rating, c.name as category,
                    group_concat(distinct concat(a.first_name, ' ', a.last_name) separator ', ') as actors
                from sakila.film f
                join sakila.film_actor fa on f.film_id = fa.film_id
                join sakila.actor a on fa.actor_id = a.actor_id
                join sakila.film_category fc on f.film_id = fc.film_id
                join sakila.category c on fc.category_id = c.category_id
                where f.title like %s
                or a.first_name like %s
                or a.last_name like %s
                or c.name like %s
                group by f.film_id, f.title, f.description, f.release_year, f.rating, c.name;"""
    
    #updated to allow partial matching
    search_term = f"%{search_term}%"
    cursor.execute(query, (search_term, search_term, search_term, search_term))

    results = cursor.fetchall()

    films = []
    for row in results:
        films.append({
            'film_id': row[0],
            'title': row[1],
            'description': row[2],
            'release_year': row[3],
            'rating': row[4],
            'category': row[5],
            'actors': row[6]
        })
    return jsonify(films)

#Feature 6: As a user I want to be able to view details of the film
@app.route('/api/get_filmdetails', methods=['GET'])
def get_film_details():
    film_id = request.args.get('film_id', type=int)
    if not film_id:
        return jsonify({'error': 'film_id is required'}), 400

    query = """select * from sakila.film where film_id = %s;"""

    #run sql query
    cursor.execute(query, (film_id,))

    #store in results
    results = cursor.fetchall()

    #process results into json format
    if not results:
        return jsonify({'error': 'Film not found'}), 404

    row = results[0]
    films = [{
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
        'last_update': row[12]
    }]

    return jsonify(films)

#Feature 7: As a user I want to be able to rent a film out to a customer
@app.route('/api/rentfilm', methods=['PUT'])
def rent_film():
    try:
        # Get data from request body
        data = request.get_json()
        required_fields = ['rental_date', 'inventory_id', 'customer_id', 'return_date']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}.'}), 400
        query = """insert into sakila.rental (rental_date, inventory_id, customer_id, return_date, staff_id) 
                   values (%s, %s, %s, %s, 1)"""
        
        cursor.execute(query, (
            data['rental_date'],
            data['inventory_id'],
            data['customer_id'],
            data['return_date']
        ))
        conn.commit()
        return jsonify({'message': f'Film rented successfully to customer {data['customer_id']}'}), 200
    
    except Exception as e:
        # reset database if error occurs
        conn.rollback()
        return jsonify({'error': 'Error! Unable to add customer.'}), 400

'''
Customer Page (customer.html)
'''
#Feature 8: As a user I want to view a list of all customers (Pref. using pagination)
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

#Feature 9: As a user I want the ability to filter/search customers by their customer id, first name or last name.
@app.route('/api/searchcustomers', methods=['GET'])
def search_customers():
    search_term = request.args.get('search', '').strip()
    if not search_term:
        return jsonify([])

    query = """select * from sakila.customer
                where customer_id like %s
                or first_name like %s
                or last_name like %s;"""

    search_term = f"%{search_term}%"
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

#Feature 10: As a user I want to be able to add a new customer
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
            
        query = """insert into sakila.customer (store_id, first_name, last_name, email,
                   address_id, create_date) values (%s, %s, %s, %s, %s, %s)"""

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

#Feature 11: As a user I want to be able to edit a customer’s details
@app.route('/api/editcustomer', methods=['PUT'])
def edit_customer():
    pass

#Feature 12: As a user I want to be able to delete a customer if they no longer wish to patron at store
@app.route('/api/deletecustomer', methods=['PUT'])
def delete_customer():
    data = request.get_json(silent=True) or {}
    customer_id = data.get('customer_id')
    if customer_id is None:
        customer_id = request.args.get('customer_id', type=int)

    if not customer_id:
        return jsonify({'error': 'customer_id is required'}), 400

    try:
        query = """delete from sakila.customer where customer_id = %s;"""

        cursor.execute(query, (customer_id,))

        conn.commit()

        return jsonify({'message': 'Customer deleted successfully'}), 200
    except Exception as e:
        # reset database if error occurs
        conn.rollback()
        return jsonify({'error': 'Error deleting customer. Please try again.'}), 500

#Feature 13: As a user I want to be able to view customer details and see their past and present rental history
@app.route('/api/get_customerdetails', methods=['GET'])
def get_customer_details():
    customer_id = request.args.get('customer_id', type=int)
    if not customer_id:
        return jsonify({'error': 'customer_id is required'}), 400

    query = """select * from sakila.customer where customer_id = %s;"""

    #run sql query
    cursor.execute(query, (customer_id,))

    #store in results
    results = cursor.fetchall()

    #process results into json format
    if not results:
        return jsonify({'error': 'Customer not found'}), 404

    row = results[0]
    customer_details = [{
        'customer_id': row[0],
        'store_id': row[1],
        'first_name': row[2],
        'last_name': row[3],
        'email': row[4],
        'address_id': row[5],
        'active': row[6] == 1,
        'create_date': row[7],
        'last_update': row[8]
    }]
    return jsonify(customer_details)

#Feature 14: As a user I want to be able to indicate that a customer has returned a rented movie 
@app.route('/api/returnfilm', methods=['PUT'])
def return_film():
    try:
        data = request.get_json()

        required_fields = ['rental_id', 'return_date']

        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
            
        query = """update sakila.rental set return_date = %s where rental_id = %s;"""

        cursor.execute(query, (
            data['return_date'],
            data['rental_id']
        ))

        conn.commit()

        return jsonify({'message': 'Film returned successfully :)'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': 'Error! Unable to return film.'}), 400



if __name__ == '__main__':
    app.run(debug=True, port=5000)