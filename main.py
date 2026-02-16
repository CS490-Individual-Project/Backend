from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

app = Flask(__name__)
CORS(app)

def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password=os.getenv('MYSQL_DB_PASSWORD'),
        database="sakila",
        port=3306,
        ssl_disabled=True,
        autocommit=False,
        connection_timeout=10
    )


def fetch_all(query, params=None):
    normalized_params = params if params is not None else ()
    last_error = None

    for _ in range(2):
        conn = None
        cursor = None
        try:
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute(query, normalized_params)
            return cursor.fetchall()
        except (mysql.connector.InterfaceError, mysql.connector.OperationalError) as error:
            last_error = error
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except mysql.connector.Error:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except mysql.connector.Error:
                    pass

    raise last_error


def execute_write(query, params=None):
    normalized_params = params if params is not None else ()
    last_error = None

    for _ in range(2):
        conn = None
        cursor = None
        try:
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute(query, normalized_params)
            conn.commit()
            return cursor.lastrowid
        except (mysql.connector.InterfaceError, mysql.connector.OperationalError) as error:
            last_error = error
            if conn is not None and conn.is_connected():
                conn.rollback()
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except mysql.connector.Error:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except mysql.connector.Error:
                    pass

    raise last_error

'''
Landing Page (index.html)
'''
#Feature 1: As a user I want to view top 5 rented films of all times
@app.route('/api/top5rented', methods=['GET'])
def get_top_five_rented():
    #run sql query
    #store in results
    results = fetch_all("""
        select f.film_id, f.title, count(f.film_id) as rental_count
        from sakila.rental r
        join sakila.inventory i on r.inventory_id = i.inventory_id 
        join sakila.film f on i.film_id = f.film_id
        group by f.film_id, f.title
        order by rental_count desc limit 5;
    """)

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
    #store in results
    results = fetch_all("""
        select a.actor_id, a.first_name, a.last_name, count(fa.film_id) as movies
        from sakila.film_actor fa 
        join sakila.actor a on fa.actor_id = a.actor_id
        group by a.actor_id 
        order by movies desc limit 5;
    """)

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
    actor_id = request.args.get('actor_id')
    if not actor_id:
        return jsonify({'error': 'Missing required query parameter: actor_id'}), 400

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
    #store in results
    results = fetch_all(query, (actor_id,))

    if not results:
        return jsonify([])

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
    search_term = request.args.get('search', '')

    if not search_term or not search_term.isalnum():
        return jsonify({'error': 'Invalid search term. Please enter a valid search term.'}), 400

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
    results = fetch_all(query, (search_term, search_term, search_term, search_term))

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
    film_id = request.args.get('film_id')
    if not film_id:
        return jsonify({'error': 'Missing required query parameter: film_id'}), 400

    query = """select f.film_id, f.title, f.description, f.release_year, f.language_id,
                    f.original_language_id, f.rental_duration, f.rental_rate, f.length,
                    f.replacement_cost, f.rating, f.special_features, f.last_update,
                    l.name as language_name
                from sakila.film f
                join sakila.language l on f.language_id = l.language_id
                where f.film_id = %s;"""

    #run sql query
    #store in results
    results = fetch_all(query, (film_id,))

    if not results:
        return jsonify({'error': 'Film not found'}), 404

    row = results[0]

    #process results into json format
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
        'special_features': sorted(list(row[11])) if isinstance(row[11], set) else row[11], #Convert python set to json list
        'last_update': row[12],
        'language': row[13]
    }]

    return jsonify(films)

#Feature 7: As a user I want to be able to rent a film out to a customer
@app.route('/api/rentfilm', methods=['PUT'])
def rent_film():
    try:
        # Get data from request body
        data = request.get_json()
        required_fields = ['rental_date', 'film_id', 'customer_id']
        
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}.'}), 400

        inventory_rows = fetch_all("""select inventory_id from sakila.inventory where film_id = %s;""", (data['film_id'],))
        inventory_id = None
        for row in inventory_rows:
            if not fetch_all("""select rental_id from sakila.rental where inventory_id = %s and return_date is null limit 1;""", (row[0],)):
                inventory_id = row[0]
                break
        if inventory_id is None:
            return jsonify({'error': 'No inventory available.'}), 400
        
        query = """insert into sakila.rental (rental_date, inventory_id, customer_id, return_date, staff_id)
                   values (%s, %s, %s, NULL, 1)"""

        execute_write(query, (
            data['rental_date'],
            inventory_id,
            data['customer_id']
        ))

        return jsonify({'message': f"Film rented successfully to customer {data['customer_id']}"}), 200

    except Exception as e:
        # reset database if error occurs
        return jsonify({'error': 'Error! Unable to rent film.'}), 400

'''
Customer Page (customer.html)
'''
#Feature 8: As a user I want to view a list of all customers (Pref. using pagination)
@app.route('/api/allcustomers', methods=['GET'])
def get_all_customers():
    #run sql query
    #store in results
    results = fetch_all("""select * from sakila.customer;""")

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
    search_term = request.args.get('search', '')

    query = """select * from sakila.customer
                where customer_id like %s
                or first_name like %s
                or last_name like %s;"""

    search_term = f"%{search_term}%"
    results = fetch_all(query, (search_term, search_term, search_term))

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

        last_row_id = execute_write(query, (
            data['store_id'],
            data['first_name'],
            data['last_name'],
            data['email'],
            data['address_id'],
            create_date
        ))
        
        return jsonify({
            'message': 'Customer added successfully',
            'customer_id': last_row_id
        }), 201
    except Exception as e:
        return jsonify({'error': 'Error adding customer. Please try again.'}), 500

#Feature 11: As a user I want to be able to edit a customer’s details
@app.route('/api/editcustomer', methods=['PUT'])
def edit_customer():
    pass

#Feature 12: As a user I want to be able to delete a customer if they no longer wish to patron at store
@app.route('/api/deletecustomer', methods=['PUT'])
def delete_customer():
    customer_id = request.args.get('customer_id')
    if not customer_id:
        return jsonify({'error': 'Missing required query parameter: customer_id'}), 400

    try:
        active_rentals = fetch_all("""select rental_id from sakila.rental where customer_id = %s and return_date is null limit 1;""", (customer_id,))
        if active_rentals:
            return jsonify({'error': 'Customer has active rentals. Cannot delete.'}), 400

        query = """delete from sakila.customer where customer_id = %s;"""
        execute_write(query, (customer_id,))

        return jsonify({'message': 'Customer deleted successfully'}), 200
    except Exception as e:
        return jsonify({'error': 'Error deleting customer. Please try again.'}), 500

#Feature 13: As a user I want to be able to view customer details and see their past and present rental history
@app.route('/api/get_customerdetails', methods=['GET'])
def get_customer_details():
    customer_id = request.args.get('customer_id')
    if not customer_id:
        return jsonify({'error': 'Missing required query parameter: customer_id'}), 400

    query = """select * from sakila.film where film_id = %s;"""

    #run sql query
    #store in results
    results = fetch_all(query, (customer_id,))

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

#Feature 14: As a user I want to be able to indicate that a customer has returned a rented movie 
@app.route('/api/returnfilm', methods=['PUT'])
def return_film():
    try:
        data = request.get_json()
        required_fields = ['customer_id', 'rental_id']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400

        # check validity of rental_id
        if not data['rental_id'] or not data['rental_id'].isdigit():
            return jsonify({'error': 'Invalid input for rental_id.'}), 400
        
        # check if rental exists
        existing_rental = fetch_all("""select rental_id from sakila.rental where rental_id = %s
                                        and customer_id = %s
                                        and return_date is null limit 1;""",
                                        (data['rental_id'], data['customer_id']))

        if not existing_rental:
            return jsonify({'error': 'Rental does not exist or is already returned.'}), 400

        query = """update sakila.rental set return_date = %s where rental_id = %s;"""
        execute_write(query, (
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            data['rental_id']
        ))

        return jsonify({'message': 'Film returned successfully'}), 200
    except Exception as e:
        return jsonify({'error': 'Error returning film. Please try again.'}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
