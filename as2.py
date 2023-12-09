import psycopg2
from psycopg2 import OperationalError
from datetime import date
import os
from dotenv import load_dotenv

load_dotenv()    

def connect_to_database():
    try:
        # Database connection parameters
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        
        # Establish the database connection
        connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        connection.autocommit = True
        # Connection successful, perform database operations here
        print("Connected to the database!")
        return connection

    except OperationalError as e:
        # Handle OperationalError (Database connection failure)
        print(f"Error: {e}")
        return None


############################################################################

# # 3rd part of assignment, implementing CRUD operations
# def insert_authors(connection, authors_data):
#         try:
            
#             query = "INSERT INTO authors (author_name, birthdate, nationality) VALUES (%s, %s, %s) RETURNING author_id;"
#             cursor = connection.cursor()

#             author_ids = []
#             for author in authors_data:
#                  cursor.execute(query, author)
#                  author_id = cursor.fetchone()[0]
#                  author_ids.append(author_id)

#             connection.commit()
#             print(f"{len(authors_data)} new authors created with IDs: {author_ids}")
#             return author_ids

#         except psycopg2.Error as e:
#             print(f"Error during insertion: {e}")
#         finally:
#             cursor.close()            


# def insert_books(connection, books_data):
#         try:

#             query = "INSERT INTO books ( title, stock_quantity , price, author_id) VALUES (%s, %s, %s, %s);"
#             cursor = connection.cursor()
            
#             for book in books_data:
#                  cursor.execute(query, book)

#             connection.commit()
#             print(f"{len(books_data)} new books records created successfully!")
            
#         except psycopg2.Error as e:
#              print(f"Error during insertion: {e}")
#         finally:
#             cursor.close()

# #my examples about authors and books
# authors_data = [
#      ("J.K. Rowling", date(1965, 7, 31), "British"),
#      ("George Orwell",date(1903, 6, 25) , "British"),
#      ("Delia Owens", date(1949, 4, 4), "American"),
#      ("To Kill a Mockinbird", date(1926,4, 25), "American")
# ]


# books_data = [
#      ("Harry Potter and Sorcerer's Stone", 100, 17.55, 1),
#      ("1984", 50, 19.84, 2),
#      ("Where the Crawdads Sing", 133, 19.89, 3),
#      ("To Kill a Mockinbird", 75, 14.99, 4)
# ]

# #usage
# db_connection = connect_to_database()


# #checking if connection is successfull
# if db_connection is not None:
#           author_ids = insert_authors(db_connection, authors_data)
#           insert_books(db_connection, [(title, quantity, price, author_ids[i % len(author_ids)]) for i, (title, quantity, price, _) in enumerate(books_data)] )
          
#       ##    closing conn
#           db_connection.close() 




def retrieve_all_books_information(connection):
    try:
        query = """
            SELECT
                b.title,
                b.stock_quantity,
                b.price,
                a.author_name,
                o.order_id
            FROM
                books b
                JOIN authors a ON b.author_id = a.author_id
                LEFT JOIN orderitems oi ON b.book_id = oi.book_id
                LEFT JOIN orders o ON oi.order_id = o.order_id;                          

        """                    
        cursor = connection.cursor()
        cursor.execute(query)

##

        #fetching all records
        books_info = cursor.fetchall()
        for record in books_info:
            print(record)

    except psycopg2.Error as e:
        print(f"Error: {e}")      


def update_book_details(connection, book_id, new_price):
    try:
        query = "UPDATE books SET price = %s WHERE book_id = %s;"
        values = (new_price, book_id) 

        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()

        print("Book details updated successfully!")

    except psycopg2.Error as e:
        print(f"Error: {e}")

def remove_book(connection, book_id):
    try:
        query = "DELETE FROM books WHERE book_id = %s;"
        values = (book_id,)

        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()

        print("Book removed successfully!")


    except psycopg2.Error as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    db_connection = connect_to_database()

    if db_connection is not None:
        retrieve_all_books_information(db_connection)
        db_connection.close()


this is about retr
########################################################################################
#look again
#if __name__ =="__main__":
#    db_connection = connect_to_database()

#    if db_connection:
#        insert_new_book(db_connection, "Unique Book", 30, 29.99, 2 )
#        retrieve_all_books_information(db_connection)
#        update_book_details(db_connection, 1, 34.99)
#        remove_book(db_connection, 3)

#        db_connection.close()


# ####################################################################

# #task 4
# def get_stock_quantity(cursor, book_id):
#     try:
#         query = "SELECT quantity_in_stock FROM books WHERE book_id = %s;"
#         cursor.execute(query, (book_id,))
#         result = cursor.fetchone()
#         return result[0] if result else None
    
#     except psycopg2.Error as e:
#         print(f"Error: {e}")
#         return None


# def place_order(connection, customer_id, book_id, quantity):
#     try:
#         current_stock = get_stock_quantity(connection.cursor(), book_id)
#         if current_stock is not None and current_stock >= quantity:
#             with connection:
#                 with connection.cursor() as cursor:
#                     #inserting into Orders table
#                     order_query = "INSERT INTO orders (customer_id, order_date) VALUES (%s, NOW()) RETURNING order"
#                     order_values = (customer_id,)
#                     cursor.execute(order_query, order_values)
#                     order_id = cursor.fetchone()[0]

#                     #inserting into orderItems table
#                     order_item_query = "INSERT INTO orderitems (order_id, book_id, quantity) VALUES (%s, %s, %s);"
#                     order_item_values = (order_id, book_id, quantity)
#                     cursor.execute(order_item_query, order_item_values)

#                     #updating books table (reducing stock quantity)
#                     update_query = "UPDATE books SET quantity_in_stock = quantity_in_stock - %s WHERE book_id = %s;"
#                     update_values = (quantity, book_id)
#                     cursor.execute(update_query, update_values)

#                 connection.commit()

#                 print(f"Order placed successfully! Order ID: {order_id}")
#         else:
#             print("Not enough books in the inventory.")

#     except psycopg2.Error as e:
#         print(f"Error: {e}")                   

# ############################################################33  

# #task 5

# def get_table_names(cursor):
#     try:
#         query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
#         cursor.execute(query)
#         tabel_names = cursor.fetchall()
#         return [name[0] for name in table_names]
    
#     except psycopg2.Error as e:
#         print(f"Error: {e}")
#         return []

# def get_table_structure(cursor, table_name):
#     try:
#         query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
#         cursor.execute(query)
#         return cursor.fetchall()
#     except psycopg2.Error as e:
#         print(f"Error: {e}")
#         return []   
    
# def get_primary_keys(cursor, table_name):
#     try:
#         query = f"SELECT column_name FROM information_schema.key_column_usage WHERE table_name = '{table_name}' AND constraint_name LIKE '%%_pkey';"
#         cursor.execute(query)
#         primary_keys = cursor.fetchall()
#         return [key[0] for key in primary_keys]
    
#     except psycopg2.Error as e:
#         print(f"Error: {e}")
#         return []
    
# def get_foreign_keys(cursor, table_name):
#     try:
#         query = f"SELECT column_name, foreign_table_name, foreign_column_name FROM information_schema.foreign_key_column_usage WHERE table_name = '{table_name}';"
#         cursor.execute(query)
#         return cursor.fetchall()
#     except psycopg2.Error as e:
#         print(f"Error: {e}")
#         return []

# # Example of usage
# if __name__ == "__main__":
#     db_connection = connect_to_database()

#     if db_connection:
#         try:
#             with db_connection.cursor() as cursor:
#                 # getting  and displaying table names
#                 table_names = get_table_names(cursor)
#                 print("Table Names:", table_names)

#                 # displaying structure of each table
#                 for table_name in table_names:
#                     print(f"\nTable Structure for {table_name}:")
#                     table_structure = get_table_structure(cursor, table_name)
#                     for column_name, data_type in table_structure:
#                         print(f"  {column_name}: {data_type}")

#                     # Display primary keys
#                     primary_keys = get_primary_keys(cursor, table_name)
#                     print("Primary Keys:", primary_keys)

#                     # Display foreign keys
#                     foreign_keys = get_foreign_keys(cursor, table_name)
#                     print("Foreign Keys:")
#                     for column_name, foreign_table, foreign_column in foreign_keys:
#                         print(f"  {column_name} references {foreign_table}({foreign_column})")

#         finally:
#             # Close the connection
#             db_connection.close()