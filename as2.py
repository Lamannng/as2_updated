import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv()

def connect_to_database():
    connection = None
    
    
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


# # INSERTION part of assignment, implementing CRUD operations
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
#      ("Harper Lee", date(1926,4, 25), "American")
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
# # 



# #inserting customer table

# def insert_customer(connection, customer_name, email, phone_number):
#     try:
#         query = "INSERT INTO customers (customer_name, email, phone_number) VALUES (%s, %s, %s) RETURNING customer_id;"
#         values = (customer_name, email, phone_number)
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         customer_id = cursor.fetchone()[0]
#         connection.commit()
#         print(f"Customer {customer_name} added with ID: {customer_id}")
#         return customer_id
    
#     except psycopg2.Error as e:
#         print(f"Error during customer insertion: {e}")
#     finally:
#         cursor.close()


# #insertion process of order table
# def insert_order(connection, order_date, customer_id):
#     try:
#         query = "INSERT INTO orders (order_date, customer_id) VALUES (%s, %s) RETURNING order_id;"
#         values = (order_date, customer_id)
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         order_id = cursor.fetchone()[0]
#         connection.commit()
#         print(f"Order added with ID: {order_id}")
#         return order_id
        
        
#     except psycopg2.Error as e:
#         print(f"Error during order insertion: {e}")
#     finally:
#         cursor.close()       


# #insertion process of orderitems table

# def insert_order_item(connection, order_id, book_id, quantity):
#     try:
#         query = """
#             INSERT INTO orderitems (order_id, book_id, quantity)
#             SELECT %s, %s, %s
#             WHERE EXISTS (SELECT 1 FROM books WHERE book_id = %s)
#             RETURNING order_item_id;
#         """
#         values = (order_id, book_id, quantity, book_id)
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         order_item_id = cursor.fetchone()

#         if order_item_id:
#             connection.commit()
#             print(f"Order Item added with ID: {order_item_id[0]}")
#             return order_item_id[0]
#         else:
#             print(f"Error: Book with ID {book_id} does not exist.")

#     except psycopg2.Error as e:
#         print(f"Error during order item insertion: {e}")
#     finally:
#         cursor.close()

# if __name__ == "__main__":
#     db_connection = connect_to_database()

#     # Check if the connection is successful before proceeding
#     if db_connection is not None:

#         author_ids = insert_authors(db_connection, authors_data)
#         insert_books(db_connection, [(title, quantity, price, author_ids[i % len(author_ids)]) for i, (title, quantity, price, _) in enumerate(books_data)])
        
        
#         # Insert four customers
#         customer_ids = []
#         for i in range(1, 5):
#             customer_name = f"Customer {i}"
#             email = f"customer{i}@example.com"
#             phone_number = f"+123456700{i}"
#             customer_id = insert_customer(db_connection, customer_name, email, phone_number)
#             customer_ids.append(customer_id)

#         # Insert two orders for each customer
#         for customer_id in customer_ids:
#             for i in range(1, 3):
#                 order_date = date.today()
#                 order_id = insert_order(db_connection, order_date, customer_id)


#                 cursor = db_connection.cursor()
#                 cursor.execute("SELECT book_id FROM books;")
#                 book_ids = cursor.fetchall()
#                 cursor.close()

#                 for book_id in book_ids:
#                     quantity = i+2
#                     insert_order_item(db_connection, order_id, book_id[0], quantity)
#         # Close the connection when done
#         db_connection.close()


#task 3 - part ii
#Retriving info

def retrieve_books_with_authors_and_orders(connection):
    try:
        query = """ 
            SELECT
                b.book_id,
                b.title,
                b.stock_quantity,
                b.price,
                a.author_name,
                o.order_id,
                o.order_date,
                c.customer_name
            FROM
                books b
            JOIN authors a ON b.author_id = a.author_id
            LEFT JOIN orderitems oi ON b.book_id = oi.book_id
            LEFT JOIN orders o ON oi.order_id = o.order_id
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            ORDER BY
                o.order_id, b.book_id;
                """
        cursor =connection.cursor()
        cursor.execute(query)
        book_info = cursor.fetchall()
        
        current_order_id = None
        for row in book_info:
            if row[5] != current_order_id:
                if current_order_id is not None:
                    print()  # Add a newline between orders
                print(f"Order ID: {row[5]} | Order Date: {row[6]} | Customer Name: {row[7]}")
                print("-" * 50)
                current_order_id = row[5]

            print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")
    except psycopg2.Error as e:
        print(f"Error during retrieval: {e}")
    finally:
        cursor.close()


if __name__ == "__main__":
    db_connection = connect_to_database()

    if db_connection is not None:
        retrieve_books_with_authors_and_orders(db_connection)
        db_connection.close()
# #printing headers for each table
#         print("Books Information")
#         print("Book ID | Title | Stock Quantity | Price | Author")
#         print("-------------------------------------------------")
#         for row in book_info:
#             print(f"{row[0]:<8} | {row[1]:<30} | {row[2]:<15} | {row[3]:<10} | {row[4]:<20}")

#         print("\nOrders Information")
#         print("Order ID | Order Date | Customer Name")
#         print("-------------------------------------")
#         for row in book_info:
#             # Print the order-related columns                                  
#             print(f"{row[0]:<8} | {row[1]:<12} | {row[2]:<15} | {row[3]:<14} | {row[4]:<20}")


#     except  psycopg2.Error as e:
#           print(f"Error during retrieval: {e}")
#     finally:
#         cursor.close()
        
    
# if __name__ == "__main__":
#     db_connection = connect_to_database()

#     if db_connection is not None:
#         retrieve_books_with_authors_and_orders(db_connection)

#         db_connection.close()




#Task 3 part - iii
# def update_books(connection, book_id, new_stock_quantity, new_price):
#     try:
#         query = "UPDATE books SET stock_quantity = %s, price = %s WHERE book_id = %s;"
#         values = (new_stock_quantity, new_price, book_id)
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         connection.commit()
#         print(f"Book with ID {book_id} updated successfully.")
#     except psycopg2.Error as e:
#         print(f"Error during book update: {e}")
#     finally:
#         cursor.close()

# if __name__ == "__main__":
#     db_connection = connect_to_database()

#     if db_connection is not None:
#         # Updating the stock quantity and price of a book with ID 1 and 3
#         update_books(db_connection, book_id=1, new_stock_quantity=33, new_price=19.99)
#         update_books(db_connection, book_id=3, new_stock_quantity=17.5, new_price=21.99)

#         # Retrieving updated book information
#         retrieve_books_with_authors_and_orders(db_connection)

#         db_connection.close()

#Task 3 - iv
def remove_book(connection, book_id):
    try:
        query_orderitems = "DELETE FROM orderitems WHERE book_id = %s;"
        values_orderitems = (book_id,)
        cursor_orderitems = connection.cursor()
        cursor_orderitems.execute(query_orderitems, values_orderitems)
        cursor_orderitems.close()

        query_books = "DELETE FROM books WHERE book_id = %s;"
        values_books = (book_id,)
        cursor_books = connection.cursor()
        cursor_books.execute(query_books, values_books)
        connection.commit()

        print(f"Book with ID {book_id} and related information removed successfully.")



    except psycopg2.Error as e:
        print(f"Error during book removal: {e}")
    finally:
        cursor_books.close()

if __name__ == "__main__":
    db_connection = connect_to_database()

    if db_connection is not None:
        # removing book with id 2
        remove_book(db_connection, book_id=2)

        # Retrieving updated book information
        retrieve_books_with_authors_and_orders(db_connection)

        db_connection.close()

def remove_book(conection, book_id):
    try:
        #removing items from orderitems related to book
        query_orderitems = "DELEWTE FROM orderitems WHERE book_id = %s;"
        values_orderitems = (book_id,)
        cursor_orderitems = conection.cursor()
        cursor_books.execute(query_books, values_books)
        connection.commit()





