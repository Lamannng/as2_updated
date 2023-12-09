import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv
from datetime import date

load_dotenv

def connect_to_database():
    connection = None

    try:
        # Database connection parameters
        database = "postgres"
        user = "postgres"
        password = "l2911q55"
        host = "localhost"
        port = "5432"

        # Establish the database connection
        connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

        # Connection successful, perform database operations here
        print("Connected to the database!")

    except OperationalError as e:
        # Handle OperationalError (Database connection failure)
        print(f"Error: {e}")
    finally:
        # Close the connection in the finally block to ensure it's always closed
        if connection is not None:
            connection.close()

if __name__ == "__main__":
    connect_to_database()



# 3rd part of assignment, implementing CRUD operations
def insert_authors(connection, authors_data):
        try:
            
            query = "INSERT INTO authors (author_name, birthdate, nationality) VALUES (%s, %s, %s) RETURNING author_id;"
            cursor = connection.cursor()

            author_ids = []
            for author in authors_data:
                 cursor.execute(query, author)
                 author_id = cursor.fetchone()[0]
                 author_ids.append(author_id)

            connection.commit()
            print(f"{len(authors_data)} new authors created with IDs: {author_ids}")
            return author_ids

        except psycopg2.Error as e:
            print(f"Error during insertion: {e}")
        finally:
            cursor.close()            


def insert_books(connection, books_data):
        try:

            query = "INSERT INTO books ( title, stock_quantity , price, author_id) VALUES (%s, %s, %s, %s);"
            cursor = connection.cursor()
            
            for book in books_data:
                 cursor.execute(query, book)

            connection.commit()
            print(f"{len(books_data)} new books records created successfully!")
            
        except psycopg2.Error as e:
             print(f"Error during insertion: {e}")
        finally:
            cursor.close()

#my examples about authors and books
authors_data = [
     ("J.K. Rowling", date(1965, 7, 31), "British"),
     ("George Orwell",date(1903, 6, 25) , "British"),
     ("Delia Owens", date(1949, 4, 4), "American"),
     ("To Kill a Mockinbird", date(1926,4, 25), "American")
]


books_data = [
     ("Harry Potter and Sorcerer's Stone", 100, 17.55, 1),
     ("1984", 50, 19.84, 2),
     ("Where the Crawdads Sing", 133, 19.89, 3),
     ("To Kill a Mockinbird", 75, 14.99, 4)
]

#usage
db_connection = connect_to_database()


#checking if connection is successfull
if db_connection is not None:
          author_ids = insert_authors(db_connection, authors_data)
          insert_books(db_connection, [(title, quantity, price, author_ids[i % len(author_ids)]) for i, (title, quantity, price, _) in enumerate(books_data)] )
          
      ##    closing conn
          db_connection.close() 


