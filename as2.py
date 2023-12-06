import psycopg2
from psycopg2 import OperationalError

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
