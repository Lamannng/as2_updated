# Bookstore Management Application
## Table of Contents

- [Introduction](#introduction)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Environment Variables](#environment-variables)
- [Usage](#usage)
  - [Connecting to the Database](#connecting-to-the-database)
  - [Inserting Data](#inserting-data)
  ...


## Introduction

This Python application manages a bookstore database, allowing users to perform CRUD operations, handle transactions, and access metadata. The application is designed to interact with a PostgreSQL database.

## Setup

### Prerequisites

Ensure you have the following installed:

- PostgreSQL database
  
### Environment Variables

Set up the following environment variables:

- `DB_NAME`: Your PostgreSQL database name
- `DB_USER`: Your PostgreSQL username
- `DB_PASSWORD`: Your PostgreSQL password
- `DB_HOST`: Your PostgreSQL host
- `DB_PORT`: Your PostgreSQL port

## Usage

1. **Connect to the Database:**
   ```python
   from bookstore_manager import connect_to_database

   # Establish a connection to the database
   db_connection = connect_to_database()


# Bookstore Database

## Description

This Python application interacts with a PostgreSQL database to manage a bookstore's data. The database schema includes the following tables:

- **`authors`**: Stores information about authors, including their name, birthdate, and nationality.

- **`books`**: Contains details about books, such as title, stock quantity, price, and a reference to the author through the `author_id` foreign key.

- **`customers`**: Records customer information, including name, email, and phone number.

- **`orders`**: Manages orders placed by customers, tracking order date and linking to the respective customer through the `customer_id` foreign key.

- **`orderitems`**: Represents the many-to-many relationship between orders and books. It stores information such as order item ID, order ID, book ID, and quantity.

## Connecting to the Database

To connect to the PostgreSQL database, the application uses the `psycopg2` library. Database connection parameters, including name, user, password, host, and port, are loaded from environment variables.

```python
# Example of connecting to the database
db_connection = connect_to_database()

# Example of inserting authors and books
author_ids = insert_authors(db_connection, authors_data)
insert_books(db_connection, books_data)

# Example of inserting customers, orders, and order items
customer_id = insert_customer(db_connection, "John Doe", "john@example.com", "+1234567890")
order_id = insert_order(db_connection, date.today(), customer_id)
insert_order_item(db_connection, order_id, book_id=1, quantity=2)

# Example of retrieving book information with authors and orders
retrieve_books_with_authors_and_orders(db_connection)

# Example of updating books
update_books(db_connection, book_id=1, new_stock_quantity=33, new_price=19.99)

# Example of removing a book and related information
remove_book(db_connection, book_id=2)

# Example of inserting an order with transaction management
insert_order(db_connection, order_date=date.today(), customer_id=1, book_id=1, quantity=2)

# Example of accessing metadata
display_table_names(db_connection)
display_table_structure(db_connection, 'books')
display_primary_keys(db_connection, 'books')
display_foreign_keys(db_connection, 'books')


Database Schema
Authors Table
author_id (Serial Primary Key)
author_name (VARCHAR, Not Null)
birthdate (DATE)
nationality (VARCHAR)

Books Table
book_id (Serial Primary Key)
title (VARCHAR, Not Null)
stock_quantity (INTEGER, Not Null)
price (DECIMAL(10, 2), Not Null)
author_id (INTEGER, Foreign Key to Authors)

Customers Table
customer_id (Serial Primary Key)
customer_name (VARCHAR, Not Null)
email (VARCHAR, Not Null)
phone_number (VARCHAR)

Orders Table
order_id (Serial Primary Key)
order_date (DATE, Not Null)
customer_id (INTEGER, Foreign Key to Customers)

OrderItems Table
order_item_id (Serial Primary Key)
order_id (INTEGER, Foreign Key to Orders)
book_id (INTEGER, Foreign Key to Books)
quantity (INTEGER, Not Null)

