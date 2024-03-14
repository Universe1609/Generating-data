import mysql.connector
import os
import datetime

from mysql.connector import Error
from faker import Faker
from dotenv import load_dotenv


fake = Faker()

#Crea un archivo .env en el directorio raiz, escribe las credenciales en este y cargalas con este comando
load_dotenv()

def create_connection(host_name, user, password, db_name):
    connection = None
    
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user,
            passwd = password,
            database = db_name
        )
        print("Conexion exitosa")
    except Error as e:
        print(f"Sucedio el siguiente error: {e}")
    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Base de datos creada")
    except Error as e:
        print(f"Ocurrio el siguiente error: {e}")
        
def execute_query(connection, query, data=None):
    cursor = connection.cursor()
    
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
            
        connection.commit()
        
        print(f"La query :{query} fue ejecutada con exito")
    except Error as e:
        print(f"Ocurrio el siguiente error: {e}")

def insert_users(connection, num_users):
    query = """
    INSERT INTO users (name, surname, age, init_date, email) VALUES (%s, %s, %s, %s, %s);
    """
    for _ in range(num_users):
        name = fake.first_name()
        surname = fake.last_name()
        age = fake.random_int(min=18, max=80, step=1)
        # Generate a random date between two dates
        start_date = datetime.date(2000, 1, 1)
        end_date = datetime.date.today()
        init_date = fake.date_between(start_date=start_date, end_date=end_date)
        email = fake.email()
        data = (name, surname, age, init_date, email)
        execute_query(connection, query, data)
        
    print("Se insertaron los datos con exito en la tabla users")
    
def insert_posts(connection, num_posts, num_users):
    query = "INSERT INTO posts (title, description, user_id) VALUES (%s, %s, %s);"
    for user_index in range(num_posts):
        title = fake.sentence(nb_words=6)
        description = fake.text(max_nb_chars=200)
        user_id = fake.random_int(min=1, max=num_users, step=1)
        data = (title, description, user_id)
        execute_query(connection, query, data)
    
    print("Se insertaron los datos con exito en la tabla posts")

connection = create_connection(os.getenv("DB_HOSTER"), os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_NAME"))

#Creamos una base de datos hello_mysql si no existe
create_database_query = "CREATE DATABASE IF NOT EXISTS hello_mysql"
create_database(connection, create_database_query)

#QUERY para crear tabla:
create_users_table = """
CREATE TABLE IF NOT EXISTS users(
    id INT AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    surname VARCHAR(100),
    age INT,
    init_date DATE,
    email VARCHAR(100) NOT NULL,
    PRIMARY KEY (id)
)
"""

create_posts_table = """
CREATE TABLE IF NOT EXISTS posts(
    id INT AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    user_id INT,
    PRIMARY KEY (id)
)
"""
connection.database=os.getenv("DB_NAME")

execute_query(connection, create_users_table)
execute_query(connection, create_posts_table)

insert_users(connection, 10)
insert_posts(connection, 20, 10)