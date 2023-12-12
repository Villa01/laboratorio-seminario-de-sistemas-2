import pymysql
import os

mysql_config = {
    'HOST': os.environ.get('HOST'),
    'USER': os.environ.get('USER'),
    'PASS': os.environ.get('PASS'),
    'DATABASE': os.environ.get('DATABASE')
}


def get_connection():
    """
    Establece una conexión con una base de datos MYSQL.
    """
    my_conn = None
    try:
        my_conn = pymysql.connect(
            host=mysql_config['HOST'],
            user=mysql_config['USER'],
            password=mysql_config['PASS'],
            database=mysql_config['DATABASE']
        )
        my_conn.mysql_config = mysql_config

    except Exception as err:
        print(f'Could not connect to database: {err}')

    return my_conn


def execute_queries(db_conn, queries):
    """
    Ejecutar una lista de queries.
    """
    for q in queries:
        execute_query(db_conn, q)


def execute_query(db_conn, query):
    """
    Inserta a la base de datos los valores, revierte si existe un error y confirma si todo sale correcto.
    """
    try:
        cursor = db_conn.cursor()
        cursor.execute(query)
        db_conn.commit()
    except Exception as e:
        print(f"Failed to execute query: {e}")
        db_conn.rollback()
    else:
        cursor.close()

