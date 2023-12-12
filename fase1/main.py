import pandas as pd
from query import insercion
from itertools import batched

from database import get_connection, execute_queries


def ingestar():
    """
    Permite ingestar los valores de las diferentes fuentes de datos y obtener un Dataframe.
    """
    df = pd.read_csv("covid.csv")
    return df


def formar_bloque(registros):
    """
    Permite obtener un bloque de inserción SQL.
    """
    valores_query = ""
    # https://docs.python.org/3/library/collections.html#collections.namedtuple
    for i, row in enumerate(registros):
        valores_query += "({},{},{},{},{},\'{}\',{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}),\n" \
            .format(*row._asdict().values())
    return valores_query[:-2] + ";\n"


def formar_bloques(registros, batch_size=2000):
    """"
    Forma los bloques segun el batch_size y devuelve las sequencias SQL que hay que ejecutar.
    """
    bloques = dividir_bloques(registros, batch_size)
    queries = []
    for bloque in bloques:
        query = insercion + formar_bloque(bloque)
        queries.append(query)

    return queries


def dividir_bloques(lista, n):
    """
    Divide una lista en listas de tamaño n
    Codigo obtenido de:
    https://docs.python.org/3/library/itertools.html#itertools.batched
    """
    return list(batched(lista, n))  # batched esta disponible en python 3.12


def insert_to_database(queries):
    """
    Ejecuta todas las queries utilizando el modulo de database
    """
    conn = get_connection()
    execute_queries(conn, queries)


def main():
    df = ingestar()
    queries = formar_bloques(list(df.itertuples()))
    insert_to_database(queries)

main()
