"""
``connect.py``
--------------

Create a connection to a PostgreSQL database, using the ``psycopg2`` or ``sqlalchemy`` module as needed.

Make sure to ``close()`` or ``dispose()`` the connection when you're done.
If you don't do this you'll eventually get a database error due to too many open connections.


Examples
--------
    >>> # Example using psycopg2
    >>> connection = make_database_connection('my_database', 'psycopg2')
    >>> cursor = connection.cursor()
    >>> cursor.execute('SELECT * FROM my_table LIMIT 500')
    >>> result = cursor.fetchall()
    >>> # Make sure to close out the connection!
    >>> cursor.close()
    >>> connection.close()

    >>> # Example using sqlalchemy
    >>> import pandas as pd
    >>> engine = make_database_connection('my_database', 'sqlalchemy')
    >>> df = pd.read_sql('SELECT * FROM my_table LIMIT 500', engine)
    >>> # Make sure you dispose the connection!
    >>> engine.dispose()
"""

import psycopg2
import sqlalchemy

from postGIS_tools.configurations import PG_PASSWORD


def make_database_connection(
        db_name: str,
        method: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = True
):
    """
    Create a connection object to a PostgreSQL database.

    :param db_name: name of the database (string). eg: 'aa_santa_clara'
    :param method: name of library (string). Either 'psycopg2' or 'sqlalchemy'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: `psycopg2.connect()` or `sqlalcehmy.create_engine()` object to be used for database I/O operations
    """

    uri = f'postgresql://{username}:{password}@{host}:{port}/{db_name}'

    if debug:
        print(f"Using {method} to connect to:\n\t{uri}")

    if method == 'sqlalchemy':
        return sqlalchemy.create_engine(uri)

    elif method == 'psycopg2':
        return psycopg2.connect(uri)
