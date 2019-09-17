"""
``create.py``
-------------

Functions for creating and managing PostgreSQL databases.


Examples
--------
    >>> # Make a new database locally
    >>> make_new_database('my_new_local_database')

    >>> # Make a new database on a server
    >>> make_new_database('my_new_remote_database', host='192.168.1.14')

    >>> # Load the custom hexagon grid function into a database that already exists
    >>> load_hexgrid_function('name_of_existing_database')

"""

import psycopg2

from postGIS_tools.db.connect import make_database_connection
from postGIS_tools.db.query import query_table
from postGIS_tools.db.update import execute_query
from postGIS_tools.queries.hexagon_grid import hex_grid_function
from postGIS_tools.db.get import get_full_list_of_tables_in_db

from postGIS_tools.assumptions import PG_PASSWORD


def load_hexgrid_function(
        database: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        debug: bool = False
):
    """
    Execute SQL code that defines the ``hex_grid()`` function in the database.

    :param database: 'name_of_the_database'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param debug: boolean to print messages to console
    :return: None
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    if debug:
        print(f'Loading the hex_grid() function into {database} on {host}')
    execute_query(database, hex_grid_function, **config)


def make_new_database(
        database_name: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        debug: bool = False
):
    """
    Create a new PostgreSQL database, load PostGIS, and define a custom hexagon function

    :param database_name: 'name_of_the_database'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param debug: boolean to print messages to console
    :return: None
    """

    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    # check to see if this database already exists
    exists_qry = f""" SELECT EXISTS(
                        SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{database_name}')
                     );  """

    exist_query_response = query_table('postgres', exists_qry, **config)

    exists_result = [str(row.exists) for idx, row in exist_query_response.iterrows()]

    if 'False' in exists_result:
        if debug:
            print(f'Make new database routine for: {database_name}')

        make_db = f"CREATE DATABASE {database_name};"

        c_postgres = make_database_connection('postgres', 'psycopg2', **config)
        c_postgres.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = c_postgres.cursor()

        cursor.execute(make_db)

        cursor.close()
        c_postgres.commit()
        c_postgres.close()

        # Check if PostGIS already exists by default. If not, add it.
        if 'geometry_columns' not in get_full_list_of_tables_in_db(database_name, **config):
            msg = f'Adding postGIS extension to {database_name}'
            execute_query(database_name, 'CREATE EXTENSION postGIS;', **config)
        else:
            msg = f'PostGIS exists by default in {database_name}'
        if debug:
            print(msg)

        # Load the custom SQL hexagon grid function
        load_hexgrid_function(database_name, **config)

    else:
        if debug:
            print(f'{database_name} already exists at {host}')

