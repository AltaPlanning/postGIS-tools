"""
``get.py``
----------

Get simple things out of the database, including:
    - list of all tables in a database
    - list of all columns in a table
    - list of all spatial tables in a database

"""
from postGIS_tools.db.connect import make_database_connection

from postGIS_tools.configurations import PG_PASSWORD


def fetch_things_from_database(
        query,
        database,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    Use ``psycopg2`` to send query to database and return the ``.fetchall()`` result.

    TODO: type hints and params

    :param query: 'SELECT * FROM my_table'
    :param database: 'name_of_the_database'
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :return: cursor.fetchall() object (list)
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    if debug:
        print('QUERYING VIA psycopg2:')
        print('-' * 40)
        print(query)

    connection = make_database_connection(database, 'psycopg2', **config)
    cursor = connection.cursor()

    cursor.execute(query)

    result = cursor.fetchall()

    cursor.close()
    connection.close()

    return result


def get_list_of_tables_in_db(
        database,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    Return a list of all tables that exist in a given database.

    TODO: type hints and params

    :param database: 'name_of_the_database'
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :return: list of all tables in database
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Get a list of all tables that are currently in the database
    q = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""

    tables = fetch_things_from_database(q, database, **config)

    tables_to_ignore = ['geography_columns', 'geometry_columns',
                        'spatial_ref_sys', 'raster_columns', 'raster_overviews']

    table_names = [t[0] for t in tables if t[0] not in tables_to_ignore]

    return table_names


def get_full_list_of_tables_in_db(
        database,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    Return a FULL list of all tables that exist in a given database.
    Unlike ``get_list_of_tables_in_db()``, this one does not ignore the following tables:
    - 'geography_columns'
    - 'geometry_columns'
    - 'spatial_ref_sys'
    - 'raster_columns'
    - 'raster_overviews'

    TODO: type hints and params

    :param database: 'name_of_the_database'
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :return: list of all tables in database
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Get a list of all tables that are currently in the database
    q = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""

    tables = fetch_things_from_database(q, database, **config)

    table_names = [t[0] for t in tables]

    return table_names


def get_list_of_columns_in_table(
        database,
        table,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    Return a list of all columns that exist in a given table

    TODO: type hints and params

    :param database: 'name_of_the_database'
    :param table: 'name_of_the_table'
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :return: list of columns
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    q = """ SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public'
            AND table_name = '{}' """.format(table)

    raw_result = fetch_things_from_database(q, database, **config)

    result = [t[0] for t in raw_result]

    return result


def get_list_of_spatial_tables_in_db(
        database,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    Return a list of all spatial tables that exist in a given database.

    :param database: 'name_of_the_database'
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :return: list of all spatial tables in database
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Get a list of all tables that are currently in the database
    q = """SELECT f_table_name AS tblname FROM geometry_columns"""

    spatial_tables = fetch_things_from_database(q, database, **config)

    spatial_table_names = [t[0] for t in spatial_tables]

    return spatial_table_names
