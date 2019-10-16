"""
``update.py``
-------------

Methods for altering data that already exists inside a PostgreSQL database.

Examples
--------

    >>> # Run and commit an arbitrary SQL statement
    >>> my_query = 'UPDATE my_table SET my_col = 1 WHERE other_col IS NOT NULL'
    >>> execute_query('my_database', my_query)

    >>> # Add a new column to a table that already exists
    >>> # If the column already exists, it will be set to NULL
    >>> add_or_nullify_column('my_database', 'my_table', 'new_column', 'INTEGER')
"""


import time

from postGIS_tools.db.get import get_list_of_columns_in_table
from postGIS_tools.db.connect import make_database_connection

from postGIS_tools.assumptions import PG_PASSWORD


def execute_query(
        database: str,
        query: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Use ``psycopg2`` to execute and commit a SQL command in the database.

    :param database: 'name_of_the_database'
    :param query: 'DROP VIEW IF EXISTS my_view;'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: None
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    if debug:
        start_time = time.time()
        print('UPDATING via psycopg2:')
        print('\t', query)

    connection = make_database_connection(database, 'psycopg2', **config)
    cursor = connection.cursor()

    cursor.execute(query)

    cursor.close()
    connection.commit()
    connection.close()

    if debug:
        runtime = round(time.time() - start_time, 2)
        print(f'\t COMMITTED IN - {runtime} seconds')


def add_or_nullify_column(
        database: str,
        tbl: str,
        column: str,
        data_type: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Add a column to a table if it doesn't exist yet
    If it does exist, set the entire column to NULL

    :param database: 'name_of_database'
    :param tbl: 'name_of_table'
    :param column: 'col_name'
    :param data_type: any valid PgSQL type: 'TEXT', 'FLOAT', etc.
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    existing_cols = get_list_of_columns_in_table(database, tbl, **config)
    col_exists = column in existing_cols

    if not col_exists:
        query = f'''ALTER TABLE {tbl} ADD COLUMN {column} {data_type};'''
    else:
        query = f""" UPDATE {tbl} SET {column} = NULL  """

    execute_query(database, query, **config)


def drop_table(
        database: str,
        tablename: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Drop a table from a PostgreSQL database

    :param database: name of the database (string)
    :param tablename: name of the table to drop (string)
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    drop_table_query = f'DROP TABLE {tablename} CASCADE;'
    execute_query(database, drop_table_query, **config)


def project_spatial_table(
        database: str,
        tablename: str,
        geom_type: str,
        orig_epsg: int,
        new_epsg: int,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Alter a table's ``geom`` column to a new EPSG

    TODO: type hints and params

    :param database: name of the database (string)
    :param tablename: name of the table (string)
    :param geom_type: name of a SQL-valid geometry type (string)
    :param orig_epsg: the EPSG that the data currently has (integer)
    :param new_epsg: the EPSG you want the data to be projected into (integer)
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    qry = f'''ALTER TABLE {tablename}
              ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg}) 
              USING ST_Transform( ST_SetSRID( geom, {orig_epsg} ), {new_epsg} ); '''
    execute_query(database, qry, **config)


def prep_spatial_table(
        database: str,
        spatial_table_name: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Spatial tables in QGIS coming from PostGIS need a unique ID column and a spatial index.
    This function executes the two SQL queries needed.
    Results in a new column called ``unique_id`` and a spatial index on the existing ``geom`` column

    :param database: 'name_of_the_db'
    :param spatial_table_name: 'name_of_the_table'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: nothing
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Add a primary key column named 'unique_id'
    unique_id_query = f'ALTER TABLE {spatial_table_name} ADD unique_id serial PRIMARY KEY;'
    execute_query(database, unique_id_query, **config)

    # Create a spatial index on the 'geom' column
    spatial_index_query = f'CREATE INDEX gix_{spatial_table_name} ON {spatial_table_name} USING GIST (geom);'
    execute_query(database, spatial_index_query, **config)


def register_geometry_column(
        database: str,
        spatial_table: str,
        geom_type: str = 'Point',
        epsg: int = 4326,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Run this query if your spatial table has an error in QGIS saying:
    ``There isn't an entry in geometry_columns``.
    Seems to be related to when you make new spatial tables via query
    Spatial tables imported via geopandas do not seem to have this problem

    TODO: type hints and params

    :param database: 'name_of_db'
    :param spatial_table: 'name_of_table'
    :param geom_type: a valid PostGIS geom type, as string: 'Point'
    :param epsg: the EPSG that the data is already in. This does not transform anything
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: nothing
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    query = f''' ALTER TABLE {spatial_table}
                 ALTER COLUMN geom TYPE geometry({geom_type}, {epsg}) 
                                        USING ST_SetSRID(geom, {epsg})'''
    execute_query(database, query, **config)
