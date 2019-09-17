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
        database,
        query,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Use ``psycopg2`` to execute and commit a SQL command in the database.

    :param database: 'name_of_the_database'
    :param query: 'DROP VIEW IF EXISTS my_view;'
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :param debug: bool, controls whether or not the query gets printed out to the console
    :return: None
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

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
        database,
        tbl,
        column,
        data_type,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Add a column to a table if it doesn't exist yet
    If it does exist, set the entire column to NULL

    :param database: 'name_of_database'
    :param tbl: 'name_of_table'
    :param column: 'col_name'
    :param data_type: any valid PgSQL type: 'TEXT', 'FLOAT', etc.
    :param debug: boolean True or False
    :return:
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    existing_cols = get_list_of_columns_in_table(database, tbl, **config)
    col_exists = column in existing_cols

    if not col_exists:
        query = f'''ALTER TABLE {tbl} ADD COLUMN {column} {data_type};'''
    else:
        query = f""" UPDATE {tbl} SET {column} = NULL  """

    execute_query(database, query, **config)


def drop_table(
        database,
        tablename,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Drop a table from a PostgreSQL database

    :param database:
    :param tablename:
    :param host:
    :param debug:
    :return:
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    drop_table_query = f'DROP TABLE {tablename} CASCADE;'
    execute_query(database, drop_table_query, **config)


def project_spatial_table(
        database,
        tablename,
        geom_type,
        orig_epsg,
        new_epsg,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Alter a table's ``geom`` column to a new EPSG

    :param database:
    :param tablename:
    :param geom_type:
    :param new_epsg:
    :param host:
    :param debug:
    :return:
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    qry = f'''ALTER TABLE {tablename}
              ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg}) 
              USING ST_Transform( ST_SetSRID( geom, {orig_epsg} ), {new_epsg} ); '''
    execute_query(database, qry, **config)


def prep_spatial_table(
        database,
        spatial_table_name,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Spatial tables in QGIS coming from PostGIS need a unique ID column and a spatial index.
    This function executes the two SQL queries needed.
    Results in a new column called ``unique_id`` and a spatial index on the existing ``geom`` column

    :param database: 'name_of_the_db'
    :param spatial_table_name: 'name_of_the_table'
    :param host: 'localhost' or '192.168.1.14'
    :param debug:
    :return: nothing
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    # Add a primary key column named 'unique_id'
    unique_id_query = f'ALTER TABLE {spatial_table_name} ADD unique_id serial PRIMARY KEY;'
    execute_query(database, unique_id_query, **config)

    # Create a spatial index on the 'geom' column
    spatial_index_query = f'CREATE INDEX gix_{spatial_table_name} ON {spatial_table_name} USING GIST (geom);'
    execute_query(database, spatial_index_query, **config)


def register_geometry_column(
        database,
        spatial_table,
        geom_type='Point',
        epsg=4326,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Run this query if your spatial table has an error in QGIS saying:
    ``There isn't an entry in geometry_columns``.
    Seems to be related to when you make new spatial tables via query
    Spatial tables imported via geopandas do not seem to have this problem

    :param database: 'name_of_db'
    :param spatial_table: 'name_of_table'
    :param geom_type: a valid PostGIS geom type, as string: 'Point'
    :param epsg: the EPSG that the data is already in. This does not transform anything
    :param host: 'localhost' or '192.168.1.14'
    :param debug:
    :return: nothing
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    query = f''' ALTER TABLE {spatial_table}
                 ALTER COLUMN geom TYPE geometry({geom_type}, {epsg}) 
                                        USING ST_SetSRID(geom, {epsg})'''
    execute_query(database, query, **config)
