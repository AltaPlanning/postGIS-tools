import os
import sys
import time
from datetime import datetime
from typing import Union

import pandas as pd
import geopandas as gpd

import psycopg2
import sqlalchemy
from geoalchemy2 import Geometry, WKTElement


from postGIS_tools.configurations import THIS_SYSTEM
from postGIS_tools.queries.hexagon_grid import hex_grid_function
from postGIS_tools.logs import log_activity
from postGIS_tools.constants import PG_PASSWORD

################################################################################
# CONNECT TO THE DATABASE
################################################################################


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

################################################################################
# GET BASIC THINGS OUT OF THE DATABASE
################################################################################


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


def get_database_list(
        default_db: str = "postgres",
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = True

):
    """
    TODO docstring

    :param host:
    :param username:
    :param password:
    :param port:
    :param debug:
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Get a list of databases that aren't 'postgres'
    q = """ SELECT datname FROM pg_database 
            WHERE datistemplate = false
                AND datname != 'postgres'; """

    # Run this query from within the 'postgres' db
    query_result = fetch_things_from_database(q, default_db, **config)
    db_list = [x[0] for x in query_result]

    return db_list

################################################################################
# TEST THAT THINGS EXIST
################################################################################


def database_exists(
        database,
        default_db: str = "postgres",
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    TODO docstring

    :param database:
    :param host:
    :param username:
    :param password:
    :param port:
    :param debug:
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, "default_db": default_db, 'debug': debug}
    db_list = get_database_list(**config)

    if database in db_list:
        return True
    else:
        return False


def spatial_table_exists(
        table,
        database,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    TODO docstring

    :param table:
    :param database:
    :param host:
    :param username:
    :param password:
    :param port:
    :param debug:
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}
    spatial_table_list = get_list_of_spatial_tables_in_db(database, **config)

    if table in spatial_table_list:
        return True
    else:
        return False


################################################################################
# QUERY THE DATABASE AND RETURN AS DATAFRAME
################################################################################


def query_table(
        db_name: str,
        query: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Query a table in a database and get the result as a ``pandas.DataFrame``

    :param db_name: 'name_of_the_database'
    :param query: 'SELECT * FROM my_table'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: ``pandas.DataFrame``
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    if debug:
        print('QUERYING...')
        print('-' * 40)
        print(query)

    engine = make_database_connection(db_name, 'sqlalchemy', **config)

    df = pd.read_sql(query, engine)

    engine.dispose()

    return df


def query_geo_table(
        db_name: str,
        query: str,
        geom_col: str = 'geom',
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Query a geo table in a SQL database and get the result as a ``geopandas.GeoDataFrame``

    Be aware of the name of the geometry column. In PostGIS it's typically called 'geom',
    but geopandas seems to expect 'geometry' instead.

    TODO: type hints and params

    :param db_name: 'name_of_the_database'
    :param query: 'SELECT gid, pop2015, geom FROM my_table WHERE pop2015 > 1000'
    :param geom_col: the name of the geometry column. Should either be 'geom' or 'geometry'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: ``geopandas.GeoDataFrame``
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    if debug:
        print('QUERYING...')
        print('-' * 40)
        print(query)

    connection = make_database_connection(db_name, 'psycopg2', **config)

    gdf = gpd.GeoDataFrame.from_postgis(query, connection, geom_col=geom_col)

    connection.close()

    return gdf


################################################################################
# UPDATE THINGS IN THE DATABASE
################################################################################


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

    log_activity(database, "pGIS.execute_query", query, **config)

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

    log_activity(database, "pGIS.add_or_nullify_column", query, **config)


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

    log_activity(database, "pGIS.drop_table", drop_table_query, **config)


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

    log_activity(database, "pGIS.project_spatial_table", qry, **config)


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

    log_activity(database, "pGIS.prep_spatial_table",
                 "Add unique_id PK and make spatial index on geom column", **config)


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
    log_activity(database, "pGIS.register_geometry_column", query, **config)


def make_geotable_from_query(
        database,
        new_tblname,
        query,
        geom_colname='geom',
        geom_type: str = 'POINT',
        epsg: int = 4326,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Quickly make a new spatial table in PostgreSQL with a query,
    using ``geopandas`` and other downstream functions to take care of table setup in the database.

    This avoids doing the setup manually, which includes adding a primary key, spatial index, etc.

    TODO: type hints and params

    :param database: 'my_database'
    :param new_tblname: 'name_of_my_new_table'
    :param query: "SELECT * FROM my_table WHERE highway = 'Local' "
    :param geom_colname: 'geom'
    :param host: 'localhost'
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Confirm that the geom type is valid
    valid_geom_types = ["POINT", "MULTIPOINT", "POLYGON", "MULTIPOLYGON", "LINESTRING", "MULTILINESTRING"]
    if geom_type not in valid_geom_types:
        print(f"Geometry type of {geom_type} is not valid.")
        print(f"Please use one of the following: {valid_geom_types}")
        print("Aborting")
        return

    if debug:
        print(f'MAKING {new_tblname} FROM:')
        print(query)

    full_query = f"""
        DROP TABLE IF EXISTS {new_tblname};
        CREATE TABLE {new_tblname} AS
        {query}
    """

    execute_query(database, full_query, **config)
    log_activity(database, "pGIS.make_geotable_from_query", full_query, **config)

    prep_spatial_table(database, new_tblname, **config)

    register_geometry_column(database, new_tblname, geom_type=geom_type, epsg=epsg, **config)

################################################################################
# MAKE A NEW DATABASE
################################################################################


def load_hexgrid_function(
        database: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Execute SQL code that defines the ``hex_grid()`` function in the database.

    :param database: 'name_of_the_database'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: None
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    if debug:
        print(f'Loading the hex_grid() function into {database} on {host}')
    execute_query(database, hex_grid_function, **config)
    log_activity(database, "pGIS.load_hexgrid_function", "see pGIS.queries.hexagon_grid.py", **config)


def make_new_database(
        database_name: str,
        default_db: str = "postgres",
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Create a new PostgreSQL database, load PostGIS, and define a custom hexagon function

    :param database_name: 'name_of_the_database'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: None
    """

    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # check to see if this database already exists
    exists_qry = f""" SELECT EXISTS(
                        SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{database_name}')
                     );  """

    exist_query_response = query_table(default_db, exists_qry, **config)

    exists_result = [str(row.exists) for idx, row in exist_query_response.iterrows()]

    if 'False' in exists_result:
        if debug:
            print(f'Make new database routine for: {database_name}')

        make_db = f"CREATE DATABASE {database_name};"

        c_postgres = make_database_connection(default_db, 'psycopg2', **config)
        c_postgres.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = c_postgres.cursor()

        cursor.execute(make_db)

        cursor.close()
        c_postgres.commit()
        c_postgres.close()

        log_activity(database_name, "pGIS.make_new_database", make_db, **config)

        # Check if PostGIS already exists by default. If not, add it.
        if 'geometry_columns' not in get_full_list_of_tables_in_db(database_name, **config):
            msg = f'Adding postGIS extension to {database_name}'
            execute_query(database_name, 'CREATE EXTENSION postGIS;', **config)
            log_activity(database_name, "pGIS.make_new_database", "Added PostGIS", **config)

        else:
            msg = f'PostGIS exists by default in {database_name}'
            log_activity(database_name, "pGIS.make_new_database", "PostGIS exists by default", **config)

        if debug:
            print(msg)

        # Load the custom SQL hexagon grid function
        load_hexgrid_function(database_name, **config)

    else:
        if debug:
            print(f'{database_name} already exists at {host}')


################################################################################
# LOAD A DATABASE DUMP FILE
################################################################################


def load_database_file(
        sql_file_path: str,
        database_name: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Load a ``.sql`` file to a new database.
    NOTE: ``psql`` must be accessible on the system path

    TODO: type hints and params. Add port to psql URI

    :param sql_file_path: '/path/to/sqlfile.sql'
    :param database_name: 'new_db_name'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    path_list = sql_file_path.split('\\')
    sql_file = path_list[-1]

    print(f'Loading {sql_file} to {database_name}')

    make_new_database(database_name, **config)

    c = f'psql "postgresql://{username}:{password}@{host}/{database_name}" <  "{sql_file_path}"'

    print(c)
    os.system(c)

    log_activity(database_name, "pGIS.load_database_file", c, **config)


################################################################################
# WRITE DATA FROM FILE TO THE DATABASE
################################################################################


def dataframe_to_postgis(
        db_name: str,
        dataframe: pd.DataFrame,
        table_name: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Write a ``pandas.DataFrame`` to a PostgreSQL database.

    :param db_name: 'name_of_the_database'
    :param dataframe: ``pandas.DataFrame``
    :param table_name: 'name_of_the_table'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: None
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    start_time = time.time()

    if debug:
        print(f'WRITING {table_name}//{db_name}//{host} FROM DATAFRAME')

    # FORCE ALL COLUMN NAMES TO LOWER-CASE (pgSQL requirement)
    dataframe.columns = [x.lower() for x in dataframe.columns]

    # CONNECT TO DATABASE, WRITE DATAFRAME, THEN DISCONNECT
    engine = make_database_connection(db_name, 'sqlalchemy', **config)
    dataframe.to_sql(table_name, engine, if_exists='replace')
    engine.dispose()

    if debug:
        # REPORT THE RUNTIME
        runtime = time.time() - start_time
        print(f'FINISHED IN {runtime} SECONDS')

    log_activity(db_name, "pGIS.dataframe_to_postgis", f"Wrote pandas.DataFrame to {table_name}", **config)


def csv_to_postgis(
        csv_filepath: str,
        table_name: str,
        db_name: str,
        overwrite: bool = True,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Write ``.CSV`` file to a database.
    Accomplished by importing to a ``pandas.DataFrame`` and calling ``dataframe_to_postgis()``.

    TODO: params

    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console

    """

    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    if debug:
        print(f'READING {csv_filepath}')

    # Does this table already exist? In order to overwrite it you have to drop the existing table first
    tables = get_list_of_tables_in_db(db_name, **config)
    if table_name in tables:
        if overwrite:
            print(f'{table_name} ALREADY EXISTS... DROPPING TABLE CASCADE')
            drop_table(db_name, table_name, **config)
        else:
            return None

    # Read .CSV file into Pandas
    try:
        df = pd.read_csv(csv_filepath)
    except:
        df = pd.read_csv(csv_filepath, encoding="ISO-8859-1")

    # Replace "Column Name" with "column_name"
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = [x.lower() for x in df.columns]

    # Remove '.' and '-' from column names.
    # i.e. 'geo.display-label' becomes 'geodisplaylabel'
    for s in ['.', '-']:
        df.columns = df.columns.str.replace(s, '')

    log_activity(db_name, "pGIS.csv_to_postgis", f"Loaded CSV from {csv_filepath}", **config)

    # Save dataframe to database
    dataframe_to_postgis(db_name, df, table_name, **config)


def geodataframe_to_postgis(
        database: str,
        geodataframe: gpd.GeoDataFrame,
        output_table_name: str,
        src_epsg: Union[bool, int] = None,
        output_epsg: Union[bool, int] = None,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Write a ``geopandas.GeoDataFrame`` to a PostGIS table in a SQL database.

    Assumes that the geometry column has already been named 'geometry'

    :param database: 'name_of_the_database'
    :param geodataframe: geopandas.GeoDataFrame
    :param output_table_name: 'name_of_the_output_table'
    :param src_epsg: if not None, will assign the geodataframe this EPSG in the format of {"init": "epsg:2227"}
    :param output_epsg: if not None, will reproject data from input EPSG to specified EPSG
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: None
    """
    start_time = time.time()
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Get the geometry type
    # It's possible there are both MULTIPOLYGONS and POLYGONS. This grabs the MULTI variant
    geom_types = list(geodataframe.geometry.geom_type.unique())
    geom_typ = max(geom_types, key=len).upper()

    if debug:
        print(f'\t PROCESSING \t {geom_typ} \t {output_table_name}')

    # Manually set the EPSG if the user passes one
    if src_epsg:
        geodataframe.crs = {"init": f"epsg:{src_epsg}"}
        epsg_code = src_epsg

    # Otherwise, try to get the EPSG value directly from the geodataframe
    else:
        try:
            # gdf should have a CRS stored like this: {'init': 'epsg:4326'}
            epsg_code = int(geodataframe.crs['init'].split(':')[1])
        except:
            print('This geodataframe does not have a valid EPSG. Aborting.')
            print(geodataframe.crs)
            return


    # Sanitize the columns before writing to the database
    # Make all column names lower case
    geodataframe.columns = [x.lower() for x in geodataframe.columns]

    # Replace the 'geom' column with 'geometry'
    if 'geom' in geodataframe.columns:
        geodataframe['geometry'] = geodataframe['geom']
        geodataframe.drop('geom', 1, inplace=True)

    # Drop the 'gid' column
    if 'gid' in geodataframe.columns:
        geodataframe.drop('gid', 1, inplace=True)

    # Rename 'unique_id' to 'old_uid'
    if 'unique_id' in geodataframe.columns:
        geodataframe['old_uid'] = geodataframe['unique_id']
        geodataframe.drop('unique_id', 1, inplace=True)

    # Build a 'geom' column using geoalchemy2 and drop the source 'geometry' column
    geodataframe['geom'] = geodataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=epsg_code))
    geodataframe.drop('geometry', 1, inplace=True)

    # write geodataframe to SQL database
    if debug:
        print(f'\t WRITING TO - {database} /// {host}')

    engine = make_database_connection(database, 'sqlalchemy', **config)
    geodataframe.to_sql(output_table_name, engine,
                        if_exists='replace', index=True, index_label='gid',
                        dtype={'geom': Geometry(geom_typ, srid=epsg_code)})
    engine.dispose()

    if debug:
        runtime = round((time.time() - start_time), 2)
        print(f'\t FINISHED IN {runtime} seconds')

    log_activity(database, "pGIS.geodataframe_to_postgis",
                 f"Wrote geopandas.GeoDataFrame to {output_table_name}", **config)

    # If provided an EPSG, alter whatever the native projection was to the output_epsg
    if output_epsg:
        project_spatial_table(database, output_table_name, geom_typ, epsg_code, output_epsg, **config)

    # Add a unique_id column and do a spatial index
    prep_spatial_table(database, output_table_name, **config)


def shp_to_postgis(
        shp_path: str,
        output_table_name: str,
        database: str,
        src_epsg: Union[bool, int] = None,
        output_epsg: Union[bool, int] = None,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Read a ``shapefile`` into ``geopandas.GeoDataFrame`` and then use ``geodataframe_to_postgis()``

    TODO: type hints and params

    :param shp_path:  r'c:\path\to\your\shapefile.shp'
    :param output_table_name: 'name_of_the_output_table'
    :param database: 'name_of_the_sql_database'
    :param src_epsg: if not None, will assign the geodataframe this EPSG in the format of {"init": "epsg:2227"}
    :param output_epsg: if not None, will reproject data from input EPSG to specified EPSG
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    if debug:
        print(f'READING - {shp_path}')

    # READ THE .SHP INTO A GEOPANDAS.GEODATAFRAME
    df = gpd.read_file(shp_path)

    # REMOVE ROWS WITH NULL GEOMETRY
    df = df[df['geometry'].notnull()]

    # EXPLODE TO TRANSFORM ANY MULTIPART FEATURES TO SINGPLEPART
    df = df.explode()

    # RESET THE INDEX AFTER EXPLODING
    df['explode'] = df.index
    df = df.reset_index()

    log_activity(database, "pGIS.shp_to_postgis", f"Loaded .SHP from {shp_path}", **config)

    # SEND THE GEODATAFRAME TO POSTGIS
    geodataframe_to_postgis(database, df, output_table_name, src_epsg=src_epsg, output_epsg=output_epsg, **config)


def shp2pgsql(
        shp_path: str,
        new_pg_name: str,
        database: str,
        srid: int,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Use the command-line ``shp2pgsql`` operation to ingest a shapefile,
    and pipe it into PostGIS using ``psql``

    TODO: add handling for machines where PSQL is not on path
    TODO: add handling for machines that are picky about the db password
    TODO: type hints and params

    :param shp_path:
    :param new_pg_name:
    :param database:
    :param srid:
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    command = f"""shp2pgsql -s {srid} -I {shp_path} {new_pg_name} | psql -U {username} -d {database} -h {host}"""
    print(command)


################################################################################
# SAVE FILES OUT FROM THE DATABASE
################################################################################


def postgis_to_shp(
        database: str,
        table_name: str,
        output_folder: str,
        geom_col: str = 'geom',
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Write a spatial PostGIS table to a shapfile using ``query_geo_table().to_file()``

    :param database: 'name_of_the_database'
    :param table_name: 'name_of_the_table'
    :param output_folder: r'c:\\path\\to\\your\\output\\shapefile\\folder'
    :param geom_col: 'geom' is default spatial column name in postGIS
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return: None
    """

    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    try:
        print(f'Creating shapefile from {table_name}')
        df = query_geo_table(database, f'SELECT * FROM {table_name}', geom_col=geom_col, **config)

        # Convert any boolean column types to strings
        for c in df.columns:
            datatype = df[c].dtype.name
            if datatype == 'bool':
                df[c] = df[c].astype(str)

        out_shp = os.path.join(output_folder, f'{table_name}.shp')
        df.to_file(out_shp)

        log_activity(database, "pGIS.postgis_to_shp", f"Saved .SHP from {table_name} to {out_shp}", **config)

    except:
        print(sys.exc_info()[0])


def dump_all_spatial_tables_to_shapefiles(
        database_name: str,
        output_folder: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Write all spatial tables in a PostGIS database to a subfolder of the output_folder.

    :param database_name: 'name_of_the_database'
    :param output_folder: folder path, e.g. r'C:\Egnyte\Shared\SERVICE_AREA\Analytics\data\Projects\El Dorado'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # put everything into a subfolder. Make it if it doesn't exist
    dump_folder = os.path.join(output_folder, f'shps_from_pgdb_{database_name}')
    if not os.path.exists(dump_folder):
        os.mkdir(dump_folder)

    log_activity(database_name, "pGIS.dump_all_spatial_tables_to_shapefiles",
                 f"Saving all spatial tables to SHP in {dump_folder}", **config)

    # Get a list of all spatial tables and export each one
    spatial_tables = get_list_of_spatial_tables_in_db(database_name, **config)
    for table in spatial_tables:
        postgis_to_shp(database_name, table, dump_folder, **config)


def dump_database_to_sql_file(
        db_to_backup: str,
        backup_folder: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = True
):
    """ Save a database stored on a host to a .sql file, using TERMINAL on OSX

    TODO: update with declaration of username and port
    TODO: REPLACE with URI

    :param db_to_backup: 'name_of_db'
    :param backup_folder: '/Users/my_name/Desktop/backup_folder'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Get a string for today's date, like '2019_02_26'
    today = str(datetime.now()).split(' ')[0].replace('-', '_')

    # USE PG_DUMP VIA COMMAND LINE TO SAVE A .SQL FILE
    sql_file = f'{db_to_backup}_{today}.sql'
    if debug:
        print(f'Using pg_dump to back up {db_to_backup} to {sql_file}')


    log_activity(db_to_backup, 'pGIS.dump_database_to_sql_file',
                 f"Using pg_dump to create {sql_file}", **config)

    if THIS_SYSTEM == 'Darwin':
        c = f""" export PGPASSWORD={password}
                 cd "{backup_folder}"
                 pg_dump -U {username} -h {host} -p {port} {db_to_backup} > {sql_file} """

    elif THIS_SYSTEM == 'Windows':
        bkp_drive = backup_folder[:2]

        c = f""" set PGPASSWORD={password}
                 {bkp_drive}
                 cd "{backup_folder}"
                 pg_dump -U {username} -h {host} -p {port} {db_to_backup} > {sql_file} """
        c = c.replace('\n                 ', ' & ')

    # RUN THE COMMAND FROM COMMAND LINE
    print(c)
    os.system(c)
