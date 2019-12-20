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


from postGIS_tools.configurations import THIS_SYSTEM, deconstruct_uri
from postGIS_tools.queries.hexagon_grid import hex_grid_function
from postGIS_tools.logs import log_activity

################################################################################
# GET BASIC THINGS OUT OF THE DATABASE
################################################################################


def fetch_things_from_database(
        query: str,
        uri: str,
        debug: bool = False
):
    """
    Use ``psycopg2`` to send query to database and return the ``.fetchall()`` result.

    :param query: your query as ``str``, e.g. ``SELECT * FROM my_table``
    :param uri: connection string

    :return: ``cursor.fetchall()`` object
    """

    if debug:
        print('-' * 40)
        print(f'## Fetching ALL from {uri}')
        print(query)

    connection = psycopg2.connect(uri)
    cursor = connection.cursor()

    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    connection.close()

    return result


def get_list_of_tables_in_db(
        uri: str,
        debug: bool = True
) -> list:
    """
    Return a list of all tables that exist in a given database.

    :param uri: connection string

    :return: list of all tables in database
    """

    # Get a list of all tables that are currently in the database
    q = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""

    tables = fetch_things_from_database(q, uri=uri, debug=debug)

    tables_to_ignore = ['geography_columns', 'geometry_columns',
                        'spatial_ref_sys', 'raster_columns', 'raster_overviews']

    table_names = [t[0] for t in tables if t[0] not in tables_to_ignore]

    return table_names


def get_full_list_of_tables_in_db(
        uri: str,
        debug: bool = True
) -> list:
    """
    Return a FULL list of all tables that exist in a given database.
    Unlike ``get_list_of_tables_in_db()``, this one does not ignore the following tables:
        - 'geography_columns'
        - 'geometry_columns'
        - 'spatial_ref_sys'
        - 'raster_columns'
        - 'raster_overviews'

    :param uri: connection string

    :return: list of all tables in database
    """

    # Get a list of all tables that are currently in the database
    q = """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""

    tables = fetch_things_from_database(q, uri=uri, debug=debug)

    table_names = [t[0] for t in tables]

    return table_names


def get_list_of_columns_in_table(
        table: str,
        uri: str,
        debug: bool = True
) -> list:
    """
    Return a list of all columns that exist in a given table

    :param table: 'name_of_the_table'
    :param uri: connection string

    :return: list of columns
    """

    q = """ SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = '{}' """.format(table)

    raw_result = fetch_things_from_database(q, uri=uri, debug=debug)

    result = [t[0] for t in raw_result]

    return result


def get_list_of_spatial_tables_in_db(
        uri: str,
        debug: bool = True
) -> list:
    """
    Return a list of all spatial tables that exist in a given database.

    :param uri: connection string

    :return: list of all spatial tables in database
    """

    # Get a list of all tables that are currently in the database
    q = """SELECT f_table_name AS tblname FROM geometry_columns"""

    spatial_tables = fetch_things_from_database(q, uri=uri, debug=debug)

    spatial_table_names = [t[0] for t in spatial_tables]

    return spatial_table_names


def get_database_list(
        uri: str,
        default_db: str = "postgres",
        debug: bool = True
) -> list:
    """
    Get a list of all databases on a PgSQL cluster. To do this, you'll need to connect to the cluster's
    default database. This is likely ``postgres``, or ``defaultdb`` on DigitalOcean.

    :return: list of databases
    """

    # Get a list of databases that aren't the default_db
    q = f""" SELECT datname FROM pg_database
            WHERE datistemplate = false
                AND datname != '{default_db}'; """

    # Run this query from within the default_db
    query_result = fetch_things_from_database(q, uri=uri, debug=debug)
    db_list = [x[0] for x in query_result]

    return db_list

################################################################################
# TEST THAT THINGS EXIST
################################################################################


def database_exists(
        database: str,
        uri: str,
        default_db: str = "postgres",
        debug: bool = True
) -> bool:
    """
    Checks to see if the `database` exists on the cluser defined by the URI

    :param database: name of the database you want to check
    :param uri: connection string

    :return: True or False bool
    """

    db_list = get_database_list(uri=uri, default_db=default_db, debug=debug)

    if database in db_list:
        return True
    else:
        return False


def spatial_table_exists(
        table: str,
        uri: str,
        debug: bool = False
) -> bool:
    """
    Check to see if a spatial table exists in a given database URI.

    :param table: name of the table you're checking for
    :param uri: connection string

    :return: True or False bool
    """

    spatial_table_list = get_list_of_spatial_tables_in_db(uri=uri, debug=debug)

    if table in spatial_table_list:
        return True
    else:
        return False


################################################################################
# QUERY THE DATABASE AND RETURN AS DATAFRAME
################################################################################


def query_table(
        query: str,
        uri: str,
        debug: bool = False
) -> pd.DataFrame:
    """
    Query a table in a database and get the result as a ``pandas.DataFrame``

    :param query: 'SELECT * FROM my_table'
    :param uri: connection string

    :return: ``pandas.DataFrame``
    """

    if debug:
        print('-' * 40)
        print(f'## QUERYING via Pandas on {uri}')
        print(query)

    engine = sqlalchemy.create_engine(uri)

    df = pd.read_sql(query, engine)

    engine.dispose()

    return df


def query_geo_table(
        query: str,
        uri: str,
        geom_col: str = 'geom',
        debug: bool = False
) -> gpd.GeoDataFrame:
    """
    Query a geo table in a SQL database and get the result as a ``geopandas.GeoDataFrame``

    Be aware of the name of the geometry column. In PostGIS it's typically called 'geom',
    but geopandas seems to expect 'geometry' instead.

    :param query: 'SELECT gid, pop2015, geom FROM my_table WHERE pop2015 > 1000'
    :param uri: connection string
    :param geom_col: the name of the geometry column. Should either be 'geom' or 'geometry'

    :return: ``geopandas.GeoDataFrame``
    """

    if debug:
        print('-' * 40)
        print(f'## QUERYING via GeoPandas on {uri}')
        print(query)

    connection = psycopg2.connect(uri)

    gdf = gpd.GeoDataFrame.from_postgis(query, connection, geom_col=geom_col)

    connection.close()

    return gdf


################################################################################
# UPDATE THINGS IN THE DATABASE
################################################################################


def execute_query(
        query: str,
        uri: str,
        debug: bool = False
):
    """
    Use ``psycopg2`` to execute and commit a SQL command in the database.

    :param query: 'DROP VIEW IF EXISTS my_view;'
    :param uri: connection string

    :return: None
    """
    start_time = time.time()

    if debug:
        print(f'## UPDATING via psycopg2 on {uri}:')
        print('\t', query)

    connection = psycopg2.connect(uri)
    cursor = connection.cursor()

    cursor.execute(query)

    cursor.close()
    connection.commit()
    connection.close()

    if debug:
        runtime = round(time.time() - start_time, 2)
        print(f'## -> COMMITTED IN - {runtime} seconds')

    if query == hex_grid_function:
        query_text = "see pGIS.queries.hexagon_grid.py"
    else:
        query_text = query

    log_activity("pGIS.execute_query", uri=uri, query_text=query_text, debug=debug)


def add_or_nullify_column(
        tbl: str,
        column: str,
        data_type: str,
        uri: str,
        debug: bool = False
):
    """
    Add a column to a table if it doesn't exist yet
    If it does exist, set the entire column to NULL

    :param tbl: 'name_of_table'
    :param column: 'col_name'
    :param data_type: any valid PgSQL type: 'TEXT', 'FLOAT', etc.
    :param uri: connection string

    :return:
    """

    existing_cols = get_list_of_columns_in_table(tbl, uri=uri, debug=debug)
    col_exists = column in existing_cols

    if not col_exists:
        query = f'''ALTER TABLE {tbl} ADD COLUMN {column} {data_type};'''
    else:
        query = f""" UPDATE {tbl} SET {column} = NULL  """

    execute_query(query, uri=uri, debug=debug)

    log_activity("pGIS.add_or_nullify_column", uri=uri, query_text=query, debug=debug)


def drop_table(
        tablename: str,
        uri: str,
        debug: bool = False
):
    """
    Drop a table from a PostgreSQL database

    :param tablename: name of the table to drop (string)
    :param uri: connection string

    :return:
    """

    drop_table_query = f'DROP TABLE {tablename} CASCADE;'
    execute_query(drop_table_query, uri=uri, debug=debug)

    log_activity("pGIS.drop_table", uri=uri, query_text=drop_table_query, debug=debug)


def project_spatial_table(
        tablename: str,
        geom_type: str,
        orig_epsg: int,
        new_epsg: int,
        uri: str,
        debug: bool = False
):
    """
    Alter a table's ``geom`` column to a new EPSG

    :param tablename: name of the table (string)
    :param geom_type: name of a SQL-valid geometry type (string)
    :param orig_epsg: the EPSG that the data currently has (integer)
    :param new_epsg: the EPSG you want the data to be projected into (integer)
    :param uri: connection string
    :return:
    """

    qry = f'''ALTER TABLE {tablename}
              ALTER COLUMN geom TYPE geometry({geom_type}, {new_epsg})
              USING ST_Transform( ST_SetSRID( geom, {orig_epsg} ), {new_epsg} ); '''
    execute_query(qry, uri=uri, debug=debug)

    log_activity("pGIS.project_spatial_table", uri=uri, query_text=qry, debug=debug)


def prep_spatial_table(
        spatial_table_name: str,
        uri: str,
        geom_colname: str = "geom",
        debug: bool = False
):
    """
    Spatial tables in QGIS coming from PostGIS need a unique ID column and a spatial index.
    This function executes the two SQL queries needed.
    Results in a new column called ``unique_id`` and a spatial index on the existing ``geom`` column

    :param spatial_table_name: 'name_of_the_table'
    :param uri: connection string
    :return: nothing
    """

    # Add a primary key column named 'uid'
    unique_id_query = f'ALTER TABLE {spatial_table_name} ADD uid serial PRIMARY KEY;'
    execute_query(unique_id_query, uri=uri, debug=debug)

    # Create a spatial index on the 'geom' column
    spatial_index_query = f'CREATE INDEX gix_{spatial_table_name} ON {spatial_table_name} USING GIST ({geom_colname});'
    execute_query(spatial_index_query, uri=uri, debug=debug)

    log_activity("pGIS.prep_spatial_table",
                 uri=uri,
                 query_text=f"Add uid PK and make spatial index on {geom_colname} column",
                 debug=debug)


def register_geometry_column(
        spatial_table: str,
        uri: str,
        geom_type: str = 'Point',
        geom_colname: str = "geom",
        epsg: int = 4326,
        debug: bool = False
):
    """
    Run this query if your spatial table has an error in QGIS saying:
    ``There isn't an entry in geometry_columns``.
    Seems to be related to when you make new spatial tables via query
    Spatial tables imported via geopandas do not seem to have this problem

    :param spatial_table: 'name_of_table'
    :param uri: connection string
    :param geom_type: a valid PostGIS geom type, as string: 'Point'
    :param epsg: the EPSG that the data is already in. This does not transform anything
    :return: nothing
    """

    query = f''' ALTER TABLE {spatial_table}
                 ALTER COLUMN {geom_colname} TYPE geometry({geom_type}, {epsg})
                                        USING ST_SetSRID({geom_colname}, {epsg})'''

    execute_query(query, uri=uri, debug=debug)

    log_activity("pGIS.register_geometry_column",
                 uri=uri,
                 query_text=query,
                 debug=debug)


def make_geotable_from_query(
        new_tblname: str,
        query: str,
        uri: str,
        geom_colname: str = "geom",
        geom_type: str = "POINT",
        epsg: int = 4326,
        debug: bool = False
):
    """
    Quickly make a new spatial table in PostgreSQL with a query, and then prep the table
    by adding a uid, geom index, and entry into the geometry_columns table

    :param new_tblname: 'name_of_my_new_table'
    :param query: "SELECT * FROM my_table WHERE highway = 'Local' "
    :param geom_colname: 'geom'
    :return:
    """

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

    execute_query(full_query, uri=uri, debug=debug)
    log_activity("pGIS.make_geotable_from_query", uri=uri, query_text=full_query, debug=debug)

    prep_spatial_table(new_tblname, uri=uri, geom_colname=geom_colname, debug=debug)

    register_geometry_column(new_tblname, uri=uri,
                             geom_type=geom_type, geom_colname=geom_colname,
                             epsg=epsg, debug=debug)

################################################################################
# MAKE A NEW DATABASE
################################################################################


def load_hexgrid_function(
        uri: str,
        debug: bool = False
):
    """
    Execute SQL code that defines the ``hex_grid()`` function in the database.

    :param uri: connection string

    :return: None
    """

    if debug:
        print(f'Loading the hex_grid() function into {uri}')

    execute_query(hex_grid_function, uri=uri, debug=debug)

    log_activity("pGIS.load_hexgrid_function",
                 uri=uri,
                 query_text="see pGIS.queries.hexagon_grid.py",
                 debug=debug)


def make_new_database(
        uri_defaultdb: str,
        uri_newdb: str,
        debug: bool = False
):
    """
    Create a new PostgreSQL database, load PostGIS, and define a custom hexagon function

    :param uri_defaultdb: connection string to the default database in the cluster
    :param uri_newdb: connection string to the new database
    :return: None
    """

    db_connection_values = deconstruct_uri(uri_newdb)
    db_name = db_connection_values["database"]

    # check to see if this database already exists
    exists_qry = f""" SELECT EXISTS(
                        SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{db_name}')
                     );  """

    exist_query_response = query_table(exists_qry, uri=uri_defaultdb, debug=debug)

    exists_result = [str(row.exists) for idx, row in exist_query_response.iterrows()]

    if 'False' in exists_result:
        if debug:
            print(f'## Make new database routine for: {db_name}')

        make_db = f"CREATE DATABASE {db_name};"

        connection = psycopg2.connect(uri_defaultdb)
        connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        cursor.execute(make_db)

        cursor.close()
        connection.commit()
        connection.close()

        log_activity("pGIS.make_new_database",
                     uri=uri_newdb,
                     query_text=make_db,
                     debug=debug)

        # Check if PostGIS already exists by default. If not, add it.
        if 'geometry_columns' not in get_full_list_of_tables_in_db(uri=uri_newdb, debug=debug):
            msg = f'## Adding postGIS extension to {db_name}'
            execute_query('CREATE EXTENSION postGIS;', uri=uri_newdb, debug=debug)

            log_activity("pGIS.make_new_database",
                         uri=uri_newdb,
                         query_text="Added PostGIS",
                         debug=debug)

        else:
            msg = f'## PostGIS exists by default in {db_name}'
            log_activity("pGIS.make_new_database",
                         uri=uri_newdb,
                         query_text="PostGIS exists by default",
                         debug=debug)

        if debug:
            print(msg)

        # Load the custom SQL hexagon grid function
        load_hexgrid_function(uri=uri_newdb, debug=debug)

    else:
        if debug:
            print(f'## This database already exists at {uri_newdb}')


################################################################################
# LOAD A DATABASE DUMP FILE
################################################################################


def load_database_file(
        sql_file_path: str,
        uri_defaultdb: str,
        uri_newdb: str,
        debug: bool = False
):
    """
    Load a ``.sql`` file to a new database.
    NOTE: ``psql`` must be accessible on the system path

    :param sql_file_path: '/path/to/sqlfile.sql'
    :param uri_defaultdb: connection string to default db for the cluster
    :param uri_newdb: connection string to new database that will be loaded up via the .sql file
    :return:
    """

    path_list = sql_file_path.split('\\')
    sql_file = path_list[-1]

    db_connection_values = deconstruct_uri(uri_newdb)
    db_name = db_connection_values["database"]

    print(f'## Loading {sql_file} to {db_name}')

    make_new_database(uri_defaultdb=uri_defaultdb, uri_newdb=uri_newdb, debug=debug)

    c = f'psql "{uri_newdb}" <  "{sql_file_path}"'

    print(c)
    os.system(c)

    log_activity("pGIS.load_database_file",
                 uri=uri_newdb,
                 query_text=c,
                 debug=debug)


################################################################################
# WRITE DATA FROM FILE TO THE DATABASE
################################################################################


def dataframe_to_postgis(
        dataframe: pd.DataFrame,
        table_name: str,
        uri: str,
        debug: bool = False
):
    """
    Write a ``pandas.DataFrame`` to a PostgreSQL database.

    :param dataframe: ``pandas.DataFrame``
    :param table_name: 'name_of_the_table'
    :param uri: connection string
    :return: None
    """

    start_time = time.time()

    if debug:
        print(f'## Writing {table_name} from Pandas dataframe to {uri}')

    # FORCE ALL COLUMN NAMES TO LOWER-CASE (pgSQL requirement)
    dataframe.columns = [x.lower() for x in dataframe.columns]

    # CONNECT TO DATABASE, WRITE DATAFRAME, THEN DISCONNECT
    engine = sqlalchemy.create_engine(uri)
    dataframe.to_sql(table_name, engine, if_exists='replace')
    engine.dispose()

    if debug:
        # REPORT THE RUNTIME
        runtime = time.time() - start_time
        print(f'## -> Finished in {runtime} seconds')

    log_activity("pGIS.dataframe_to_postgis",
                 uri=uri,
                 query_text=f"Wrote pandas.DataFrame to {table_name}",
                 debug=debug)


def csv_to_postgis(
        csv_filepath: str,
        table_name: str,
        uri: str,
        overwrite: bool = False,
        debug: bool = False
):
    """
    Write ``.CSV`` file to a database.
    Accomplished by importing to a ``pandas.DataFrame`` and calling ``dataframe_to_postgis()``.

    :param csv_filepath: file path to .csv file
    :param table_name: name of the table to create
    :param uri: connection string
    :param overwrite: bool to control whether you want to overwrite the table, should it already exist in the db
    :return:
    """

    if debug:
        print(f'READING {csv_filepath}')

    # Does this table already exist? In order to overwrite it you have to drop the existing table first
    # If overwrite is set to False, it will return without
    tables = get_list_of_tables_in_db(uri=uri, debug=debug)
    if table_name in tables:
        if overwrite:
            print(f'## {table_name} ALREADY EXISTS... DROPPING TABLE CASCADE')
            drop_table(table_name, uri=uri, debug=debug)
        else:
            print(f'## {table_name} ALREADY EXISTS... Will not replace. Aborting.')
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
    for s in ['.', '-', '(', ')', '+']:
        df.columns = df.columns.str.replace(s, '')

    log_activity("pGIS.csv_to_postgis",
                 uri=uri,
                 query_text=f"Loaded CSV from {csv_filepath}",
                 debug=debug)

    # Save dataframe to database
    dataframe_to_postgis(df, table_name, uri=uri, debug=debug)


def geodataframe_to_postgis(
        geodataframe: gpd.GeoDataFrame,
        output_table_name: str,
        uri: str,
        src_epsg: Union[bool, int] = None,
        output_epsg: Union[bool, int] = None,
        debug: bool = False
):
    """
    Write a ``geopandas.GeoDataFrame`` to a PostGIS table in a SQL database.

    Assumes that the geometry column has already been named 'geometry'

    :param geodataframe: geopandas.GeoDataFrame
    :param output_table_name: 'name_of_the_output_table'
    :param src_epsg: if not None, will assign the geodataframe this EPSG in the format of {"init": "epsg:2227"}
    :param output_epsg: if not None, will reproject data from input EPSG to specified EPSG
    :param uri: connection string
    :return: None
    """
    start_time = time.time()

    # Get the geometry type
    # It's possible there are both MULTIPOLYGONS and POLYGONS. This grabs the MULTI variant
    geom_types = list(geodataframe.geometry.geom_type.unique())
    geom_typ = max(geom_types, key=len).upper()

    if debug:
        print(f'## PROCESSING {geom_typ} geodataframe to {output_table_name} in SQL')

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

    # Rename 'uid' to 'old_uid'
    if 'uid' in geodataframe.columns:
        geodataframe['old_uid'] = geodataframe['uid']
        geodataframe.drop('uid', 1, inplace=True)

    # Build a 'geom' column using geoalchemy2 and drop the source 'geometry' column
    geodataframe['geom'] = geodataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=epsg_code))
    geodataframe.drop('geometry', 1, inplace=True)

    # write geodataframe to SQL database
    if debug:
        print(f'## -> WRITING TO {uri}')

    engine = sqlalchemy.create_engine(uri)
    geodataframe.to_sql(output_table_name, engine,
                        if_exists='replace', index=True, index_label='gid',
                        dtype={'geom': Geometry(geom_typ, srid=epsg_code)})
    engine.dispose()

    if debug:
        runtime = round((time.time() - start_time), 2)
        print(f'\t FINISHED IN {runtime} seconds')

    log_activity("pGIS.geodataframe_to_postgis",
                 uri=uri,
                 query_text=f"Wrote geopandas.GeoDataFrame to {output_table_name}",
                 debug=debug)

    # If provided an EPSG, alter whatever the native projection was to the output_epsg
    if output_epsg:
        project_spatial_table(output_table_name, geom_typ, epsg_code, output_epsg, uri=uri, debug=debug)

    # Add a unique_id column and do a spatial index
    prep_spatial_table(output_table_name, uri=uri, debug=debug)


def shp_to_postgis(
        shp_path: str,
        output_table_name: str,
        uri: str,
        src_epsg: Union[bool, int] = None,
        output_epsg: Union[bool, int] = None,
        debug: bool = False
):
    """
    Read a ``shapefile`` into ``geopandas.GeoDataFrame`` and then use ``geodataframe_to_postgis()``

    :param shp_path:  r'c:\path\to\your\shapefile.shp'
    :param output_table_name: 'name_of_the_output_table'
    :param uri: connection string
    :param src_epsg: if not None, will assign the geodataframe this EPSG in the format of {"init": "epsg:2227"}
    :param output_epsg: if not None, will reproject data from input EPSG to specified EPSG
    :return:
    """

    if debug:
        print(f'## READING - {shp_path}')

    # READ THE .SHP INTO A GEOPANDAS.GEODATAFRAME
    gdf = gpd.read_file(shp_path)

    # REMOVE ROWS WITH NULL GEOMETRY
    gdf = gdf[gdf['geometry'].notnull()]

    # EXPLODE TO TRANSFORM ANY MULTIPART FEATURES TO SINGPLEPART
    gdf = gdf.explode()

    # RESET THE INDEX AFTER EXPLODING
    gdf['explode'] = gdf.index
    gdf = gdf.reset_index()

    log_activity("pGIS.shp_to_postgis",
                 uri=uri,
                 query_text="Loaded .SHP from {shp_path}",
                 debug=debug)

    # SEND THE GEODATAFRAME TO POSTGIS
    geodataframe_to_postgis(gdf, output_table_name, uri=uri, src_epsg=src_epsg, output_epsg=output_epsg, debug=debug)


################################################################################
# SAVE FILES OUT FROM THE DATABASE
################################################################################


def postgis_to_shp(
        table_name: str,
        output_folder: str,
        uri: str,
        geom_col: str = 'geom',
        debug: bool = False
):
    """
    Write a spatial PostGIS table to a shapfile using ``query_geo_table().to_file()``

    :param table_name: 'name_of_the_table'
    :param output_folder: r'c:\\path\\to\\your\\output\\shapefile\\folder'
    :param uri: connection string
    :param geom_col: 'geom' is default spatial column name in postGIS
    :return: None
    """

    try:
        print(f'## Creating shapefile from {table_name}')
        df = query_geo_table(f'SELECT * FROM {table_name}',
                             uri=uri,
                             geom_col=geom_col,
                             debug=debug)

        # Convert any boolean column types to strings
        for c in df.columns:
            datatype = df[c].dtype.name
            if datatype == 'bool':
                df[c] = df[c].astype(str)

        out_shp = os.path.join(output_folder, f'{table_name}.shp')
        df.to_file(out_shp)

        log_activity("pGIS.postgis_to_shp",
                     uri=uri,
                     query_text="Saved .SHP from {table_name} to {out_shp}",
                     debug=debug)

    except:
        print("## -> an error occured:")
        print(sys.exc_info()[0])


def dump_all_spatial_tables_to_shapefiles(
        output_folder: str,
        uri: str,
        debug: bool = False
):
    """
    Write all spatial tables in a PostGIS database to a subfolder of the output_folder.

    :param output_folder: folder path, e.g. r'C:\Egnyte\Shared\SERVICE_AREA\Analytics\data\Projects\El Dorado'
    :param uri: connection string
    :return:
    """

    database_name = deconstruct_uri(uri)["database"]

    # put everything into a subfolder. Make it if it doesn't exist
    dump_folder = os.path.join(output_folder, f'shps_from_pgdb_{database_name}')
    if not os.path.exists(dump_folder):
        os.mkdir(dump_folder)

    log_activity("pGIS.dump_all_spatial_tables_to_shapefiles",
                 uri=uri,
                 query_text=f"Saving all spatial tables to SHP in {dump_folder}",
                 debug=debug)

    # Get a list of all spatial tables and export each one
    spatial_tables = get_list_of_spatial_tables_in_db(uri, debug=debug)
    for table in spatial_tables:
        postgis_to_shp(table, dump_folder, uri=uri, debug=debug)


def dump_database_to_sql_file(
        backup_folder: str,
        uri: str,
        debug: bool = True
):
    """ Save a database stored on a host to a .sql file

    :param backup_folder: '/Users/my_name/Desktop/backup_folder'
    :param uri: connection string
    :return:
    """

    # Get a string for today's date, like '2019_02_26'
    today = str(datetime.now()).split(' ')[0].replace('-', '_')

    database_name = deconstruct_uri(uri)["database"]

    # USE PG_DUMP VIA COMMAND LINE TO SAVE A .SQL FILE
    sql_name = f'{database_name}_{today}.sql'
    sql_file = os.path.join(backup_folder, sql_name)

    if debug:
        print(f'## Using pg_dump to back up {database_name} to {sql_name}')

    log_activity('pGIS.dump_database_to_sql_file',
                 uri=uri,
                query_text=f"Using pg_dump to create {sql_name}",
                 debug=debug)

    system_call = f'pg_dump {uri} > "{sql_file}" '

    # RUN THE COMMAND FROM COMMAND LINE
    if debug:
        print(system_call)
    os.system(system_call)

    return sql_file