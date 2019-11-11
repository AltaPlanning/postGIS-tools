"""
``write.py``
------------

Write data files to a PostgreSQL database. Including:
    - ``.CSV`` file
    - ``.SHP`` shapefile

This is enabled by helper functions that write:
    - ``pandas.DataFrame``
    - ``geopandas.GeoDataFrame``


Examples
--------

    >>> # Load a CSV file
    >>> my_csv_path = r'c:\data\my_file.csv'
    >>> csv_to_postgis(my_csv_path, 'tbl_name_in_database', 'database_name')

    >>> # Load a shapefile
    >>> my_shp_path = r'c:\data\my_shapefile.shp'
    >>> shp_to_postgis(my_shp_path, 'tbl_name_in_database', 'database_name')
"""

from typing import Union

import time
import pandas as pd
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement

from postGIS_tools.db.connect import make_database_connection
from postGIS_tools.db.get import get_list_of_tables_in_db
from postGIS_tools.db.update import drop_table, project_spatial_table, prep_spatial_table

from postGIS_tools.configurations import PG_PASSWORD


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

    # Save dataframe to database
    dataframe_to_postgis(db_name, df, table_name, **config)


def geodataframe_to_postgis(
        database: str,
        geodataframe: gpd.GeoDataFrame,
        output_table_name: str,
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

    # Get the EPSG value
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

    # If provided an EPSG, alter whatever the native projection was to the output_epsg
    if output_epsg:
        project_spatial_table(database, output_table_name, geom_typ, epsg_code, output_epsg, **config)

    # Add a unique_id column and do a spatial index
    prep_spatial_table(database, output_table_name, **config)


def shp_to_postgis(
        shp_path: str,
        output_table_name: str,
        database: str,
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

    # SEND THE GEODATAFRAME TO POSTGIS
    geodataframe_to_postgis(database, df, output_table_name, output_epsg=output_epsg, **config)


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
