"""
``query.py``
------------

Extract spatial and non-spatial data from a PostgreSQL database.

Examples
--------

    >>> # Query non-spatial data
    >>> tabular_query = 'SELECT * FROM my_table WHERE my_column > 5'
    >>> dataframe = query_table('my_database', tabular_query)

    >>> # Query geo-data
    >>> geo_query = 'SELECT gid, ST_BUFFER(geom, 500) FROM my_table'
    >>> geodataframe = query_geo_table('my_database', geo_query)

"""
import pandas as pd
import geopandas as gpd

from postGIS_tools.db.connect import make_database_connection

from postGIS_tools.configurations import PG_PASSWORD


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
