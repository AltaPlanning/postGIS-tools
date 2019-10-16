"""
Overview of ``create_data.py``
------------------------------

This module facilitates the process of creating a persistent result from a query.

Examples
--------

    >>> # Filter a table and save the result
    >>> my_query = "SELECT * FROM my_table WHERE highway = 'Local' "
    >>> make_geotable_from_query(database, 'filtered_table', my_query)

    >>> # Do some PostGIS geoprocessing and save the result
    >>> my_query = "SELECT ST_BUFFER(geom, 150) FROM my_table "
    >>> make_geotable_from_query(database, 'buffered_table', my_query)

"""

from postGIS_tools.db.query import query_geo_table
from postGIS_tools.db.write import geodataframe_to_postgis

from postGIS_tools.assumptions import PG_PASSWORD


def make_geotable_from_query(
        database,
        new_tblname,
        query,
        geom_colname='geom',
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
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

    if debug:
        print(f'MAKING {new_tblname} FROM:')
        print(query)

    gdf = query_geo_table(database, query, geom_col=geom_colname, **config)
    geodataframe_to_postgis(database, gdf, new_tblname, **config)
