"""
Overview of ``copy.py``
-----------------------

This module makes it easy to copy data within and between databases and hosts.

Examples
--------

    >>> # Copy a table within a database
    >>> copy_spatial_table_same_db('src_tbl', 'dest_tbl', 'database')

    >>> # Copy a table between databases on the same host
    >>> copy_spatial_table_same_host('src_tbl', 'dest_tbl', 'src_database', 'dest_database')

    >>> # Copy a table from a local to remote database
    >>> copy_spatial_table('src_tbl_name', 'dest_tbl_name', 'localhost', 'src_db', '192.168.1.14', 'dest_db')

"""

from postGIS_tools.db.query import query_geo_table
from postGIS_tools.db.write import geodataframe_to_postgis

from postGIS_tools.assumptions import PG_PASSWORD


def copy_spatial_table(
        source_table_name,
        destination_table_name,
        source_host,
        source_db,
        destination_host,
        destination_db,
        epsg=None,
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Copy a spatial table from one db/host to another table/db/host.
    If an ESPG is passed, this will also reproject the geom column for you.

    TODO: handle different passwords on different databases
    TODO: incorporate ports
    TODO: type hints and params

    :param source_table_name: 'name_of_source_spatial_table'
    :param destination_table_name: 'name_of_new_copy'
    :param source_host: '192.168.1.14'
    :param source_db: 'my_source_database'
    :param destination_host: 'localhost'
    :param destination_db: 'my_destination_database'
    :param epsg: None is default, but could be an int like: 2227
    :return: nothing, but creates a copy of the source table
    """

    if debug:
        print(f'COPYING {source_table_name} in {source_db} @ {source_host} TO {destination_table_name} in {destination_db} @ {destination_host}')

    source_config = {'host': source_host, 'password': password, 'username': username, 'debug': debug}
    dest_config =   {'host': destination_host, 'password': password, 'username': username, 'debug': debug}

    # Get a geodataframe with the source_config
    gdf = query_geo_table(source_db, f'SELECT * FROM {source_table_name}', geom_col='geom', **source_config)

    # Write the geodataframe to database with dest_config
    geodataframe_to_postgis(destination_db, gdf, destination_table_name, output_epsg=epsg, **dest_config)


def copy_spatial_table_same_db(
        src_tbl,
        dest_tbl,
        database,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Make a copy of a spatial table inside the same database.

    :param src_tbl:
    :param dest_tbl:
    :param database:
    :param host:
    :return:
    """

    config = {'password': password, 'username': username, 'debug': debug}

    copy_spatial_table(src_tbl, dest_tbl, host, database, host, database, **config)


def copy_spatial_table_same_host(
        src_tbl,
        dest_tbl,
        src_database,
        dest_database,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = False
):
    """
    Make a copy of a spatial table on the same host but into a different database.

    :param src_tbl:
    :param dest_tbl:
    :param src_database:
    :param dest_database:
    :param host:
    :return:
    """
    config = {'password': password, 'username': username, 'debug': debug}

    copy_spatial_table(src_tbl, dest_tbl, host, src_database, host, dest_database, **config)
