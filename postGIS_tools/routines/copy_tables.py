"""
Overview of ``copy_tables.py``
------------------------------

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
from typing import Union

import postGIS_tools
from postGIS_tools.constants import PG_PASSWORD


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
    NOTE: THIS IS DEPRECATED, USE transfer_spatial_table() INSTEAD
    BEING KEPT IN CODEBASE TEMPORARILY FOR SOME BACKWARD-COMPATIBILITY

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
    gdf = postGIS_tools.functions.query_geo_table(source_db, f'SELECT * FROM {source_table_name}', geom_col='geom', **source_config)

    # Write the geodataframe to database with dest_config
    postGIS_tools.functions.geodataframe_to_postgis(destination_db, gdf, destination_table_name, output_epsg=epsg, **dest_config)


def transfer_spatial_table(
        source_table_name: str,
        source_uri: str,
        destination_uri: str,
        destination_table_name: Union[bool, str] = None,
        epsg: Union[bool, int] = None,
        debug: bool = True
):
    """
    Copy a spatial table from one db/host to another table/db/host.
    If an ESPG is passed, this will also reproject the geom column for you.

    TODO: docstring

    :param source_table_name: 'name_of_source_spatial_table'
    :param destination_table_name: 'name_of_new_copy'. If ``None`` then will use the source table name.
    :param epsg: None is default, but could be an int like: 2227
    :return: nothing, but creates a copy of the source table
    """

    if debug:
        print(f'## COPYING FROM {source_table_name} in {source_config}')
        print(f"## \t TO {destination_table_name} in {destination_config}")

    if destination_table_name is None:
        destination_table_name = source_table_name

    # Get a geodataframe with the source_uri
    gdf = postGIS_tools.functions.query_geo_table(f'SELECT * FROM {source_table_name}', source_uri,
                                                  geom_col='geom', debug=debug)

    # Write the geodataframe to database with dest_config
    postGIS_tools.functions.geodataframe_to_postgis(gdf, destination_table_name, destination_uri,
                                                    output_epsg=epsg, debug=debug)


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


if __name__ == "__main__":
    config, _ = postGIS_tools.get_postGIS_config()
    local_config = config["localhost"]
    digital_ocean_config = config["digitalocean_projects"]

    print(digital_ocean_config)

    local_db = "aa_2019_216_mv_modal_2019_12_04"
    do_db = "mountain_view_modal_2019_216"

    postGIS_tools.make_new_database(do_db, default_db="defaultdb", debug=True, **digital_ocean_config)

    for table in postGIS_tools.get_list_of_spatial_tables_in_db(local_db, **local_config):
        transfer_spatial_table(table, local_db, table, do_db, local_config, digital_ocean_config)
