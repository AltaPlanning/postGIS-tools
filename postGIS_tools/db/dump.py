"""
``dump.py``
-----------

Move data out of a PostgreSQL database into a shapefile or .SQL backup of the entire database.

Examples
--------

    >>> # Save a spatial table to shapefile
    >>> postgis_to_shp('my_database', 'my_table', r'C:\\path\\to\\folder')


    >>> # Back up the entire database
    >>> dump_database_to_sql_file('my_database', 'localhost', r'C:\\path\\to\\folder')


"""

import os
import sys
from datetime import datetime

from postGIS_tools.db.query import query_geo_table
from postGIS_tools.db.get import get_list_of_spatial_tables_in_db

from postGIS_tools.assumptions import THIS_SYSTEM, PG_PASSWORD


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

    :param db_to_backup: 'name_of_db'
    :param backup_folder: '/Users/my_name/Desktop/backup_folder'
    :param host: name of the pgSQL host (string). eg: 'localhost' or '192.168.1.14'
    :param username: valid PostgreSQL database username (string). eg: 'postgres'
    :param password: password for the supplied username (string). eg: 'mypassword123'
    :param port: port number for the PgSQL database. eg: 5432
    :param debug: boolean to print messages to console
    :return:
    """

    # Get a string for today's date, like '2019_02_26'
    today = str(datetime.now()).split(' ')[0].replace('-', '_')

    # USE PG_DUMP VIA COMMAND LINE TO SAVE A .SQL FILE
    sql_file = f'{db_to_backup}_{today}.sql'
    if debug:
        print(f'Using pg_dump to back up {db_to_backup} to {sql_file}')

    if THIS_SYSTEM == 'Darwin':
        c = f""" export PGPASSWORD={password}
                 cd "{backup_folder}"
                 pg_dump -U postgres -h {host} {db_to_backup} > {sql_file} """

    elif THIS_SYSTEM == 'Windows':
        bkp_drive = backup_folder[:2]

        c = f""" set PGPASSWORD={password}
                 {bkp_drive}
                 cd "{backup_folder}"
                 pg_dump -U postgres -h {host} {db_to_backup} > {sql_file} """
        c = c.replace('\n             ', '& ')

    # RUN THE COMMAND FROM COMMAND LINE
    print(c)
    os.system(c)
