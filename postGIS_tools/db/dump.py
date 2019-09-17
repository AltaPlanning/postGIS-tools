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
        database,
        table_name,
        output_folder,
        geom_col='geom',
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Write a spatial PostGIS table to a shapfile using ``query_geo_table().to_file()``

    :param database: 'name_of_the_database'
    :param table_name: 'name_of_the_table'
    :param output_folder: r'c:\\path\\to\\your\\output\\shapefile\\folder'
    :param geom_col: 'geom' is default spatial column name in postGIS
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :return: None
    """

    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

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
        database_name,
        output_folder,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """
    Write all spatial tables in a PostGIS database to a subfolder of the output_folder.

    :param database_name: 'name_of_the_database'
    :param output_folder: folder path, e.g. r'C:\Egnyte\Shared\SERVICE_AREA\Analytics\data\Projects\El Dorado'
    :param host: by default is 'localhost', but could also be '192.168.1.14'
    :return:
    """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    # put everything into a subfolder. Make it if it doesn't exist
    dump_folder = os.path.join(output_folder, f'shps_from_pgdb_{database_name}')
    if not os.path.exists(dump_folder):
        os.mkdir(dump_folder)

    # Get a list of all spatial tables and export each one
    spatial_tables = get_list_of_spatial_tables_in_db(database_name, **config)
    for table in spatial_tables:
        postgis_to_shp(database_name, table, dump_folder, **config)


def dump_database_to_sql_file(
        db_to_backup,
        backup_folder,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=False
):
    """ Save a database stored on a host to a .sql file, using TERMINAL on OSX

    :param db_to_backup: 'name_of_db'
    :param host: 'localhost' or '192.168.1.14'
    :param backup_folder: '/Users/my_name/Desktop/backup_folder'
    :return:
    """

    # Get a string for today's date, like '2019_02_26'
    today = str(datetime.now()).split(' ')[0].replace('-', '_')

    # USE PG_DUMP VIA COMMAND LINE TO SAVE A .SQL FILE
    sql_file = f'{db_to_backup}_{today}.sql'
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
