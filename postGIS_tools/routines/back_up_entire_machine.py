"""
TODO: docstrings all across file

"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import postGIS_tools as pGIS
from postGIS_tools.assumptions import PG_PASSWORD


def get_database_list(
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = True

):
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    # Get a list of databases that aren't 'postgres'
    q = """ SELECT datname FROM pg_database 
            WHERE datistemplate = false
                AND datname != 'postgres'; """

    # Run this query from within the 'postgres' db
    query_result = pGIS.fetch_things_from_database(q, 'postgres', **config)
    db_list = [x[0] for x in query_result]

    return db_list


def back_up_all_databases(
        backup_folder: str,
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = True
):
    """ Save each database to file """

    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    for database in get_database_list(**config):
        pGIS.dump_database_to_sql_file(database, backup_folder, **config)


def remove_all_databases(
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = True
):
    """ DROP each non-postgres database. Proceed with caution """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    for db_name in get_database_list(**config):
        query = f"DROP DATABASE {db_name}"

        if debug:
            print(query)
        
        connection = psycopg2.connect(dbname='postgres',
                                      user='postgres',
                                      host=host,
                                      password=password)

        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cursor = connection.cursor()
        cursor.execute(query)
        cursor.close()
        connection.commit()
        connection.close()

