# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import postGIS_tools as pGIS
from postGIS_tools.assumptions import PG_PASSWORD


def get_database_list(
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=True

):
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    # Get a list of databases that aren't 'postgres'
    q = """ SELECT datname FROM pg_database 
            WHERE datistemplate = false
                AND datname != 'postgres'; """

    # Run this query from within the 'postgres' db
    query_result = pGIS.fetch_things_from_database(q, 'postgres', **config)
    db_list = [x[0] for x in query_result]

    return db_list


def back_up_all_databases(
        backup_folder,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        debug=True
):
    """ Save each database to file """

    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

    for database in get_database_list(**config):
        pGIS.dump_database_to_sql_file(database, backup_folder, **config)


def remove_all_databases(
    host='localhost',
    username='postgres',
    password=PG_PASSWORD,
    debug=True
):
    """ DROP each non-postgres database. Proceed with caution """
    config = {'host': host, 'password': password, 'username': username, 'debug': debug}

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

