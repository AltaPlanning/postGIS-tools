"""
TODO: docstrings all across file



Examples
--------

    >>> from postGIS_tools.configurations import get_postGIS_config, USER_DESKTOP
    >>> config, _ = get_postGIS_config()
    >>> config["localhost"]["debug"] = True
    >>> back_up_all_databases(USER_DESKTOP, **config["localhost"])
    >>> remove_all_databases(databases_to_keep=["aaron", "postgres"], **config["localhost"])

"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import postGIS_tools as pGIS
from postGIS_tools.constants import PG_PASSWORD





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

    for database in pGIS.get_database_list(**config):
        pGIS.dump_database_to_sql_file(database, backup_folder, **config)


def remove_all_databases(
        databases_to_keep: list = ["postgres"],
        host: str = 'localhost',
        username: str = 'postgres',
        password: str = PG_PASSWORD,
        port: int = 5432,
        debug: bool = True
):
    """ DROP each database that's not in the list of databases_to_keep """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    for db_name in pGIS.get_database_list(**config):
        if db_name not in databases_to_keep:

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


if __name__ == "__main__":
    pass
    # remove_all_databases(["postgres", "aaron"])
