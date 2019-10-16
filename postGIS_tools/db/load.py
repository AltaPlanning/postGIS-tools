"""
``load.py``
-----------

Load a database via a previously-backed up .SQL file

"""
import os

from postGIS_tools.assumptions import PG_PASSWORD
from postGIS_tools.db.create import make_new_database


def load_database_file(
        sql_file_path,
        database_name,
        host='localhost',
        username='postgres',
        password=PG_PASSWORD,
        port: int = 5432,
        debug=False
):
    """
    Load a ``.sql`` file to a new database.
    NOTE: ``psql`` must be accessible on the system path

    TODO: type hints and params. Add port to psql URI

    :param sql_file_path: '/path/to/sqlfile.sql'
    :param database_name: 'new_db_name'
    :param host: 'localhost' or '192.168.1.14'
    :return:
    """
    config = {'host': host, 'username': username, 'password': password, 'port': port, 'debug': debug}

    path_list = sql_file_path.split('\\')
    sql_file = path_list[-1]

    print(f'Loading {sql_file} to {database_name}')

    make_new_database(database_name, **config)

    c = f'psql "postgresql://{username}:{password}@{host}/{database_name}" <  "{sql_file_path}"'

    print(c)
    os.system(c)