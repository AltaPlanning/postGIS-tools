"""
Overview of ``logs.py``
-----------------------

This is a low-tech solution to leaving a trail of breadcrumbs behind
in your ``PostgreSQL`` database. It creates a table named ``db_history``
and many ``postGIS_tools.functions`` use the ``log_activity()`` function
to update this table after successfully completing their task.

Example
-------

    >>> import postGIS_tools as pGIS
    >>> pGIS.log_activity("my_database", "execute_query", "UPDATE my_table SET my_col = 'my value'", debug=True)
    Logging to db_history:
            INSERT INTO db_history (username, function_name, query_text, update_time, user_os, user_cpu)
                VALUES ('aaron', 'execute_query', 'UPDATE my_table SET my_col = ''my value''',
                        '2019-11-23 09:00:26 PST', 'Darwin', 'Aaron-MBP.local');

"""
import os
from datetime import datetime
from pytz import timezone
import psycopg2

from postGIS_tools.configurations import THIS_SYSTEM, THIS_USER, THIS_COMPUTER, LOCAL_CONFIG_FOLDER

SIMPLE_LOG_FILE = os.path.join(LOCAL_CONFIG_FOLDER, "LOGFILE-postGIS_tools.txt")

def _make_log_table(
        uri: str,
        debug: bool = True
):
    """
    Execute a query to create the ``db_history`` table.

    :param uri: connection string

    :return: creates ``db_history`` table within specified database
    """
    query_to_make_table = f"""
        CREATE TABLE db_history (
            uid SERIAL PRIMARY KEY,
            username VARCHAR(255),
            function_name TEXT,
            query_text TEXT,
            update_time TIMESTAMP WITH TIME ZONE, 
            user_os VARCHAR(255),
            user_cpu VARCHAR(255)
        )
    """

    if debug:
        print(f"Making db_history log table within {uri}")

    try:
        connection = psycopg2.connect(uri)
        cur = connection.cursor()
        cur.execute(query_to_make_table)
        cur.close()
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


def _log_table_exists(
        uri: str,
        debug: bool = True
):
    """
    Check if the ``db_history`` log table exists. If not, run ``_make_log_table()``

    :param uri: connection string
    :return: boolean True or False if the log table exists
    """

    exists_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"

    connection = psycopg2.connect(uri)
    cursor = connection.cursor()
    cursor.execute(exists_query)

    result = cursor.fetchall()
    result_list = [x[0] for x in result]

    cursor.close()
    connection.close()

    if "db_history" in result_list:
        return True
    else:
        if debug:
            print("The db_history log table does not yet exist")

        return False


def log_activity(
        function_name: str,
        uri: str,
        query_text: str = "",
        local_timezone: str = "US/Pacific",
        debug: bool = False
):
    """
    Record what was done to a database in a new row within the ``db_history`` table.
    User must specify the database and function name that was run.
    Optionally, the raw SQL code can also be stored in the query_text column.

    If the ``db_history`` table does not exist yet, make the table before inserting this row.

    :param function_name: the name of the function that was run (string)
    :param uri: connection string
    :param query_text: the SQL text that was run (string)
    :param local_timezone: the user's local (or preferred) timezone (string). Must match ``datetime.datetime`` options

    :return: inserts a new row into the ``db_history`` table
    """

    # Get a timestamp for right now
    right_now = timezone(local_timezone).localize(datetime.now())
    right_now = right_now.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Create the db_history log table in the database if it doesn't exist yet
    if not _log_table_exists(uri=uri, debug=debug):
        _make_log_table(uri=uri, debug=debug)

    # Create the local text file if it doesn't exist yet
    if not os.path.exists(SIMPLE_LOG_FILE):
        with open(SIMPLE_LOG_FILE, "w") as textfile:
            textfile.write("uri, user, function, query_text, timestamp, system, computer\r\n")
            textfile.close()

    # Escape any single quotes in the query text
    query_text = query_text.replace("'", "''")

    # Insert the values as a new row
    insert_query = f"""
        INSERT INTO db_history (username, function_name, query_text, update_time, user_os, user_cpu)
            VALUES ('{THIS_USER}', '{function_name}', '{query_text}', 
                    '{right_now}', '{THIS_SYSTEM}', '{THIS_COMPUTER}');
    """

    if debug:
        print("""
        Logging to db_history:""")
        print(insert_query)

    # Do the text update
    with open(SIMPLE_LOG_FILE, "a") as textfile:
        textfile.write(", ".join([uri, THIS_USER, function_name, query_text, right_now, THIS_SYSTEM, THIS_COMPUTER]) + "\r\n")
        textfile.close()

    # Do the database update
    try:
        connection = psycopg2.connect(uri)
        cursor = connection.cursor()
        cursor.execute(insert_query)
        cursor.close()
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    pass