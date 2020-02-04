import postGIS_tools as pGIS

from postGIS_tools.configurations import get_postGIS_config, make_uri

from ward import test


def _test_make_new_database(hostname):
    DATABASE = "test_db"

    user_config, super_user_config = get_postGIS_config(verbose=False)

    uri = make_uri(DATABASE, **user_config[hostname])
    super_uri = make_uri(**super_user_config[hostname])

    # Make a new database
    pGIS.make_new_database(uri_defaultdb=super_uri, uri_newdb=uri, debug=False)

    # Confirm it exists
    assert pGIS.database_exists(DATABASE, uri=uri, default_db=super_user_config[hostname]["database"], debug=False)


@test("make_new_database() makes a database on localhost")
def _():
    _test_make_new_database("localhost")


@test("make_new_database() makes a database on digital ocean")
def _():
    _test_make_new_database("digitalocean_projects")
