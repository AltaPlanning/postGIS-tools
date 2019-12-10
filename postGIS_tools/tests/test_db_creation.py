import postGIS_tools as pGIS

from postGIS_tools.configurations import get_postGIS_config, make_uri


def test_database_creation(hostname):
    DATABASE = "test_db"

    print(f"<><> Testing the creation of {DATABASE} on {hostname}")
    user_config, super_user_config = get_postGIS_config(verbose=True)

    uri = make_uri(DATABASE, **user_config[hostname])
    super_uri = make_uri(**super_user_config[hostname])

    # Make a new database
    pGIS.make_new_database(uri_defaultdb=super_uri, uri_newdb=uri, debug=False)

    # Confirm it exists
    if not pGIS.database_exists(DATABASE, uri=uri, default_db=super_user_config[hostname]["database"], debug=False):
        print(f"<><> -> ERROR CREATING {DATABASE} AT {uri}")


if __name__ == "__main__":
    for config_host in ["localhost", "digitalocean_projects"]:
        test_database_creation(config_host)

