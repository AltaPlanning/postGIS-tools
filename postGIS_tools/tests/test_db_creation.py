import postGIS_tools as pGIS

from postGIS_tools.configurations import get_postGIS_config


if __name__ == "__main__":

    user_config, super_user_config = get_postGIS_config()

    DATABASE = "test_db"

    # Make a local database
    local_config = super_user_config["localhost"]
    pGIS.make_new_database(DATABASE, debug=True, **local_config)

    # Make a database on the D.O. cloud
    do_config = super_user_config["digitalocean"]
    print(super_user_config)
    pGIS.make_new_database(DATABASE, debug=True, **do_config)
