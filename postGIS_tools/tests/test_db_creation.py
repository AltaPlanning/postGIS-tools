from postGIS_tools.db.create import make_new_database

from postGIS_tools.configurations import get_postGIS_config


if __name__ == "__main__":

    CONFIG, CONFIG_FULL = get_postGIS_config()

    DATABASE = "test_db"

    # Make a local database
    local_config = CONFIG_FULL["localhost"]
    make_new_database(DATABASE, debug=True, **local_config)

    # Make a database on the D.O. cloud
    do_config = CONFIG_FULL["digitalocean"]
    print(CONFIG_FULL)
    make_new_database(DATABASE, debug=True, **do_config)
