from postGIS_tools.db.create import make_new_database

from postGIS_tools.configurations import CONFIG_FULL

print(CONFIG_FULL)

if __name__ == "__main__":

    db_name = "test_db"

    # Make a local database
    local_config = CONFIG_FULL["localhost"]
    make_new_database(db_name, debug=True, **local_config)

    # Make a database on the D.O. cloud
    do_config = CONFIG_FULL["digitalocean"]
    print(CONFIG_FULL)
    make_new_database(db_name, debug=True, **do_config)
