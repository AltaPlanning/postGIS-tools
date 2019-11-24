import postGIS_tools as pGIS

from postGIS_tools.configurations import get_postGIS_config


if __name__ == "__main__":

    user_config, super_user_config = get_postGIS_config()
    local_config = super_user_config["localhost"]
    do_config = super_user_config["do_projects"]

    DATABASE = "test_db"

    for config in [local_config, do_config]:

        # Make a new database
        pGIS.make_new_database(DATABASE, **config)

        # Confirm it exists
        if not pGIS.database_exists(DATABASE, **config):
            print(f"ERROR CREATING {DATABASE} AT {config}")
