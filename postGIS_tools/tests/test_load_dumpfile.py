import os
import postGIS_tools


def test_load_database_file():
    """
    Load a .sql file that was created via an earlier ``pg_dump`` process
    """
    config, _ = postGIS_tools.get_postGIS_config()

    uri = postGIS_tools.make_uri("loaded_from_dump", **config["localhost"])
    super_uri = postGIS_tools.make_uri("postgres", **config["localhost"])

    folder = postGIS_tools.configurations.LOCAL_CONFIG_FOLDER
    sql_filepath = os.path.join(folder, "dumped.sql")

    postGIS_tools.load_database_file(sql_filepath, super_uri, uri, debug=True)


if __name__ == "__main__":
    test_load_database_file()
