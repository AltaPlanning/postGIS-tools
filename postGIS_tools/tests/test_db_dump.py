import os
import postGIS_tools as pGIS


def test_dump_database_to_sql_file(output_folder, uri):
    """ Dump a database file and then confirm that it exists """

    print(f"<><> Testing the export of {uri} to {output_folder}")

    # Run the function
    sql_file = pGIS.dump_database_to_sql_file(output_folder, uri, debug=True)

    # Test that the output SQL file exists
    if not os.path.exists(sql_file):
        print(f"<><> --> File was not created")
    else:
        print(f"<><> --> SQL file was successfully created")


if __name__ == "__main__":
    config, _ = pGIS.get_postGIS_config()

    DATABASE = "test_db"
    OUTPUT_FOLDER = pGIS.USER_DOCUMENTS_FOLDER
    URI = pGIS.make_uri(DATABASE, **config["localhost"])

    test_dump_database_to_sql_file(OUTPUT_FOLDER, URI)
