from postGIS_tools.logs import log_activity


def test_log_activity():
    """
    Confirm that you can create/update the log table for a given URI
    """

    uri = "postgresql://postgres@localhost:5432/test_from_qgis"

    log_activity("postGIS_tools.tests.test_log.test_log_activity()", uri=uri,
                 query_text="This is from running the Python PostGIS tests.", debug=True)


if __name__ == "__main__":
    test_log_activity()
