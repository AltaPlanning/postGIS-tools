from postGIS_tools.logs import log_activity
from ward import test


@test("log_activity() works properly")
def _():
    """
    Confirm that you can create/update the log table for a given URI
    """
    try:
        uri = "postgresql://postgres@localhost:5432/test_from_qgis"

        log_activity("postGIS_tools test run", uri=uri,
                     query_text="This is from running the Python PostGIS tests.", debug=False)
        assert True

    except:
        assert False
