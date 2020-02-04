import postGIS_tools as pGIS

from ward import test

DATABASE = "test_db"
URI = pGIS.make_uri(DATABASE, **pGIS.CONFIG["localhost"])


def test_loading_of_shapefile(shp_path, uri, pg_table_name, epsg):

    pGIS.shp_to_postgis(shp_path, pg_table_name, uri=uri, output_epsg=epsg, debug=False)

    assert pGIS.spatial_table_exists(pg_table_name, uri=uri, debug=False)


@test("Loaded NYC subway stops shapefile from URL")
def _():
    test_loading_of_shapefile("https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=Shapefile",
                              URI, "nyc_subway_stops", 2263)


@test("Loaded NYC subway lines shapefile from URL")
def _():
    test_loading_of_shapefile("https://data.cityofnewyork.us/api/geospatial/3qz8-muuu?method=export&format=Shapefile",
                              URI, "nyc_subway_routes", 2263)
