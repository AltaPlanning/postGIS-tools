import postGIS_tools as pGIS


def test_loading_of_shapefile(hostname):
    """
    Load up a shapefile and then confirm that it exists as a spatial table in the database
    """

    DATABASE = "test_db"

    config, _ = pGIS.get_postGIS_config()

    uri = pGIS.make_uri(DATABASE, **config[hostname])

    print(f"<><> Testing the import of a shapefile to {uri}")

    # Import NYC subway stops
    nyc_subway_stops_url = "https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=Shapefile"
    pGIS.shp_to_postgis(nyc_subway_stops_url, "nyc_subway_stops", uri=uri, output_epsg=2263, debug=True)

    # Import NYC subway routes
    nyc_subway_routes_url = "https://data.cityofnewyork.us/api/geospatial/3qz8-muuu?method=export&format=Shapefile"
    pGIS.shp_to_postgis(nyc_subway_routes_url, "nyc_subway_routes", uri=uri, output_epsg=2263, debug=True)

    for table in ["nyc_subway_stops", "nyc_subway_routes"]:
        if not pGIS.spatial_table_exists(table, uri=uri, debug=True):
            print(f"<><> -> ERROR LOADING {table} to {uri}")


if __name__ == "__main__":
    for config_host in ["localhost", "digitalocean_projects"]:
        test_loading_of_shapefile(config_host)
