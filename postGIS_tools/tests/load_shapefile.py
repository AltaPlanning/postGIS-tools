import postGIS_tools as pGIS


if __name__ == "__main__":

    user_config, _ = pGIS.get_postGIS_config()
    local_config = user_config["localhost"]
    do_config = user_config["do_projects"]

    DATABASE = "test_db"

    for config in [local_config, do_config]:
        print(f"Testing {config}")

        # Import NYC subway stops
        nyc_subway_stops_url = "https://data.cityofnewyork.us/api/geospatial/arq3-7z49?method=export&format=Shapefile"
        pGIS.shp_to_postgis(nyc_subway_stops_url, "nyc_subway_stops", DATABASE, output_epsg=2263, **config)

        # Import NYC subway routes
        nyc_subway_routes_url = "https://data.cityofnewyork.us/api/geospatial/3qz8-muuu?method=export&format=Shapefile"
        pGIS.shp_to_postgis(nyc_subway_routes_url, "nyc_subway_routes", DATABASE, output_epsg=2263, **config)

        for table in ["nyc_subway_stops", "nyc_subway_routes"]:
            if not pGIS.spatial_table_exists(table, DATABASE, **config):
                print(f"ERROR LOADING {table} to {config}")
