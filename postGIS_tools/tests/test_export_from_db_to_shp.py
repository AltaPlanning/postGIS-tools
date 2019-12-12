import os
import postGIS_tools as pGIS

config, _ = pGIS.get_postGIS_config()
database = "test_db"

CONFIG = config["localhost"]
URI = pGIS.make_uri(database, **CONFIG)
OUTPUT_FOLDER = pGIS.USER_DESKTOP


def export_shapefile(table_name):

    print(f"<><> Testing the creation of {table_name} to {OUTPUT_FOLDER}")

    # pGIS.postgis_to_shp("block_groups_w_od", OUTPUT_FOLDER, URI)

    if not os.path.exists(os.path.join(OUTPUT_FOLDER, f"{table_name}.shp")):
        print(f"<><> --> This table was not created")
    else:
        print(f"<><> --> Shapefile created properly")


if __name__ == "__main__":
    export_shapefile("nyc_subway_routes")