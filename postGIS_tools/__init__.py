"""
Overview
--------

The ``postGIS_tools`` module abstracts the various SQL operations necessary to
effectively work with spatial data.


All functions in the sub-modules are available with the following import:

    >>> import postGIS_tools as pGIS

"""

from postGIS_tools.db.connect import make_database_connection
from postGIS_tools.db.create import load_hexgrid_function, make_new_database
from postGIS_tools.db.dump import postgis_to_shp, dump_all_spatial_tables_to_shapefiles, dump_database_to_sql_file
from postGIS_tools.db.get import fetch_things_from_database, get_list_of_tables_in_db, get_list_of_columns_in_table, get_list_of_spatial_tables_in_db, get_full_list_of_tables_in_db
from postGIS_tools.db.load import load_database_file
from postGIS_tools.db.query import query_table, query_geo_table
from postGIS_tools.db.update import execute_query, add_or_nullify_column, drop_table, project_spatial_table, prep_spatial_table, register_geometry_column, make_geotable_from_query
from postGIS_tools.db.write import dataframe_to_postgis, csv_to_postgis, geodataframe_to_postgis, shp_to_postgis, shp2pgsql

from postGIS_tools.queries.hexagon_grid import sql_to_make_hex_grid, hex_grid_function

from postGIS_tools.routines.copy import copy_spatial_table, copy_spatial_table_same_db, copy_spatial_table_same_host
