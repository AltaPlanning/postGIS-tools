# EPSG 102643 (Bay Area)
query_to_create_epsg_102643 = '''
    INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext)
    values ( 102643, 'ESRI', 102643, 
            '+proj=lcc +lat_1=37.06666666666667 +lat_2=38.43333333333333 +lat_0=36.5 +lon_0=-120.5 +x_0=2000000 +y_0=500000.0000000002 +datum=NAD83 +units=us-ft +no_defs ', 'PROJCS["NAD_1983_StatePlane_California_III_FIPS_0403_Feet",GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["False_Easting",6561666.666666666],PARAMETER["False_Northing",1640416.666666667],PARAMETER["Central_Meridian",-120.5],PARAMETER["Standard_Parallel_1",37.06666666666667],PARAMETER["Standard_Parallel_2",38.43333333333333],PARAMETER["Latitude_Of_Origin",36.5],UNIT["Foot_US",0.30480060960121924],AUTHORITY["EPSG","102643"]]'
    );  '''

# EPSG 102645 (LA area)
query_to_create_epsg_102645 = '''
    INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) 
    values ( 102645, 'ESRI', 102645, 
            '+proj=lcc +lat_1=34.03333333333333 +lat_2=35.46666666666667 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000.0000000002 +datum=NAD83 +units=us-ft +no_defs ', 'PROJCS["NAD_1983_StatePlane_California_V_FIPS_0405_Feet",GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["False_Easting",6561666.666666666],PARAMETER["False_Northing",1640416.666666667],PARAMETER["Central_Meridian",-118],PARAMETER["Standard_Parallel_1",34.03333333333333],PARAMETER["Standard_Parallel_2",35.46666666666667],PARAMETER["Latitude_Of_Origin",33.5],UNIT["Foot_US",0.30480060960121924],AUTHORITY["EPSG","102645"]]'
            );  '''

# COLLECTIONS OF WEIRD EPSG VALUES ENCOUNTERED
epsg_6420 = {'proj': 'lcc', 'lat_1': 37.06666666666667, 'lat_2': 38.43333333333333,
             'lat_0': 36.5, 'lon_0': -120.5, 'x_0': 2000000, 'y_0': 500000.0000000001,
             'ellps': 'GRS80', 'towgs84': '0,0,0,0,0,0,0', 'units': 'us-ft', 'no_defs': True}

epsg_102003 = {'proj': 'aea', 'lat_1': 29.5, 'lat_2': 45.5,
            'lat_0': 37.5, 'lon_0': -96, 'x_0': 0, 'y_0': 0,
            'datum': 'NAD83', 'units': 'm', 'no_defs': True}


weird_epsgs = [(6420, epsg_6420),
               (102003, epsg_102003)]
