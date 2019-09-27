# postGIS-tools
Python tools for interacting with spatial and non-spatial 
data in PostgreSQL &amp; PostGIS

## Documentation
Documentation for this package can be found at https://postgis-tools.readthedocs.io/en/latest/

## Development Environment Setup
```shell script
(base) ~ conda create --name pGIS_development python=3.7
(base) ~ conda activate pGIS_development
(pGIS_development) ~ conda config --add channels conda-forge
(pGIS_development) ~ conda config --set channel_priority strict
(pGIS_development) ~ conda install -c conda-forge pyproj=1.9.6
(pGIS_development) ~ conda install -c conda-forge geopandas
(pGIS_development) ~ conda install -c conda-forge psycopg2
(pGIS_development) ~ conda install -c conda-forge geoalchemy2
```

If you want to use ``sphinx`` to produce the documentation, 
you'll need to install two more modules:
```shell script
(pGIS_development) ~ conda install -c conda-forge sphinx
(pGIS_development) ~ conda install -c conda-forge sphinx_rtd_theme
```

Finally, you may also want `ipython` as well:
```shell script
(pGIS_development) ~ conda install -c conda-forge ipython
```