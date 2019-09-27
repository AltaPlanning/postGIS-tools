# postGIS-tools
Python tools for interacting with spatial and non-spatial 
data in PostgreSQL &amp; PostGIS

## Documentation
Documentation for this package can be found at https://postgis-tools.readthedocs.io/en/latest/

## Development Environment Setup
```shell script
conda install -c conda-forge psycopg2
conda install -c conda-forge geoalchemy2
conda install -c conda-forge geopandas
```

If you want to use ``sphinx`` to produce the documentation, 
you'll need to install two more modules:
```shell script
conda install -c conda-forge sphinx
conda install -c conda-forge sphinx_rtd_theme
```