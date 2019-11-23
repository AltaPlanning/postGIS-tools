# postGIS-tools
Python tools for interacting with spatial and non-spatial
data in PostgreSQL &amp; PostGIS

## Documentation
Documentation for this package can be found at https://aaronfraint.github.io/postGIS-tools/html

## Development Environment Setup
The most straightforward way to set up an environment is to use the
included ``environment.yml`` file:
```shell script
(base) ~ conda env create -f environment.yml
```

A more hands-on approach entails:
```shell script
(base) ~ conda create --name pGIS_dev python=3.7
(base) ~ conda activate pGIS_development
(pGIS_dev) ~ conda config --add channels conda-forge
(pGIS_dev) ~ conda config --set channel_priority strict
(pGIS_dev) ~ conda install -c conda-forge pyproj=1.9.6
(pGIS_dev) ~ conda install -c conda-forge geopandas
(pGIS_dev) ~ conda install -c conda-forge psycopg2
(pGIS_dev) ~ conda install -c conda-forge geoalchemy2
(pGIS_dev) ~ conda install -c conda-forge sphinx
(pGIS_dev) ~ conda install -c conda-forge sphinx_rtd_theme
(pGIS_dev) ~ conda install -c conda-forge ipython
```

## Sphinx documentation
```shell script
cd documentation
sphinx-apidoc -f -e -M -o source/ ../ ../setup.py
make html
```