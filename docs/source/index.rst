.. postGIS-tools documentation master file, created by
   sphinx-quickstart on Tue Sep 17 14:34:13 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

``postGIS-tools`` documentation
===============================

This module simplifies the process of using ``Python``
to perform spatial analysis with data stored in a ``PostgreSQL`` database
with the ``PostGIS`` extension. It leverages other open-source modules including
``geopandas``, ``pandas``, ``sqlalchemy``, ``geoalchemy2``, and ``psycopg2``.

Assuming you've already installed the dependencies, installation is simple:

.. code-block:: console

  pip install git+https://github.com/aaronfraint/postGIS-tools.git


Here's an example showing how to create a SQL database, load a shapefile, and
extract the data as a ``geopandas.GeoDataFrame``:

.. code-block:: python

  import postGIS_tools as pGIS

  # Make a new database
  pGIS.make_new_database("my_new_database", host="localhost")

  # Load a shapefile as a spatial table
  pGIS.shp_to_postgis("/path/to/my/shapefile.shp", "my_table_in_postgres", "my_new_database")

  # Extract the spatial data as a geopandas geodataframe
  gdf = pGIS.query_geo_table("my_new_database", "SELECT * FROM my_table_in_postgres")

  # Iterate over all features in the spatial table
  for idx, row in gdf.iterrows():
      # do things...
      pass

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
