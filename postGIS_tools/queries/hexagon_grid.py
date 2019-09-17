"""
Summary of ``hexagon_grid.py``
------------------------------

Text-based definition of a SQL function to create a hexagon grid overlay

"""


def sql_to_make_hex_grid(
        extent_table,
        epsg,
        output_table_name,
        size
):
    """
    This function returns a string with a valid SQL query that creates
    a hexagon layer covering the input {extent_table}.

    :param extent_table: 'name_of_table_to_cover'
    :param epsg: integer for EPSG you want the hexagons to be in
    :param output_table_name: 'hexagon_layer'
    :param size: float value, 1 = 1 square KM
    :return: string, valid SQL query
    """

    create_hex_grid_command = f"""
    DROP TABLE IF EXISTS {output_table_name};
    CREATE TABLE {output_table_name} (
      gid SERIAL NOT NULL PRIMARY KEY,
      geom GEOMETRY('POLYGON', {epsg}, 2) NOT NULL
    )
    WITH (OIDS=FALSE);  

    INSERT INTO {output_table_name} (geom)
    SELECT hex_grid({size},
                    (select st_xmin(st_transform(st_collect(geom), 4326)) from {extent_table}),
                    (select st_ymin(st_transform(st_collect(geom), 4326)) from {extent_table}),
                    (select st_xmax(st_transform(st_collect(geom), 4326)) from {extent_table}),
                    (select st_ymax(st_transform(st_collect(geom), 4326)) from {extent_table}),
                    4326, {epsg}, {epsg});"""

    return create_hex_grid_command.replace('\n', '')


hex_grid_function = """
------ this function was found and copied from the web link below:
------ https://github.com/minus34/postgis-scripts/blob/master/hex-grid/create-hex-grid-function.sql
--------------------------------------------------------------------------------------------------------------------------------
-- HEX GRID - Create function
--------------------------------------------------------------------------------------------------------------------------------
--
-- Hugh Saalmans (@minus34)
-- 2015/04/10
--
-- DESCRIPTION:
--
-- Function returns a grid of mathmatically correct hexagonal polygons.
-- Useful for hexbinning (the art of mapping clusters of information unbiased by political/historical/statistical boundaries).
--
-- INPUT
--
--   areakm2     : area of each hexagon in square km.
--                   - note: hexagon size can be off slightly due to coordinate rounding in the calcs.
--
--   xmin, ymin  : min coords of the grid extents.
--
--   xmax, ymax  : max coords of the grid extents.
--
--   inputsrid   : the coordinate system (SRID) of the input min/max coords.
--
--   workingsrid : the SRID used to process the polygons.
--                   - SRID must be a projected coord sys (i.e. in metres) as the calcs require ints. Degrees are out.
--                   - should be an equal area SRID such as Albers or Lambert Azimuthal (e.g. Australia = 3577, US = 2163).
--                   - using Mercator will NOT return hexagons of equal area due to its distortions (don't try it in Greenland).
--
--   ouputsrid   : the SRID of the output polygons.
--
-- NOTES
--
--   Hexagon height & width are rounded up & down to the nearest metre, hence the area may be off slightly.
--   This is due to the Postgres generate_series function which doesn't support floats.
--
--   Why are my areas wrong in QGIS, MapInfo, etc...?
--      Let's assume you created WGS84 lat/long hexagons, you may have noticed the areas differ by almost half in a desktop GIS
--      like QGIS or MapInfo Pro. This is due to the way those tools display geographic coordinate systems like WGS84 lat/long.
--      Running the following query in PostGIS will confirm the min & max sizes of your hexagons (in km2):
--
--         SELECT (SELECT (MIN(ST_Area(geom::geography, FALSE)) / 1000000.0)::numeric(10,3) From my_hex_grid) AS minarea,
--               (SELECT (MAX(ST_Area(geom::geography, FALSE)) / 1000000.0)::numeric(10,3) From my_hex_grid) AS maxarea;
--
--   Hey, why doesn't the grid cover the area I defined using my min/max extents?
--      Assuming you used lat/long extents and processed the grid with an equal area projection, the projection caused your
--      min/max coords to describe a conical shape, not a rectangular one - and the conical area didn't cover everything you
--      wanted to include.  See au-hex-grid.png as an example of this.
--      If you're bored - learn why projections distort maps here: http://www.icsm.gov.au/mapping/about_projections.html
--
--   This code is based on this PostGIS Wiki article: https://trac.osgeo.org/postgis/wiki/UsersWikiGenerateHexagonalGrid
--
--   Dimension calcs are based on formulae from: http://hexnet.org/content/hexagonal-geometry
--
-- LICENSE
--
-- This work is licensed under the Apache License, Version 2: https://www.apache.org/licenses/LICENSE-2.0
--
--------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION hex_grid(areakm2 FLOAT, xmin FLOAT, ymin FLOAT, xmax FLOAT, ymax FLOAT, inputsrid INTEGER,
  workingsrid INTEGER, ouputsrid INTEGER)
  RETURNS SETOF geometry AS
$BODY$

DECLARE
  minpnt GEOMETRY;
  maxpnt GEOMETRY;
  x1 INTEGER;
  y1 INTEGER;
  x2 INTEGER;
  y2 INTEGER;
  aream2 FLOAT;
  qtrwidthfloat FLOAT;
  qtrwidth INTEGER;
  halfheight INTEGER;

BEGIN

  -- Convert input coords to points in the working SRID
  minpnt = ST_Transform(ST_SetSRID(ST_MakePoint(xmin, ymin), inputsrid), workingsrid);
  maxpnt = ST_Transform(ST_SetSRID(ST_MakePoint(xmax, ymax), inputsrid), workingsrid);

  -- Get grid extents in working SRID coords
  x1 = ST_X(minpnt)::INTEGER;
  y1 = ST_Y(minpnt)::INTEGER;
  x2 = ST_X(maxpnt)::INTEGER;
  y2 = ST_Y(maxpnt)::INTEGER;

  -- Get height and width of hexagon - FLOOR and CEILING are used to get the hexagon size closer to the requested input area
  aream2 := areakm2 * 1000000.0;
  qtrwidthfloat := sqrt(aream2/(sqrt(3.0) * (3.0/2.0))) / 2.0;

  qtrwidth := FLOOR(qtrwidthfloat);
  halfheight := CEILING(qtrwidthfloat * sqrt(3.0));

  -- Return the hexagons - done in pairs, with one offset from the other
  RETURN QUERY (
    SELECT ST_Transform(ST_SetSRID(ST_Translate(geom, x_series::FLOAT, y_series::FLOAT), workingsrid), ouputsrid) AS geom
      FROM generate_series(x1, x2, (qtrwidth * 6)) AS x_series,
           generate_series(y1, y2, (halfheight * 2)) AS y_series,
           (
             SELECT ST_GeomFromText(
               format('POLYGON((0 0, %s %s, %s %s, %s %s, %s %s, %s %s, 0 0))',
                 qtrwidth, halfheight,
                 qtrwidth * 3, halfheight,
                 qtrwidth * 4, 0,
                 qtrwidth * 3, halfheight * -1,
                 qtrwidth, halfheight * -1
               )
             ) AS geom
             UNION
             SELECT ST_Translate(
               ST_GeomFromText(
                 format('POLYGON((0 0, %s %s, %s %s, %s %s, %s %s, %s %s, 0 0))',
                   qtrwidth, halfheight,
                   qtrwidth * 3, halfheight,
                   qtrwidth * 4, 0,
                   qtrwidth * 3, halfheight * -1,
                   qtrwidth, halfheight * -1
                 )
               )
             , qtrwidth * 3, halfheight) as geom
           ) AS two_hex);

END$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
"""

