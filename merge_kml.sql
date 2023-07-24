//CREATE DATALAKE
create stage geostage;

list @geostage;

//UPLOAD KML FILES TO DATALAKE
/*
put file:///Users/shidayatullah/Documents/POC/XL/*.kml @geostage AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
*/

//LOAD KML
/*
https://github.com/Snowflake-Labs/sf-samples/blob/main/samples/geospatial/Python%20UDFs/PY_LOAD_KML.sql
*/
CREATE OR REPLACE FUNCTION PY_LOAD_KML(PATH_TO_FILE string)
returns table (wkb binary, properties object)
language python
runtime_version = 3.8
imports=('@geostage/archive.zip')
packages = ('fiona', 'shapely')
handler = 'KMLReader'
AS $$
import fiona
from shapely.geometry import shape
import sys
IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]

class KMLReader:        
    def process(self, PATH_TO_FILE: str):
      fiona.drvsupport.supported_drivers['libkml'] = 'rw' # enable KML support which is disabled by default
      fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # enable KML support which is disabled 
      shapefile = fiona.open(f"zip://{import_dir}/archive.zip/{PATH_TO_FILE}")
      for record in shapefile:
        yield ((shape(record['geometry']).wkb, dict(record['properties'])))
$$;

list @geostage;

CREATE OR REPLACE TABLE xl_db.public.kml_example_a AS SELECT * FROM table(PY_LOAD_KML('example_a.kml'));
CREATE OR REPLACE TABLE xl_db.public.kml_example_b AS SELECT * FROM table(PY_LOAD_KML('example_b.kml'));


select * from kml_example_a;
select * from kml_example_b;


//MAKE VALID function:
CREATE OR REPLACE FUNCTION PY_MAKEVALID_WKB(geowkb BINARY) 
RETURNS BINARY 
LANGUAGE python 
runtime_version = 3.8 
packages = ('shapely') 
handler = 'udf' 
AS $$
import shapely
from shapely.geometry import shape, mapping
from shapely import wkb
from shapely.validation import make_valid
def udf(geowkb):
    g1 = wkb.loads(geowkb)
    if g1.is_valid == True:
        g1 = g1.buffer(0.000001, resolution = 1, join_style = 1)
        g1 = g1.simplify(0.000001)
        fixed_shape = g1
    else:
        fixed_shape = make_valid(g1)
    return  wkb.dumps(fixed_shape)
$$;

//drop table kml_geofixed;

//CREATE VALID TEMPORARY TABLE
create table kml_geofixed_a as
SELECT to_geography(PY_MAKEVALID_WKB(wkb), TRUE) AS geofixed,
       st_npoints(geofixed) as npoints
FROM kml_example_a
WHERE length(wkb) < 6000000
AND st_isvalid(geofixed) = TRUE;

create table kml_geofixed_b as
SELECT to_geography(PY_MAKEVALID_WKB(wkb), TRUE) AS geofixed,
       st_npoints(geofixed) as npoints
FROM kml_example_b
WHERE length(wkb) < 6000000
AND st_isvalid(geofixed) = TRUE;

//SELECT FIXED GEO
select * from kml_geofixed_a;
select * from kml_geofixed_b;

//
select ST_STARTPOINT(st_asgeojson(geofixed)) from kml_geofixed_a;
select st_asgeojson(geofixed) from kml_geofixed_a;
SELECT ST_MAKEPOINT(37.5, 45.5);

select * from citibike.information_schema.table_storage_metrics;
select table_name,row_count,bytes from information_schema.tables where bytes is not null order by row_count desc;

//combine
create table kml_geofixed_merged as 
select * from kml_geofixed_a
union all 
select * from kml_geofixed_b;

//SELECT ALL
SELECT * FROM kml_geofixed_merged;

//AGGREGATE
SELECT PY_UNION_AGG(ARRAY_AGG(st_asgeojson(geofixed))) FROM kml_geofixed_merged;


//get CENTROID
select ST_CENTROID(select geofixed from kml_geofixed_a limit 1);
select ST_CENTROID(select geofixed from kml_geofixed_b limit 1);


//CALCULATE DIRECT DISTANCE FROM CENTROID
with a as (
select ST_CENTROID(select geofixed from kml_geofixed_a limit 1) as centro_a
),
b as (
select ST_CENTROID(select geofixed from kml_geofixed_b limit 1) as centro_b
)
select st_distance(centro_a,centro_b) distance_in_km from a,b;


//CALCULATE NEAREST
with a as (
select  geofixed as geofixed_a from kml_geofixed_a
),
b as (
select  geofixed as geofixed_b from kml_geofixed_b
)
select st_distance(geofixed_a,geofixed_b) distance_in_km from a CROSS JOIN b;

