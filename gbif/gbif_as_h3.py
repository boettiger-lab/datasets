import ibis
from ibis import _

con = ibis.duckdb.connect("gbif.duckdb", extensions=["spatial", "httpfs"])
con.raw_sql(
    '''
    INSTALL h3 FROM community;
    LOAD h3;
    '''
)

@ibis.udf.scalar.builtin
def h3_latlng_to_cell(lat: float, lng: float, zoom: int) -> int:
    ...
@ibis.udf.scalar.builtin
def hex(array) -> str:
    ...

@ibis.udf.scalar.builtin
def h3_cell_to_boundary_wkt	(array) -> str:
    ...


parquet = "s3://gbif-open-data-us-east-1/occurrence/2024-10-01/occurrence.parquet/**"
parquet = "2024-10-01/**"
gbif = con.read_parquet(parquet)

## ugh loop over instead
import pathlib
directory_path = pathlib.Path("2024-10-01")
files = [file for file in directory_path.iterdir() if file.is_file()]

# Loop over the list of files
for file in files:
    (con.read_parquet(f"{file}")
      .filter(_.decimallatitude.notnull(),
              _.decimallongitude.notnull(), 
             )
      .mutate(geom = _.decimallongitude.point(_.decimallatitude))
      .mutate(
        h0 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 0)),
        h1 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 1)),
        h2 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 2)),
        h3 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 3)),
        h4 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 4)),
        h5 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 5)),
        h6 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 6)),
        h7 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 7)),
        h8 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 8)),
        h9 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 9)),
        h10 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 10)),
        h11 = hex(h3_latlng_to_cell(_.decimallatitude, _.decimallongitude, 11)),
     )
      .to_parquet(f"gbif_hex/{file}.parquet")
    )
    




# +



#gbif = con.read_parquet("gbif_us_h3.parquet", "gbif")
# -

## optionally create a real 'geom; layer
#gbif.to_parquet("gbif_geo.parquet")


## Optional, Filter to specific polygon (CA)
#import geopandas as gpd
#states = gpd.read_file("https://github.com/nvkelso/natural-earth-vector/raw/master/geojson/ne_110m_admin_1_states_provinces.geojson")
#ca_polygon = states[states.name == 'California']
#ca_poly_expr = ibis.literal(ca_polygon.geometry.iloc[0])
#(con
# .table("gbif_with_geom")
# .filter( _.geom.within(ca_poly_expr))
#).to_parquet("gbif_ca.geoparquet")
# -


