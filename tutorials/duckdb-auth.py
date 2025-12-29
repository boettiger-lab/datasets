import duckdb
import os
endpoint = os.getenv("AWS_S3_ENDPOINT", "s3-west.nrp-nautilus.io")
url_style = "path"
use_ssl = os.getenv("AWS_HTTPS", "TRUE")

set_secrets = f'''
CREATE OR REPLACE SECRET s3_key (
    TYPE S3,
    KEY_ID '',
    SECRET '',
    USE_SSL '{use_ssl}',
    ENDPOINT '{endpoint}',
    URL_STYLE '{url_style}',
    REGION 'us-east-1'
);
'''

duckdb.sql("INSTALL httpfs; LOAD httpfs;")

duckdb.sql(set_secrets)
x = duckdb.read_parquet("s3://public-mappinginequality/mappinginequality.parquet").limit(1)
print(x)
